"""内存额度下的外排与级联多路归并。

每个排序签名对应一个ExternalSorter：行先进入内存缓冲，达到mem_rows就把
一个有序run写到临时JSONL文件；最终用级联k路归并输出全分区稳定物理行序。
物理行序的tiebreak只有rid（baseline同规则），rid不参与peer划分——
业务对等组在window算子内按ORDER BY键另行判定。
归并流同时给出每行所属run、run内序号与字节偏移，调试侧车据此按需seek，
而不是把整个分区重新载回内存。
"""
import heapq
import json
import os
import tempfile
from decimal import Decimal, InvalidOperation
from functools import cmp_to_key


class TempSpace:
    """统一管理本查询产生的临时文件与临时空间统计。"""

    def __init__(self, base_dir=None):
        self._dir = tempfile.mkdtemp(prefix='windowspill_', dir=base_dir)
        self.bytes_written = 0
        self.files_created = 0
        self.peak_bytes = 0
        self._seq = 0

    @property
    def spill_dir(self):
        return self._dir

    def path(self, prefix):
        self._seq += 1
        self.files_created += 1
        return os.path.join(self._dir, '%s_%06d.jsonl' % (prefix, self._seq))

    def account_write(self, size):
        self.bytes_written += size
        self.peak_bytes = max(self.peak_bytes, self.bytes_written)

    def account_remove(self, size):
        self.bytes_written -= size
        self.peak_bytes = max(self.peak_bytes, self.bytes_written)

    def existing_bytes(self):
        """按临时目录现存文件重算占用，避免级联归并的记账漂移。"""
        total = 0
        for name in os.listdir(self._dir):
            try:
                total += os.path.getsize(os.path.join(self._dir, name))
            except OSError:
                pass
        return total

    def cleanup(self):
        if not os.path.isdir(self._dir):
            return
        for name in os.listdir(self._dir):
            try:
                os.remove(os.path.join(self._dir, name))
            except OSError:
                pass
        try:
            os.rmdir(self._dir)
        except OSError:
            pass


def _scalar_rank(value):
    """分区/排序标量的跨类型次序：null、bool、Decimal数字、字符串。"""
    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        return (2, Decimal(value))
    if isinstance(value, str):
        return (3, value)
    raise ValueError('排序/分区键出现不支持的类型：%r' % (value,))


def _as_decimal(raw, field):
    """行内数值字段契约：十进制字符串或null，与baseline一致。"""
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError('字段%s必须是十进制字符串或null，布尔值不接受' % field)
    try:
        return raw if isinstance(raw, Decimal) else Decimal(raw)
    except (InvalidOperation, TypeError):
        raise ValueError('字段%s必须是十进制字符串或null，收到%r' % (field, raw))


def make_row_comparator(order_keys, partition_fields=()):
    """生成最终物理行序比较器。

    先按PARTITION BY键（标量类型次序null<bool<数字<字符串，同类型自然序）
    分区聚集，再按各ORDER BY键的null次序与升降序比较；业务键全相等时
    以rid兜底，保证输出可复现。rid仅作物理tiebreak，不进入peer判定。
    """
    partition_fields = tuple(partition_fields)

    def compare(a, b):
        for field in partition_fields:
            c = _compare_scalar(a.get(field), b.get(field))
            if c:
                return c
        for key in order_keys:
            field = key.field
            xa = _as_decimal(a.get(field), field)
            xb = _as_decimal(b.get(field), field)
            if xa is None or xb is None:
                if xa is None and xb is None:
                    c = 0
                else:
                    c = (-1 if xa is None else 1) * (1 if key.nulls_first else -1)
            else:
                c = (xa > xb) - (xa < xb)
                if not key.ascending:
                    c = -c
            if c:
                return c
        return (a['rid'] > b['rid']) - (a['rid'] < b['rid'])

    return compare


def _compare_scalar(a, b):
    """分区标量比较：null最小，其余按类型秩再按自然序。"""
    if a is None or b is None:
        if a is None and b is None:
            return 0
        return -1 if a is None else 1
    ra, rb = _scalar_rank(a), _scalar_rank(b)
    if ra[0] != rb[0]:
        return ra[0] - rb[0]
    return (ra[1] > rb[1]) - (ra[1] < rb[1])


class SortStats:
    def __init__(self, signature):
        self.signature = signature
        self.rows = 0
        self.runs_created = 0
        self.merge_rounds = 0
        self.peak_runs_merged = 0
        self.spill_bytes = 0
        self.buffer_peak_rows = 0

    def as_dict(self):
        return {
            'signature': self.signature,
            'rows': self.rows,
            'runs_created': self.runs_created,
            'merge_rounds': self.merge_rounds,
            'peak_runs_merged': self.peak_runs_merged,
            'spill_bytes': self.spill_bytes,
            'buffer_peak_rows': self.buffer_peak_rows,
        }


class ExternalSorter:
    """单个排序签名的外排器。

    run_files: run_id -> 仍可读的run文件路径。run_id在级联归并中保持稳定，
    调试侧车只存(run_id, run内位置, 字节偏移)即可按需定位原始行。
    """

    def __init__(self, temp_space, order_keys, partition_fields, mem_rows,
                 fan_in=8, tag='sort', force_spill=False):
        self.temp = temp_space
        self.order_keys = order_keys
        self.partition_fields = tuple(partition_fields)
        self.mem_rows = mem_rows
        self.fan_in = fan_in
        self.tag = tag
        self.force_spill = force_spill
        self._buffer = []
        self.run_files = {}
        self._all_paths = []
        self._run_counter = 0
        self.stats = SortStats(self._signature_label())
        self._comparator = make_row_comparator(order_keys,
                                               self.partition_fields)
        self._key_func = cmp_to_key(self._comparator)

    def _signature_label(self):
        parts = ['PARTITION BY ' + ', '.join(self.partition_fields)]
        for k in self.order_keys:
            parts.append('ORDER BY %s %s NULLS %s' % (
                k.field, 'ASC' if k.ascending else 'DESC',
                'FIRST' if k.nulls_first else 'LAST'))
        return ' | '.join(parts)

    def partition_tuple(self, row):
        return tuple(row.get(f) for f in self.partition_fields)

    def add(self, row):
        self._buffer.append(row)
        self.stats.rows += 1
        self.stats.buffer_peak_rows = max(
            self.stats.buffer_peak_rows, len(self._buffer))
        if len(self._buffer) >= self.mem_rows:
            self._spill()

    def _new_run_id(self):
        rid = self._run_counter
        self._run_counter += 1
        return rid

    def _write_run(self, rows):
        run_id = self._new_run_id()
        path = self.temp.path('%s_run%d' % (self.tag, run_id))
        ordered = sorted(rows, key=self._key_func)
        payload = '\n'.join(json.dumps(r, ensure_ascii=False, allow_nan=False)
                            for r in ordered)
        data = (payload + '\n').encode('utf-8')
        with open(path, 'wb') as fh:
            fh.write(data)
        self.temp.account_write(len(data))
        self.stats.spill_bytes += len(data)
        self.stats.runs_created += 1
        self.run_files[run_id] = path
        self._all_paths.append(path)
        return run_id, path

    def _spill(self):
        run_id, _path = self._write_run(self._buffer)
        self._buffer = []
        return run_id

    @staticmethod
    def _run_lines(path):
        with open(path, 'rb') as fh:
            pos = 0
            for line in fh:
                yield pos, line
                pos += len(line)

    def iter_sorted(self):
        for row, run_id, pos, offset in self.iter_sorted_with_offsets():
            yield row, run_id, pos

    def iter_sorted_with_offsets(self):
        """输出 (row, run_id, pos_in_run, byte_offset)。"""
        if not self.run_files:
            if self.force_spill and self._buffer:
                run_id = self._spill()
                yield from self._merge_live([run_id])
                return
            ordered = sorted(self._buffer, key=self._key_func)
            self._buffer = []
            for pos, row in enumerate(ordered):
                yield row, None, pos, -1
            return
        if self._buffer:
            self._spill()
        current = list(self.run_files)
        while len(current) > self.fan_in:
            nxt = []
            for i in range(0, len(current), self.fan_in):
                batch = current[i:i + self.fan_in]
                self.stats.peak_runs_merged = max(
                    self.stats.peak_runs_merged, len(batch))
                nxt.append(self._merge_to_run(batch))
                self.stats.merge_rounds += 1
            current = nxt
        self.stats.peak_runs_merged = max(
            self.stats.peak_runs_merged, len(current))
        if len(current) > 1:
            self.stats.merge_rounds += 1
        yield from self._merge_live(current)

    def _merge_to_run(self, input_ids):
        out_id = self._new_run_id()
        out_path = self.temp.path('%s_merge%d' % (self.tag, out_id))
        size = 0
        with open(out_path, 'wb') as out:
            for row, _rid, _pos, _offset in self._merge_stream(input_ids):
                line = (json.dumps(row, ensure_ascii=False, allow_nan=False)
                        + '\n').encode('utf-8')
                out.write(line)
                size += len(line)
        self.temp.account_write(size)
        self.stats.spill_bytes += size
        self.stats.runs_created += 1
        self.run_files[out_id] = out_path
        self._all_paths.append(out_path)
        for rid in input_ids:
            self._discard_run(rid)
        return out_id

    def _merge_live(self, run_ids):
        yield from self._merge_stream(run_ids)

    def _merge_stream(self, run_ids):
        heap = []
        for run_id in run_ids:
            gen = self._run_lines(self.run_files[run_id])
            try:
                offset, line = next(gen)
            except StopIteration:
                continue
            row = json.loads(line.decode('utf-8'))
            heap.append((self._key_func(row), run_id, 0, offset, row, gen))
        heapq.heapify(heap)
        while heap:
            _key, run_id, pos, offset, row, gen = heapq.heappop(heap)
            yield row, run_id, pos, offset
            try:
                next_offset, next_line = next(gen)
            except StopIteration:
                continue
            next_row = json.loads(next_line.decode('utf-8'))
            heapq.heappush(heap, (self._key_func(next_row), run_id,
                                  pos + 1, next_offset, next_row, gen))

    def _discard_run(self, run_id):
        path = self.run_files.pop(run_id, None)
        if path is None:
            return
        try:
            size = os.path.getsize(path)
        except OSError:
            size = 0
        try:
            os.remove(path)
        except OSError:
            return
        self.temp.account_remove(size)

    def cleanup(self):
        for run_id in list(self.run_files):
            self._discard_run(run_id)
        self._buffer = []
