"""ROWS/RANGE/GROUPS窗口帧查询算子。

单遍流式语义（对应需求）：
- 输入来自外排归并流，每个窗口维护单调帧边界与滑动聚合，
  进入帧加贡献、离开帧减贡献，不为任一输出行重扫旧帧；
- 物理稳定行序（业务键+rid tiebreak）只决定ROWS位置和输出次序，
  peer完全由业务ORDER BY键决定，rid不参与对等组划分；
- 帧外单元及时驱逐；UNBOUNDED PRECEDING下旧peer的单元可归档
  （贡献保留在运行聚合中）；UNBOUNDED FOLLOWING的待发引用可外溢；
  调试侧车按分区落定长记录、按索引seek，整分区不重新载回；
- 同一排序遍上的多个窗口各持独立帧状态，复用排序绝不复用/改写帧。
"""
import json
import struct
from decimal import Decimal

from .sortio import _as_decimal

_RECORD = struct.Struct('>iqq i')          # run_id, pos_in_run, byte_offset, gid
_RECORD_SIZE = _RECORD.size


# ---------- 聚合 ---------------------------------------------------------

def zero_agg(measures):
    return {m.alias: {'sum': Decimal(0), 'count': 0} for m in measures}


def contributions(row, measures):
    """单元对各measure的原始标量贡献；COUNT(*)恒为1，COUNT(field)忽略null。"""
    out = {}
    for m in measures:
        if m.fn == 'COUNT':
            out[m.alias] = 1 if m.field == '*' or row.get(m.field) is not None else 0
        else:
            raw = row.get(m.field)
            out[m.alias] = None if raw is None else _as_decimal(raw, m.field)
    return out


def apply_contrib(agg, contrib, measures, sign):
    for m in measures:
        slot = agg[m.alias]
        value = contrib[m.alias]
        if m.fn == 'COUNT':
            slot['count'] += sign * value
        elif value is not None:
            slot['sum'] += sign * value
            slot['count'] += sign


def _round_half_even(dec):
    """AVG始终在Decimal域计算，去掉商的尾随零，不经过二进制浮点。"""
    if dec == dec.to_integral_value():
        return dec.quantize(Decimal(1))
    return dec.normalize()


def format_value(agg, measure):
    slot = agg[measure.alias]
    if measure.fn == 'COUNT':
        return slot['count']
    if slot['count'] == 0:
        return None                           # 空帧sum/avg为null、count为0
    if measure.fn == 'SUM':
        return str(slot['sum'])
    return str(_round_half_even(slot['sum'] / slot['count']))


def same_peer(a_raw, b_raw, field):
    """peer判定只看业务ORDER BY键；null与null同peer，null与非null互异。"""
    if a_raw is None or b_raw is None:
        return a_raw is None and b_raw is None
    return _as_decimal(a_raw, field) == _as_decimal(b_raw, field)


def _encode_ref(ref):
    """backlog外溢编码：Decimal贡献转字符串，保持JSON可序列化。"""
    contrib = {}
    for alias, value in ref['contrib'].items():
        contrib[alias] = str(value) if isinstance(value, Decimal) else value
    return {'idx': ref['idx'], 'gid': ref['gid'], 'rid': ref['rid'],
            'key_raw': ref['key_raw'], 'contrib': contrib}


# ---------- 单窗口状态 ---------------------------------------------------

class WindowState:
    def __init__(self, spec, temp_space, mem_rows, run_files, debug):
        self.spec = spec
        self.temp = temp_space
        self.mem_rows = mem_rows
        self.run_files = run_files            # run_id -> run路径，调试按需seek
        self.debug = debug
        self.frame = spec.frame
        self.key = spec.order_key
        self.measures = spec.measures

        # 分区局部状态
        self.partition = None
        self.entries = []                     # 紧凑帧条目（无整行JSON）
        self.base_idx = 0                     # entries[0]的逻辑行号
        self.peers = []                       # peer元数据，gid = peer_head + 槽位
        self.peer_head = 0
        self.agg = zero_agg(self.measures)
        self.agg_lo = 0
        self.agg_hi = -1
        self.cur_idx = -1
        self.cur_gid = -1
        self._finalizing = False
        self._partition_size = None
        self.pending = []                     # UNBOUNDED FOLLOWING待发紧凑引用
        self.spill_path = None
        self.spilled_refs = 0
        self.sidecar_fh = None
        self.sidecar_base = 0
        self.sidecar_local = 0
        self.partition_seq = 0

        self.stats = {
            'window': spec.name,
            'frame_mode': spec.frame.mode,
            'frame_start': self._label(spec.frame.start),
            'frame_end': self._label(spec.frame.end),
            'exclude': spec.frame.exclude,
            'partitions': 0,
            'rows_emitted': 0,
            'frame_evaluations': 0,
            'entries_evicted': 0,
            'peak_peer_records': 0,
            'backlog_spill_rows': 0,
            'backlog_spill_bytes': 0,
            'debug_sidecar_records': 0,
            'debug_sidecar_bytes': 0,
            'debug_seek_reads': 0,
            'peak_live_entries': 0,
        }
        self.results = []

    @staticmethod
    def _label(b):
        if b.kind == 'UNBOUNDED':
            return 'UNBOUNDED %s' % b.direction
        if b.kind == 'CURRENT':
            return 'CURRENT ROW'
        return '%s %s' % (b.offset, b.direction)

    # ---- 分区切换：null排序规则与帧相等处理在此自然重置 ----------------

    def add_cell(self, row, run_id, pos, offset):
        part = tuple(row.get(f) for f in self.spec.partition_by)
        if self.partition is not None and part != self.partition:
            self._end_partition()
        if self.partition is None:
            self.partition = part
            self.partition_seq += 1
            self.stats['partitions'] += 1
            if self.debug:
                self._open_sidecar()

        idx = self.cur_idx + 1
        gid = self._assign_peer(row, idx)
        entry = {'idx': idx, 'gid': gid, 'rid': row['rid'],
                 'key_raw': row.get(self.key.field),
                 'contrib': contributions(row, self.measures)}
        self.entries.append(entry)
        self.stats['peak_live_entries'] = max(
            self.stats['peak_live_entries'], len(self.entries))
        if self.debug:
            self._write_sidecar(run_id, pos, offset, gid)

        self.cur_idx = idx
        self.cur_gid = gid
        lo, hi = self._resolve_bounds(row)
        self._advance_frame(lo, hi)
        self.stats['frame_evaluations'] += 1

        if self.frame.end.kind == 'UNBOUNDED':
            self._enqueue_pending(entry)
        else:
            self._emit(entry, lo, hi)
        self._release(lo)
    def close(self):
        self._end_partition()
        if self.sidecar_fh is not None:
            self.sidecar_fh.close()
            self.sidecar_fh = None

    def _end_partition(self):
        if self.partition is None:
            return
        if self.frame.end.kind == 'UNBOUNDED':
            # 分区到齐：开放上界此时可解析为真实末行，在完整分区上逐行补算。
            # 边界计算需要全分区peer（不能用cur_idx截断的可见前缀）。
            self._finalizing = True
            self._partition_size = self.cur_idx + 1
            for ref in self._iter_pending():
                entry = self._entry(ref['idx'], nullable=True) or ref
                self.cur_idx = entry['idx']
                self.cur_gid = entry['gid']
                lo, hi = self._resolve_bounds(self._row_view(entry))
                self._advance_frame(lo, hi)
                self._emit(entry, lo, hi)
                self._release(lo)
            self._finalizing = False
            self._partition_size = None
        if self.sidecar_fh is not None:
            self.sidecar_fh.flush()
        self.partition = None
        self.entries = []
        self.base_idx = 0
        self.peers = []
        self.peer_head = 0
        self.agg = zero_agg(self.measures)
        self.agg_lo, self.agg_hi = 0, -1
        self.cur_idx, self.cur_gid = -1, -1
        # 分区切换：待发backlog、侧车基址全部复位，
        # null排序与帧相等处理不会跨分区泄漏
        self._partition_size = None
        self.pending = []
        self.spill_path = None
        self.spilled_refs = 0
        self.sidecar_base += self.sidecar_local
        self.sidecar_local = 0

    # ---- peer：仅业务ORDER BY键，rid绝不参与 ---------------------------

    def _assign_peer(self, row, idx):
        raw = row.get(self.key.field)
        if not self.peers or not same_peer(self.peers[-1]['key_raw'], raw,
                                           self.key.field):
            self.peers.append({'gid': self.peer_head + len(self.peers),
                               'start': idx, 'end': idx, 'key_raw': raw,
                               'agg': zero_agg(self.measures)})
        else:
            self.peers[-1]['end'] = idx
        peer = self.peers[-1]
        apply_contrib(peer['agg'], contributions(row, self.measures),
                      self.measures, 1)
        return peer['gid']

    def _peer(self, gid):
        return self.peers[gid - self.peer_head]

    # ---- 帧边界 ---------------------------------------------------------

    def _resolve_bounds(self, row):
        # 流式阶段UNBOUNDED FOLLOWING尚未到分区末：返回开放上界标记，
        # 由_advance_frame保持聚合延伸而不回收任何单元。
        mode = self.frame.mode
        if (self.frame.end.kind == 'UNBOUNDED'
                and self.frame.end.direction == 'FOLLOWING'
                and not getattr(self, '_finalizing', False)):
            if mode == 'ROWS':
                lo = self._edge_index(self.frame.start, self.cur_idx,
                                      self.cur_idx)
                lo = max(0, lo)
            elif mode == 'GROUPS':
                # 流式阶段结果延迟到finalize，这里lo仅用于延伸聚合；
                # 权威GROUPS边界（含越界判空）在finalize的_groups_bounds。
                max_gid = self.peer_head + len(self.peers) - 1
                glo_raw = self._edge_index(self.frame.start, self.cur_gid,
                                           max_gid)
                glo = max(self.peer_head, min(max_gid, glo_raw))
                lo = self._peer(glo)['start']
            else:
                lo = self._open_range_lo()
            return lo, 'OPEN'
        if mode == 'ROWS':
            return self._rows_bounds()
        if mode == 'GROUPS':
            return self._groups_bounds()
        return self._range_bounds(row)

    def _open_range_lo(self):
        """UNBOUNDED FOLLOWING流式阶段的起点lo：值区间下界对应的最小物理索引。"""
        boundary = self.frame.start
        n = self.cur_idx + 1
        if boundary.kind == 'UNBOUNDED':
            return 0
        peer = self._peer(self.cur_gid)
        raw = peer['key_raw']
        if raw is None or boundary.kind == 'CURRENT':
            return peer['start']
        cur = _as_decimal(raw, self.key.field)
        target = cur + (boundary.offset if boundary.direction == 'FOLLOWING'
                        else -boundary.offset)
        field = self.key.field
        framed = []
        for p in self.peers:
            praw = p['key_raw']
            if praw is None:
                continue
            if _as_decimal(praw, field) >= target:
                framed.extend(range(p['start'], min(p['end'], n - 1) + 1))
        return min(framed) if framed else n

    def _edge_index(self, boundary, anchor, unit_max):
        """unit_max是该单位（物理行号或gid）的合法最大取值，非计数。"""
        if boundary.kind == 'UNBOUNDED':
            return 0 if boundary.direction == 'PRECEDING' else unit_max
        if boundary.kind == 'CURRENT':
            return anchor
        off = boundary.offset
        return anchor - off if boundary.direction == 'PRECEDING' else anchor + off

    def _rows_bounds(self):
        n = self._visible_n()
        max_idx = n - 1
        lo = self._edge_index(self.frame.start, self.cur_idx, max_idx)
        hi = self._edge_index(self.frame.end, self.cur_idx, max_idx)
        return max(0, lo), min(max_idx, hi)

    def _groups_bounds(self):
        # GROUPS边界以gid为单位：anchor=当前gid，单位上界=可见peer的最大gid。
        # FOLLOWING偏移可能越过最后一个peer，此时帧为空（不得夹回到末组）。
        max_gid = self.peer_head + len(self.peers) - 1
        glo_raw = self._edge_index(self.frame.start, self.cur_gid, max_gid)
        ghi_raw = self._edge_index(self.frame.end, self.cur_gid, max_gid)
        if glo_raw > max_gid or ghi_raw < self.peer_head or glo_raw > ghi_raw:
            n = self._visible_n()
            return n, n - 1                          # 空帧
        glo = max(self.peer_head, glo_raw)
        ghi = min(max_gid, ghi_raw)
        return self._peer(glo)['start'], self._peer(ghi)['end']

    def _visible_n(self):
        return self._partition_size if self._partition_size is not None \
            else self.cur_idx + 1

    def _range_bounds(self, row):
        """RANGE按排序键的值区间定义，值区间与ASC/DESC无关。

        三要素：
        1. 值区间[lo_value, hi_value]：CURRENT取当前键，PRECEDING减、
           FOLLOWING加；null当前行退化为仅同null peer；
        2. 在已到达单元(0..cur_idx)中筛出键落入值区间的单元——null不属
           任何值域帧；这些单元在物理行序中必连续（排序输出性质）；
        3. 物理帧[lo,hi]取该连续集合的最小/最大物理索引。
        ASC/DESC只改变键的物理排列，不改变值区间与筛选谓词。
        """
        n = self._partition_size if self._partition_size is not None \
            else self.cur_idx + 1
        cur_peer = self._peer(self.cur_gid)
        current_raw = row.get(self.key.field)
        field = self.key.field

        def value_target(boundary):
            cur = _as_decimal(current_raw, field)
            if boundary.kind == 'CURRENT':
                return cur
            return cur + (boundary.offset if boundary.direction == 'FOLLOWING'
                          else -boundary.offset)

        # null当前行：只纳入同null peer（契约）
        if current_raw is None:
            return cur_peer['start'], cur_peer['end']

        if self.frame.start.kind == 'UNBOUNDED':
            lo_value = None
        else:
            lo_value = value_target(self.frame.start)
        if self.frame.end.kind == 'UNBOUNDED':
            hi_value = None
        else:
            hi_value = value_target(self.frame.end)

        framed = []
        for peer in self.peers:
            raw = peer['key_raw']
            if raw is None:
                continue                       # null不属值域帧
            value = _as_decimal(raw, field)
            if lo_value is not None and value < lo_value:
                continue
            if hi_value is not None and value > hi_value:
                continue
            for phys in range(peer['start'], min(peer['end'], n - 1) + 1):
                framed.append(phys)
        if not framed:
            return n, n - 1                    # 空帧
        return min(framed), max(framed)

    def _key_at(self, index):
        entry = self._entry(index, nullable=True)
        if entry is not None:
            return entry['key_raw']
        for peer in self.peers:                       # 已归档单元从peer恢复键
            if peer['start'] <= index <= peer['end']:
                return peer['key_raw']
        raise RuntimeError('RANGE指针引用了无记录的逻辑行%d' % index)

    # ---- 滑动聚合：进入加、离开减，全部单调 -----------------------------

    def _entry(self, index, nullable=False):
        slot = index - self.base_idx
        if 0 <= slot < len(self.entries):
            return self.entries[slot]
        if nullable:
            return None
        raise RuntimeError('逻辑行%d已离开内存窗口' % index)

    def _advance_frame(self, lo, hi):
        if hi == 'OPEN':
            # 开放上界（UNBOUNDED FOLLOWING流式阶段）：最终帧必含lo到分区末，
            # 此刻只延伸聚合，绝不按lo回收——那些单元在最终帧里仍在帧内。
            while self.agg_hi < self.cur_idx:
                self.agg_hi += 1
                apply_contrib(self.agg, self._entry(self.agg_hi)['contrib'],
                              self.measures, 1)
            return
        if lo > hi:
            # 空帧：显式清空运行聚合；下一非空帧由_align_agg双向重建，
            # 不依赖单调回退假设，空帧的lo=n不会污染下一帧起点。
            self._drain_agg()
            return
        self._align_agg(lo, hi)

    def _drain_agg(self):
        """移出当前运行聚合内的全部单元，得到空聚合。"""
        while self.agg_lo <= self.agg_hi:
            entry = self._entry(self.agg_lo, nullable=True)
            if entry is None:
                break
            apply_contrib(self.agg, entry['contrib'], self.measures, -1)
            self.agg_lo += 1

    def _align_agg(self, lo, hi):
        """把运行聚合对齐到目标[lo,hi]，允许finalize阶段的双向移动。

        有界流式帧的边界天然单调；UNBOUNDED FOLLOWING收尾时空帧与非空帧
        交替，agg_lo可能已越过目标，故这里按需向前/向后调整，成员条目
        在UNBOUNDED FOLLOWING下按_release策略保留到分区末。
        """
        # 回收上界之后的单元（hi可能小于当前agg_hi）
        while self.agg_hi > hi:
            entry = self._entry(self.agg_hi, nullable=True)
            if entry is not None:
                apply_contrib(self.agg, entry['contrib'], self.measures, -1)
            self.agg_hi -= 1
        # 延伸上界
        while self.agg_hi < hi:
            self.agg_hi += 1
            apply_contrib(self.agg, self._entry(self.agg_hi)['contrib'],
                          self.measures, 1)
        # 回收下界之前
        while self.agg_lo < lo:
            entry = self._entry(self.agg_lo, nullable=True)
            if entry is not None:
                apply_contrib(self.agg, entry['contrib'], self.measures, -1)
            self.agg_lo += 1
        # 回补下界（finalize空帧后可能需要）
        while self.agg_lo > lo:
            self.agg_lo -= 1
            apply_contrib(self.agg, self._entry(self.agg_lo)['contrib'],
                          self.measures, 1)

    def _release(self, lo):
        """驱逐不会再被访问的紧凑条目，保证大分区不整分区驻留。

        当前peer及帧内条目不驱逐：EXCLUDE GROUP/TIES与帧部分裁剪需要它们。
        UNBOUNDED FOLLOWING帧在分区末才输出，收尾前任一peer都可能被排除
        调整引用，因此流式阶段不驱逐，随分区结束整体释放；其驻留条目只含
        排序键与标量贡献（无整行JSON），待发引用另由backlog外溢。
        有界帧中更早peer且已离开运行聚合的条目可即时驱逐。
        """
        if self.frame.end.kind == 'UNBOUNDED':
            self.stats['peak_live_entries'] = max(
                self.stats['peak_live_entries'], len(self.entries))
            self.stats['peak_peer_records'] = max(
                self.stats.get('peak_peer_records', 0), len(self.peers))
            return
        cur_peer_start = self._peer(self.cur_gid)['start']
        while self.entries:
            e = self.entries[0]
            if e['idx'] >= self.agg_lo or e['idx'] >= cur_peer_start:
                break
            self.entries.pop(0)
            self.base_idx += 1
            self.stats['entries_evicted'] += 1
        self.stats['peak_live_entries'] = max(
            self.stats['peak_live_entries'], len(self.entries))
        self.stats['peak_peer_records'] = max(
            self.stats.get('peak_peer_records', 0), len(self.peers))

    # ---- 排除规则：只作用于当前行peer与帧的相交部分 --------------------

    def _exclude_adjustment(self, entry, lo, hi):
        """计算应从帧聚合中扣除的排除聚合。

        排除只作用于“当前行peer与帧的相交部分”：当前行若不在帧内，
        CURRENT ROW/TIES/GROUP都不得扣除它（帧外单元本就不在聚合中）。
        """
        rule = self.frame.exclude
        adjustment = zero_agg(self.measures)
        if rule == 'NO OTHERS':
            return adjustment, set()
        peer = self._peer(entry['gid'])
        begin = max(lo, peer['start'])
        end = min(hi, peer['end'])
        if begin > end:
            # 当前peer与帧不相交（常见于FOLLOWING起点越过当前peer）：无排除
            return adjustment, set()
        if rule == 'CURRENT ROW':
            if lo <= entry['idx'] <= hi:
                apply_contrib(adjustment, entry['contrib'],
                              self.measures, 1)
                return adjustment, {entry['idx']}
            return adjustment, set()
        # 相交索引集合：当前peer中真正落在帧[lo,hi]内的单元
        intersect = list(range(begin, end + 1))
        if rule == 'GROUP':
            # 排除整个相交peer段
            for i in intersect:
                live = self._entry(i, nullable=True)
                if live is None:
                    raise RuntimeError('当前peer条目缺失：逻辑行%d' % i)
                apply_contrib(adjustment, live['contrib'], self.measures, 1)
            return adjustment, set(intersect)
        # TIES：排除相交peer段中除当前行外的同伴行（当前行若在帧外，
        # 它本就不在聚合里，绝不参与加减）
        ties_idx = {i for i in intersect if i != entry['idx']}
        for i in ties_idx:
            live = self._entry(i, nullable=True)
            if live is None:
                raise RuntimeError('当前peer条目缺失：逻辑行%d' % i)
            apply_contrib(adjustment, live['contrib'], self.measures, 1)
        return adjustment, ties_idx

    def _emit(self, entry, lo, hi):
        if lo > hi:
            # 空帧：任何排除规则都无成员可排，直接输出空聚合（sum/avg=null,count=0）
            record = {'rid': entry['rid'],
                      'values': {m.alias: format_value(self.agg, m)
                                 for m in self.measures}}
            if self.debug:
                record['debug'] = self._debug_members(entry, lo, hi, set())
            self.results.append(record)
            self.stats['rows_emitted'] += 1
            return
        adjustment, excluded_idx = self._exclude_adjustment(entry, lo, hi)
        values = {}
        if not excluded_idx:
            for m in self.measures:
                values[m.alias] = format_value(self.agg, m)
        else:
            for m in self.measures:
                slot, fix = self.agg[m.alias], adjustment[m.alias]
                if m.fn == 'COUNT':
                    values[m.alias] = slot['count'] - fix['count']
                else:
                    count = slot['count'] - fix['count']
                    if count == 0:
                        values[m.alias] = None
                    elif m.fn == 'SUM':
                        values[m.alias] = str(slot['sum'] - fix['sum'])
                    else:
                        values[m.alias] = str(_round_half_even(
                            (slot['sum'] - fix['sum']) / count))
        record = {'rid': entry['rid'], 'values': values}
        if self.debug:
            record['debug'] = self._debug_members(entry, lo, hi, excluded_idx)
        self.results.append(record)
        self.stats['rows_emitted'] += 1

    # ---- UNBOUNDED FOLLOWING待发引用外溢 --------------------------------

    def _enqueue_pending(self, entry):
        # 引用自包含：紧凑键与贡献随backlog外溢，整行JSON不驻留
        self.pending.append({'idx': entry['idx'], 'gid': entry['gid'],
                             'rid': entry['rid'],
                             'key_raw': entry['key_raw'],
                             'contrib': entry['contrib']})
        if len(self.pending) >= self.mem_rows:
            if self.spill_path is None:
                self.spill_path = self.temp.path(
                    'backlog_%s_p%d' % (self.spec.name, self.partition_seq))
            with open(self.spill_path, 'a', encoding='utf-8') as fh:
                for ref in self.pending:
                    line = json.dumps(_encode_ref(ref), ensure_ascii=False,
                                      allow_nan=False) + '\n'
                    fh.write(line)
                    self.spilled_refs += 1
                    self.stats['backlog_spill_rows'] += 1
                    size = len(line.encode('utf-8'))
                    self.stats['backlog_spill_bytes'] += size
            self.pending = []

    def _iter_pending(self):
        if self.spill_path is not None:
            with open(self.spill_path, 'r', encoding='utf-8') as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    ref = json.loads(line)
                    ref['contrib'] = self._decode_contrib(ref['contrib'])
                    yield ref
        for ref in self.pending:
            yield ref

    def _decode_contrib(self, raw_contrib):
        """按measure类型把外溢贡献还原：COUNT为整数，SUM/AVG为Decimal或null。"""
        out = {}
        for m in self.measures:
            value = raw_contrib[m.alias]
            if m.fn == 'COUNT' or value is None:
                out[m.alias] = value
            else:
                out[m.alias] = Decimal(value)
        return out

    def _row_view(self, entry):
        # RANGE在分区收尾时只需要排序键，构造最小行视图
        return {self.key.field: entry['key_raw']}

    # ---- 调试侧车：每分区定长记录，按索引seek，不整分区载回 ------------

    def _open_sidecar(self):
        path = self.temp.path('sidecar_%s_p%d' %
                              (self.spec.name, self.partition_seq))
        self.sidecar_fh = open(path, 'w+b')
        self.sidecar_local = 0

    def _write_sidecar(self, run_id, pos, offset, gid):
        self.sidecar_fh.write(_RECORD.pack(
            -1 if run_id is None else run_id, pos, offset, gid))
        self.sidecar_local += 1
        self.stats['debug_sidecar_records'] += 1
        self.stats['debug_sidecar_bytes'] += _RECORD_SIZE

    def _read_sidecar(self, logical_idx):
        self.sidecar_fh.seek((self.sidecar_base + logical_idx) * _RECORD_SIZE)
        return _RECORD.unpack(self.sidecar_fh.read(_RECORD_SIZE))

    def _debug_members(self, entry, lo, hi, excluded_idx):
        members = []
        for index in range(lo, hi + 1):
            live = self._entry(index, nullable=True)
            if live is not None:
                rid, gid = live['rid'], live['gid']
            else:
                run_id, _pos, offset, gid = self._read_sidecar(index)
                with open(self.run_files[run_id], 'rb') as fh:
                    fh.seek(offset)
                    row = json.loads(fh.readline().decode('utf-8'))
                rid = row['rid']
                self.stats['debug_seek_reads'] += 1
            members.append({'idx': index, 'peer': gid, 'rid': rid,
                            'excluded': index in excluded_idx})
        return {'frame': [lo, hi], 'members': members}


# ---------- 排序遍驱动 ---------------------------------------------------

class WindowPass:
    """一个排序签名上的一遍流式计算，承载该组全部窗口（帧状态各自独立）。"""

    def __init__(self, specs, sorter, temp_space, mem_rows, debug):
        self.sorter = sorter
        self.states = [WindowState(spec, temp_space, mem_rows,
                                   sorter.run_files, debug) for spec in specs]

    def run(self):
        for row, run_id, pos, offset in self.sorter.iter_sorted_with_offsets():
            for state in self.states:
                state.add_cell(row, run_id, pos, offset)
        for state in self.states:
            state.close()
        return [{
            'window': s.spec.name,
            'partition_by': list(s.spec.partition_by),
            'order_by': [{'field': s.key.field,
                          'ascending': s.key.ascending,
                          'nulls_first': s.key.nulls_first}],
            'rows': s.results,
            'stats': s.stats,
        } for s in self.states]
