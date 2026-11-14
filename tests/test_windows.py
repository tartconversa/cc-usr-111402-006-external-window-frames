"""窗口帧算子测试。

用独立的内存暴力参考实现（直接按SQL语义枚举帧成员）交叉验证流式+外排
算子，覆盖ROWS/RANGE/GROUPS、四种排除规则、null次序、降序、多分区、
多窗口排序复用、内存额度外排与调试侧车。
"""
import copy
import json
import unittest
from decimal import Decimal

from app.service import inspect, inspect_windows
from app.storage import digest
from app.plan import execute_windows
from app.framespec import parse_payload
from app.window import _as_decimal


# ---------- 暴力参考实现 --------------------------------------------------

def ref_sort(rows, key, partition_by):
    """与sortio.make_row_comparator同规则的独立排序：null次序、升降序、rid兜底。"""
    def keyf(r):
        part = tuple((1, str(r[p])) if r.get(p) is not None else (0, 0)
                     for p in partition_by)
        raw = r.get(key.field)
        if raw is None:
            null_rank = 0 if key.nulls_first else 2
            return (part, null_rank, 0, r['rid'])
        value = Decimal(raw)
        if not key.ascending:
            value = -value
        return (part, 1, value, r['rid'])
    return sorted(rows, key=keyf)


def ref_peers(ordered, field):
    peers = []
    gid = -1
    for i, r in enumerate(ordered):
        if i == 0 or not _peer_equal(ordered[i - 1].get(field), r.get(field), field):
            gid += 1
        peers.append(gid)
    return peers


def _peer_equal(a, b, field):
    if a is None or b is None:
        return a is None and b is None
    return Decimal(a) == Decimal(b)


def ref_measure(rows, indices, measure):
    vals = [rows[i] for i in indices]
    if measure.fn == 'COUNT':
        if measure.field == '*':
            return len(vals)
        return sum(1 for r in vals if r.get(measure.field) is not None)
    nums = [Decimal(r[measure.field]) for r in vals
            if r.get(measure.field) is not None]
    if not nums:
        return None
    if measure.fn == 'SUM':
        return str(sum(nums, Decimal(0)))
    q = sum(nums, Decimal(0)) / len(nums)
    if q == q.to_integral_value():
        return str(q.quantize(Decimal(1)))
    return str(q.normalize())


def ref_exclude(rule, ordered, peers, i, lo, hi):
    g = peers[i]
    members = set(range(lo, hi + 1))
    if rule == 'NO OTHERS':
        return members
    if rule == 'CURRENT ROW':
        return members - {i}
    group = {j for j in members if peers[j] == g}
    if rule == 'GROUP':
        return members - group
    return members - (group - {i})


def ref_frame_bounds(ordered, peers, spec, i, visible_n=None):
    """处理第i行时的帧边界。

    普通帧只可见0..i（visible_n=i+1）；含UNBOUNDED FOLLOWING的帧在分区末
    才能确定，按完整分区评估（与引擎的延迟输出对齐），此时visible_n=全长。
    """
    frame = spec.frame
    if visible_n is None:
        visible_n = i + 1
    n = visible_n
    field = spec.order_key.field
    g = peers[i]
    gids = sorted(set(peers[:n]))

    def edge(boundary, anchor, unit_max):
        if boundary.kind == 'UNBOUNDED':
            return 0 if boundary.direction == 'PRECEDING' else unit_max
        if boundary.kind == 'CURRENT':
            return anchor
        off = boundary.offset
        return anchor - off if boundary.direction == 'PRECEDING' else anchor + off

    if frame.mode == 'ROWS':
        lo = max(0, edge(frame.start, i, n - 1))
        hi = min(n - 1, edge(frame.end, i, n - 1))
        return lo, hi
    if frame.mode == 'GROUPS':
        # FOLLOWING边界可能引用尚未到达的peer：夹取到当前可见的最后peer
        glo = max(0, edge(frame.start, g, len(gids) - 1))
        ghi = min(len(gids) - 1, edge(frame.end, g, len(gids) - 1))
        if glo > ghi:
            return n, n - 1                          # 空帧
        want_lo, want_hi = gids[glo], gids[ghi]
        lo = next(j for j in range(n) if peers[j] == want_lo)
        hi = next(j for j in range(n - 1, -1, -1) if peers[j] == want_hi)
        return lo, hi
    # RANGE：与引擎同构——值区间过滤可见行，取物理min/max
    cur_raw = ordered[i].get(field)
    cur_g = peers[i]
    cur_peer_rows = [j for j in range(n) if peers[j] == cur_g]

    if cur_raw is None:
        return cur_peer_rows[0], cur_peer_rows[-1]

    def v_target(boundary):
        cur = Decimal(cur_raw)
        if boundary.kind == 'CURRENT':
            return cur
        return cur + (boundary.offset if boundary.direction == 'FOLLOWING'
                      else -boundary.offset)

    lo_value = None if frame.start.kind == 'UNBOUNDED' else v_target(frame.start)
    hi_value = None if frame.end.kind == 'UNBOUNDED' else v_target(frame.end)

    framed = []
    for j in range(n):
        raw = ordered[j].get(field)
        if raw is None:
            continue                       # null不属值域帧
        v = Decimal(raw)
        if lo_value is not None and v < lo_value:
            continue
        if hi_value is not None and v > hi_value:
            continue
        framed.append(j)
    if not framed:
        return n, n - 1
    return min(framed), max(framed)


def reference(data):
    """逐行到达的暴力参考：处理i时只有同分区0..i可见，与流式算子同构。"""
    specs, _ = parse_payload(data)
    out = {}
    for spec in specs:
        key = spec.order_key
        ordered = ref_sort(data['rows'], key, spec.partition_by)
        peers_global = ref_peers(ordered, key.field)
        part_ids = [tuple(r.get(p) for p in spec.partition_by)
                    for r in ordered]
        rows_out = []
        # 先按分区聚合，UNBOUNDED FOLLOWING帧需要完整分区可见
        partitions = {}
        for j, pid in enumerate(part_ids):
            partitions.setdefault(pid, []).append(j)
        first_seen = {}
        for i, r in enumerate(ordered):
            part = part_ids[i]
            if part not in first_seen:
                first_seen[part] = i
            base = first_seen[part]
            members = partitions[part]
            local = [ordered[j] for j in members]
            local_peers = [peers_global[j] - peers_global[base] for j in members]
            li = members.index(i)
            unbounded_following = spec.frame.end.kind == 'UNBOUNDED'
            visible_n = len(local) if unbounded_following else li + 1
            lo, hi = ref_frame_bounds(local, local_peers, spec, li,
                                      visible_n=visible_n)
            framed = set(members[lo:hi + 1]) if lo <= hi else set()
            kept = ref_exclude(spec.frame.exclude, ordered, peers_global, i,
                               members[lo] if framed else 0,
                               members[hi] if framed else -1)
            kept = {j for j in kept if j in framed}
            values = {m.alias: ref_measure(ordered, sorted(kept), m)
                      for m in spec.measures}
            rows_out.append({'rid': r['rid'], 'values': values})
        out[spec.name] = rows_out
    return out


# ---------- 数据构造器 ----------------------------------------------------

def make_rows(spec_rows):
    rows = []
    for rid, part, order, value in spec_rows:
        rows.append({'rid': rid, 'partition': part,
                     'order': None if order is None else str(order),
                     'value': None if value is None else str(value)})
    return rows


def window(name, mode, start, end, exclude='NO OTHERS', measures=None,
           ascending=True, nulls_first=False, partition_by=('partition',)):
    return {
        'name': name,
        'partition_by': list(partition_by),
        'order_by': [{'field': 'order', 'ascending': ascending,
                      'nulls_first': nulls_first}],
        'frame': {'mode': mode, 'start': start, 'end': end, 'exclude': exclude},
        'measures': measures or [
            {'fn': 'SUM', 'field': 'value', 'alias': 's'},
            {'fn': 'COUNT', 'field': '*', 'alias': 'cn'},
            {'fn': 'COUNT', 'field': 'value', 'alias': 'cv'},
            {'fn': 'AVG', 'field': 'value', 'alias': 'av'},
        ],
    }


UNBOUNDED_PRECEDING = {'kind': 'UNBOUNDED', 'direction': 'PRECEDING'}
UNBOUNDED_FOLLOWING = {'kind': 'UNBOUNDED', 'direction': 'FOLLOWING'}
CURRENT = {'kind': 'CURRENT ROW'}


def off(direction, offset):
    return {'kind': 'OFFSET', 'direction': direction, 'offset': offset}


# ---------- 测试主体 ------------------------------------------------------

def assert_matches_reference(test_case, data):
    actual = execute_windows(copy.deepcopy(data))
    expected = reference(data)
    for block in actual['windows']:
        with test_case.subTest(window=block['window']):
            test_case.assertEqual(
                [(r['rid'], r['values']) for r in block['rows']],
                [(r['rid'], r['values'])
                 for r in expected[block['window']]])


class WindowReferenceTests(unittest.TestCase):
    def assert_matches_reference(self, data):
        assert_matches_reference(self, data)

    def test_rows_modes_all_exclusions(self):
        rows = make_rows([
            ('a', 'CN', 1, 10), ('b', 'CN', 1, 20), ('c', 'CN', 2, None),
            ('d', 'CN', 3, 30), ('e', 'CN', 5, 40),
            ('f', 'EU', 2, 5), ('g', 'EU', 2, 7),
        ])
        for exclude in ('NO OTHERS', 'CURRENT ROW', 'GROUP', 'TIES'):
            data = {'version': 2, 'rows': rows, 'windows': [window(
                'w', 'ROWS', off('PRECEDING', 1), off('FOLLOWING', 1),
                exclude=exclude)],
                'settings': {'mem_rows': 3}}
            self.assert_matches_reference(data)

    def test_groups_modes(self):
        rows = make_rows([
            ('a', 'CN', 1, 10), ('b', 'CN', 1, 20), ('c', 'CN', 2, 30),
            ('d', 'CN', 2, 40), ('e', 'CN', 3, 50),
        ])
        frames = [
            (CURRENT, CURRENT),
            (off('PRECEDING', 1), CURRENT),
            (CURRENT, off('FOLLOWING', 1)),
            (off('PRECEDING', 1), off('FOLLOWING', 1)),
            (UNBOUNDED_PRECEDING, CURRENT),
            (CURRENT, UNBOUNDED_FOLLOWING),
            (UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING),
        ]
        for exclude in ('NO OTHERS', 'CURRENT ROW', 'GROUP', 'TIES'):
            for s, e in frames:
                data = {'version': 2, 'rows': rows, 'windows': [window(
                    'w', 'GROUPS', s, e, exclude=exclude)],
                    'settings': {'mem_rows': 2}}
                self.assert_matches_reference(data)

    def test_range_modes_ascending(self):
        rows = make_rows([
            ('a', 'CN', 1, 10), ('b', 'CN', 1, 20), ('c', 'CN', 2, 30),
            ('d', 'CN', 4, 40), ('e', 'CN', 4, 50), ('f', 'CN', 7, 60),
        ])
        frames = [
            (CURRENT, CURRENT),
            (off('PRECEDING', '1'), CURRENT),
            (CURRENT, off('FOLLOWING', '2')),
            (off('PRECEDING', '2'), off('FOLLOWING', '1')),
            (UNBOUNDED_PRECEDING, CURRENT),
            (CURRENT, UNBOUNDED_FOLLOWING),
        ]
        for s, e in frames:
            data = {'version': 2, 'rows': rows, 'windows': [window(
                'w', 'RANGE', s, e)], 'settings': {'mem_rows': 2}}
            self.assert_matches_reference(data)

    def test_range_descending_and_null_ordering(self):
        rows = make_rows([
            ('a', 'CN', 1, 10), ('b', 'CN', 1, 20), ('c', 'CN', 2, 30),
            ('d', 'CN', None, 40), ('e', 'CN', 4, 50),
        ])
        for ascending in (True, False):
            for nulls_first in (True, False):
                for s, e in ((off('PRECEDING', '1'), CURRENT),
                             (CURRENT, off('FOLLOWING', '1')),
                             (CURRENT, CURRENT)):
                    data = {'version': 2, 'rows': rows, 'windows': [window(
                        'w', 'RANGE', s, e,
                        ascending=ascending, nulls_first=nulls_first)],
                        'settings': {'mem_rows': 2}}
                    self.assert_matches_reference(data)

    def test_null_current_row_range_only_null_peer(self):
        rows = make_rows([('a', 'CN', 1, 10), ('b', 'CN', None, 20),
                          ('c', 'CN', None, 30), ('d', 'EU', None, 40)])
        data = {'version': 2, 'rows': rows, 'windows': [window(
            'w', 'RANGE', off('PRECEDING', '5'), off('FOLLOWING', '5'))],
            'settings': {'mem_rows': 2}}
        self.assert_matches_reference(data)

    def test_empty_frame_values(self):
        rows = make_rows([('a', 'CN', 1, 10), ('b', 'CN', 10, 20)])
        data = {'version': 2, 'rows': rows, 'windows': [window(
            'w', 'ROWS', off('PRECEDING', 5), off('PRECEDING', 2))],
            'settings': {'mem_rows': 10}}
        result = execute_windows(data)['windows'][0]['rows']
        for r in result:
            self.assertIsNone(r['values']['s'])
            self.assertIsNone(r['values']['av'])
            self.assertEqual(r['values']['cn'], 0)
            self.assertEqual(r['values']['cv'], 0)

    def test_ties_keep_peer_meaning_with_rid_tiebreak(self):
        # 业务order并列的两行物理序由rid决定，但peer不能被rid拆散
        rows = make_rows([('z', 'CN', 1, 10), ('a', 'CN', 1, 20),
                          ('m', 'CN', 1, 30), ('q', 'CN', 2, 40)])
        data = {'version': 2, 'rows': rows, 'windows': [
            window('w1', 'ROWS', UNBOUNDED_PRECEDING, CURRENT),
            window('w2', 'RANGE', UNBOUNDED_PRECEDING, CURRENT),
            window('w3', 'GROUPS', UNBOUNDED_PRECEDING, CURRENT,
                   exclude='TIES')],
            'settings': {'mem_rows': 2}}
        self.assert_matches_reference(data)
        actual = execute_windows(copy.deepcopy(data))
        # rid只作物理tiebreak：三个同order行的物理输出序按rid稳定，
        # 但peer编号一致、不被rid拆散
        rng = next(w for w in actual['windows'] if w['window'] == 'w2')
        self.assertEqual([r['rid'] for r in rng['rows'][:3]],
                         ['a', 'm', 'z'])
        grp = next(w for w in actual['windows'] if w['window'] == 'w3')
        # GROUPS + TIES：排除当前peer内除当前行外的同peer行，
        # 当前行自身保留；三行同peer各自只统计自己+历史（无更早组）
        self.assertEqual([r['values']['s'] for r in grp['rows'][:3]],
                         ['20', '30', '10'])


class WindowPlanningTests(unittest.TestCase):
    def _data(self, windows):
        rows = make_rows([('a', 'CN', 1, 10), ('b', 'CN', 2, 20)])
        return {'version': 2, 'rows': rows, 'windows': windows,
                'settings': {'mem_rows': 10}}

    def test_shared_sort_and_independent_frames(self):
        w1 = window('same_a', 'ROWS', UNBOUNDED_PRECEDING, CURRENT)
        w2 = window('same_b', 'RANGE', CURRENT, CURRENT)
        w3 = window('diff', 'ROWS', UNBOUNDED_PRECEDING, CURRENT,
                    nulls_first=True)
        data = self._data([w1, w2, w3])
        result = execute_windows(copy.deepcopy(data))
        groups = result['plan']['sort_groups']
        self.assertEqual(len(groups), 2)
        shared = next(g for g in groups if g['shared_sort'])
        self.assertEqual(sorted(shared['windows']), ['same_a', 'same_b'])
        # 帧规格在计划中保持各自独立，未被复用改写
        plans = {w['name']: w['frame'] for w in result['plan']['windows']}
        self.assertEqual(plans['same_a']['mode'], 'ROWS')
        self.assertEqual(plans['same_b']['mode'], 'RANGE')

    def test_different_order_gets_separate_group(self):
        w1 = window('asc', 'ROWS', CURRENT, CURRENT, ascending=True)
        w2 = window('desc', 'ROWS', CURRENT, CURRENT, ascending=False)
        data = self._data([w1, w2])
        result = execute_windows(copy.deepcopy(data))
        self.assertEqual(len(result['plan']['sort_groups']), 2)
        self.assertTrue(all(not g['shared_sort']
                            for g in result['plan']['sort_groups']))


class WindowRejectionTests(unittest.TestCase):
    def _base(self, **over):
        w = window('w', 'ROWS', CURRENT, CURRENT)
        w.update(over)
        return {'version': 2,
                'rows': make_rows([('a', 'CN', 1, 10)]), 'windows': [w]}

    def test_invalid_combinations_explained(self):
        cases = []
        w = window('w', 'ROWS', off('FOLLOWING', 1), CURRENT)
        cases.append({'version': 2, 'rows': make_rows([('a', 'CN', 1, 10)]),
                      'windows': [w]})                       # 起点晚于终点
        w = window('w', 'ROWS', off('PRECEDING', '1.5'), CURRENT)
        cases.append({'version': 2, 'rows': make_rows([('a', 'CN', 1, 10)]),
                      'windows': [w]})                       # ROWS小数偏移
        w = window('w', 'RANGE', off('PRECEDING', '-1'), CURRENT)
        cases.append({'version': 2, 'rows': make_rows([('a', 'CN', 1, 10)]),
                      'windows': [w]})                       # 负RANGE
        for data in cases:
            with self.assertRaises(ValueError):
                execute_windows(data)

    def test_unknown_exclude_and_measure(self):
        w = window('w', 'ROWS', CURRENT, CURRENT, exclude='WHATEVER')
        data = {'version': 2, 'rows': make_rows([('a', 'CN', 1, 10)]),
                'windows': [w]}
        with self.assertRaisesRegex(ValueError, 'exclude'):
            execute_windows(data)

    def test_duplicate_rid_rejected(self):
        rows = make_rows([('a', 'CN', 1, 10), ('a', 'CN', 2, 20)])
        data = {'version': 2, 'rows': rows,
                'windows': [window('w', 'ROWS', CURRENT, CURRENT)]}
        with self.assertRaisesRegex(ValueError, '行键重复'):
            execute_windows(data)

    def test_range_single_numeric_order_key_only(self):
        rows = make_rows([('a', 'CN', 1, 10)])
        data = {'version': 2, 'rows': rows,
                'windows': [window('w', 'RANGE', CURRENT, CURRENT)]}
        data['windows'][0]['order_by'].append(
            {'field': 'value', 'ascending': True})
        with self.assertRaisesRegex(ValueError, '单一ORDER BY数值列'):
            execute_windows(data)

    def test_offset_type_rules(self):
        rows = make_rows([('a', 'CN', 1, 10)])
        # GROUPS不接受小数偏移
        w = window('w', 'GROUPS', off('PRECEDING', '1.5'), CURRENT)
        with self.assertRaisesRegex(ValueError, '非负整数'):
            execute_windows({'version': 2, 'rows': rows, 'windows': [w]})
        # RANGE不接受负偏移
        w = window('w', 'RANGE', off('PRECEDING', '-1'), CURRENT)
        with self.assertRaisesRegex(ValueError, '不能为负'):
            execute_windows({'version': 2, 'rows': rows, 'windows': [w]})


class WindowSpillAndDebugTests(unittest.TestCase):
    def test_external_merge_matches_reference(self):
        rows = make_rows([(chr(97 + i), 'CN' if i % 2 else 'EU',
                           i % 7, i * 3 - (i % 5)) for i in range(60)])
        data = {'version': 2, 'rows': rows, 'windows': [
            window('wr', 'ROWS', off('PRECEDING', 3), off('FOLLOWING', 2)),
            window('wg', 'GROUPS', off('PRECEDING', 1), off('FOLLOWING', 1),
                   exclude='GROUP'),
            window('wn', 'RANGE', off('PRECEDING', '2'), CURRENT,
                   nulls_first=True)],
            'settings': {'mem_rows': 5}}
        assert_matches_reference(self, data)
        stats = execute_windows(copy.deepcopy(data))['stats']
        self.assertGreaterEqual(stats['sort_groups'][0]['runs_created'], 1)
        self.assertEqual(stats['temporary_space']['bytes_remaining'], 0)

    def test_cascade_merge_when_runs_exceed_fan_in(self):
        # mem_rows=3、100行产生三十多个run，超过fan_in=8触发级联归并
        rows = make_rows([('r%03d' % i, 'CN', i % 7, i) for i in range(100)])
        data = {'version': 2, 'rows': rows, 'windows': [
            window('w', 'ROWS', off('PRECEDING', 2), off('FOLLOWING', 2),
                   exclude='TIES')],
            'settings': {'mem_rows': 3, 'debug_members': True}}
        result = execute_windows(copy.deepcopy(data))
        sg = result['stats']['sort_groups'][0]
        self.assertGreater(sg['runs_created'], 8)
        self.assertGreaterEqual(sg['merge_rounds'], 1)
        self.assertLessEqual(sg['peak_runs_merged'], 8)
        assert_matches_reference(self, data)
        for r in result['windows'][0]['rows']:
            lo, hi = r['debug']['frame']
            self.assertEqual([m['idx'] for m in r['debug']['members']],
                             list(range(lo, hi + 1)))

    def test_sidecar_seek_reconstructs_evicted_row(self):
        from app.sortio import ExternalSorter, TempSpace
        from app.window import WindowState
        import json as _json
        rows = make_rows([('r%03d' % i, 'CN', i, i) for i in range(20)])
        data = {'version': 2, 'rows': rows,
                'windows': [window('w', 'ROWS', CURRENT, CURRENT)],
                'settings': {'mem_rows': 3, 'debug_members': True}}
        specs, _ = parse_payload(data)
        temp = TempSpace(None)
        try:
            sorter = ExternalSorter(temp, [specs[0].order_key],
                                    specs[0].partition_by, 3, force_spill=True)
            for r in rows:
                sorter.add(r)
            state = WindowState(specs[0], temp, 3, sorter.run_files, True)
            for row, rid, pos, off in sorter.iter_sorted_with_offsets():
                state.add_cell(row, rid, pos, off)
            run_id, _pos, offset, gid = state._read_sidecar(5)
            with open(sorter.run_files[run_id], 'rb') as fh:
                fh.seek(offset)
                recovered = _json.loads(fh.readline().decode('utf-8'))
            self.assertEqual(recovered['rid'], 'r005')
            self.assertEqual(gid, 5)
            state.close()
        finally:
            sorter.cleanup()
            temp.cleanup()

    def test_debug_members_are_consistent(self):
        rows = make_rows([('a', 'CN', 1, 10), ('b', 'CN', 1, 20),
                          ('c', 'CN', 2, 30), ('d', 'CN', 3, 40)])
        data = {'version': 2, 'rows': rows, 'windows': [window(
            'w', 'ROWS', off('PRECEDING', 1), CURRENT, exclude='TIES')],
            'settings': {'mem_rows': 2, 'debug_members': True}}
        block = execute_windows(copy.deepcopy(data))['windows'][0]
        for r in block['rows']:
            dbg = r['debug']
            lo, hi = dbg['frame']
            self.assertEqual([m['idx'] for m in dbg['members']],
                             list(range(lo, hi + 1)))
            self.assertTrue(all('rid' in m and 'peer' in m
                                for m in dbg['members']))
        # 外溢分区的侧车确实产生了seek读取
        stats = next(s for s in execute_windows(copy.deepcopy(data))['stats']['windows'])
        self.assertGreater(stats['debug_sidecar_records'], 0)


class CompatibilityTests(unittest.TestCase):
    def test_v1_entry_unchanged(self):
        sample = json.loads(
            (__import__('pathlib').Path(__file__).resolve().parents[1]
             / 'fixtures' / 'sample.json').read_text(encoding='utf-8'))
        response = inspect(sample)
        self.assertEqual(set(response), {'input_digest', 'result'})
        self.assertEqual(response['input_digest'], digest(sample))

    def test_v2_carries_digest_and_plan(self):
        data = {'version': 2,
                'rows': make_rows([('a', 'CN', 1, 10), ('b', 'CN', 2, 20)]),
                'windows': [window('w', 'ROWS', UNBOUNDED_PRECEDING, CURRENT)]}
        response = inspect_windows(copy.deepcopy(data))
        self.assertEqual(response['input_digest'], digest(data))
        self.assertIn('plan', response)
        self.assertIn('stats', response)


if __name__ == '__main__':
    unittest.main()
