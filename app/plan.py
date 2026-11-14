"""窗口查询计划：排序复用判定、输入校验与执行编排。

复用规则（对应需求）：
- partition_by与ORDER BY（字段、升降序、null次序）完全相同的窗口共用一次
  外排与一遍流式计算，计划中显式标注shared_sort；
- 排序不同的窗口分列不同sort_group，给出各自的实际计划；
- 复用仅发生在排序层，每个窗口始终保留自己的帧规格与独立帧状态，
  不存在为复用而改写帧的路径。
"""
import os

from .framespec import parse_payload
from .sortio import TempSpace, ExternalSorter, _as_decimal
from .window import WindowPass


def _validate_rows(rows, specs):
    """行级契约校验，错误信息沿用baseline措辞以保持一致体验。"""
    if not isinstance(rows, list) or not rows:
        raise ValueError('rows必须是非空数组')
    seen = set()
    numeric_fields = set()
    present_fields = {'rid'}
    for spec in specs:
        present_fields.update(spec.partition_by)
        numeric_fields.add(spec.order_key.field)
        present_fields.add(spec.order_key.field)
        for m in spec.measures:
            if m.field != '*':
                numeric_fields.add(m.field)
                present_fields.add(m.field)
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('每行必须是对象')
        rid = row.get('rid')
        if not isinstance(rid, str) or not rid:
            raise ValueError('rid必须是非空字符串')
        if rid in seen:
            raise ValueError('行键重复')
        seen.add(rid)
        for field in present_fields - {'rid'}:
            if field not in row:
                raise ValueError('行%s缺少字段%s' % (rid, field))
            value = row[field]
            if value is None:
                continue
            if not isinstance(value, (str, int)) or isinstance(value, bool):
                raise ValueError('字段%s必须是十进制字符串或null，收到%r'
                                 % (field, value))
            if field in numeric_fields:
                _as_decimal(value, field)        # 非十进制字符串在此拒绝


def _group_by_sort(specs):
    groups = []
    index = {}
    for spec in specs:
        sig = spec.sort_signature()
        if sig not in index:
            index[sig] = len(groups)
            groups.append({'signature': sig, 'specs': []})
        groups[index[sig]]['specs'].append(spec)
    return groups


def build_plan(specs, settings):
    groups = _group_by_sort(specs)
    plan = {'sort_groups': [], 'windows': []}
    for gi, group in enumerate(groups):
        first = group['specs'][0]
        sort_group = {
            'sort_group_id': gi,
            'partition_by': list(first.partition_by),
            'order_by': [{'field': first.order_key.field,
                          'ascending': first.order_key.ascending,
                          'nulls_first': first.order_key.nulls_first}],
            'shared_sort': len(group['specs']) > 1,
            'windows': [s.name for s in group['specs']],
        }
        plan['sort_groups'].append(sort_group)
        for spec in group['specs']:
            plan['windows'].append({
                'name': spec.name,
                'sort_group_id': gi,
                'frame': {
                    'mode': spec.frame.mode,
                    'start': _boundary(spec.frame.start),
                    'end': _boundary(spec.frame.end),
                    'exclude': spec.frame.exclude,
                },
                'measures': [{'fn': m.fn, 'field': m.field, 'alias': m.alias}
                             for m in spec.measures],
            })
    return plan


def _boundary(b):
    if b.kind == 'UNBOUNDED':
        return {'kind': 'UNBOUNDED', 'direction': b.direction}
    if b.kind == 'CURRENT':
        return {'kind': 'CURRENT ROW'}
    offset = str(b.offset) if not isinstance(b.offset, int) else b.offset
    return {'kind': 'OFFSET', 'direction': b.direction, 'offset': offset}


def execute_windows(data):
    """v2窗口查询算子入口，返回计划、逐窗口结果与公开统计。"""
    specs, settings = parse_payload(data)
    _validate_rows(data['rows'], specs)
    groups = _group_by_sort(specs)

    temp = TempSpace(settings['tmp_dir'])
    sorters = []
    try:
        window_outputs = []
        sort_stats = []
        for gi, group in enumerate(groups):
            first = group['specs'][0]
            sorter = ExternalSorter(
                temp, [first.order_key],
                first.partition_by, settings['mem_rows'],
                tag='g%d' % gi, force_spill=settings['debug_members'])
            for row in data['rows']:
                sorter.add(row)
            sorters.append(sorter)
            passes = WindowPass(group['specs'], sorter, temp,
                                settings['mem_rows'],
                                settings['debug_members']).run()
            window_outputs.extend(passes)
            # 归并在WindowPass运行时才发生，统计须在遍跑完后快照
            sort_stats.append(sorter.stats.as_dict())

        plan = build_plan(specs, settings)
        stats = {
            'sort_groups': sort_stats,
            'windows': [out['stats'] for out in window_outputs],
            'temporary_space': {
                'files_created': temp.files_created,
                'peak_bytes': temp.peak_bytes,
                'bytes_remaining': None,
            },
            'memory': {
                'mem_rows': settings['mem_rows'],
                'window_peak_live_entries': max(
                    (out['stats']['peak_live_entries']
                     for out in window_outputs), default=0),
            },
        }
        result = {'plan': plan, 'windows': window_outputs, 'stats': stats}
        for sorter in sorters:
            sorter.cleanup()
        temp.cleanup()
        # 临时目录整体回收后回填：正常结束残留为0，异常清理失败才非0
        stats['temporary_space']['bytes_remaining'] = (
            temp.existing_bytes() if os.path.isdir(temp.spill_dir) else 0)
        return result
    finally:
        temp.cleanup()
