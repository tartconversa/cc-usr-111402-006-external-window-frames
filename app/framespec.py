"""窗口帧规格的解析与校验。

v2资料在rows之外声明windows列表。本模块只负责把普通dict转成不可变规格，
并把所有非法组合以中文ValueError解释拒绝，不执行任何数据计算。

契约范围（docs/data-contract.md）：
- 单ORDER BY数值列，值为十进制字符串或null；
- ROWS/GROUPS偏移为非负整数，RANGE偏移为非负Decimal；
- RANGE值偏移仅支持单一数值排序键；
- null与非null互为不同peer；空帧sum/avg为null、count为0。
"""
from decimal import Decimal, InvalidOperation

FRAME_MODES = ('ROWS', 'GROUPS', 'RANGE')
EXCLUDE_RULES = ('NO OTHERS', 'CURRENT ROW', 'GROUP', 'TIES')
AGG_FNS = ('SUM', 'COUNT', 'AVG')


class OrderKey:
    __slots__ = ('field', 'ascending', 'nulls_first')

    def __init__(self, field, ascending, nulls_first):
        self.field = field
        self.ascending = bool(ascending)
        self.nulls_first = bool(nulls_first)

    def signature(self):
        return (self.field, self.ascending, self.nulls_first)


class Boundary:
    """帧边界。kind ∈ UNBOUNDED/CURRENT/OFFSET。"""
    __slots__ = ('kind', 'direction', 'offset')

    def __init__(self, kind, direction=None, offset=None):
        self.kind = kind
        self.direction = direction          # PRECEDING / FOLLOWING
        self.offset = offset                # int(ROWS/GROUPS) 或 Decimal(RANGE)

    def rank(self):
        """边界在帧方向上的先后次序，用于静态校验start不得晚于end。"""
        if self.kind == 'UNBOUNDED':
            return (0, 0) if self.direction == 'PRECEDING' else (4, 0)
        if self.kind == 'CURRENT':
            return (2, 0)
        if self.direction == 'PRECEDING':
            return (1, -int(self.offset) if isinstance(self.offset, int) else -self.offset)
        return (3, self.offset)


class Frame:
    __slots__ = ('mode', 'start', 'end', 'exclude')

    def __init__(self, mode, start, end, exclude):
        self.mode = mode
        self.start = start
        self.end = end
        self.exclude = exclude


class Measure:
    __slots__ = ('fn', 'field', 'alias')

    def __init__(self, fn, field, alias):
        self.fn = fn
        self.field = field                  # '*' 仅COUNT允许，表示计行
        self.alias = alias


class WindowSpec:
    __slots__ = ('name', 'partition_by', 'order_key', 'frame', 'measures')

    def __init__(self, name, partition_by, order_key, frame, measures):
        self.name = name
        self.partition_by = tuple(partition_by)
        self.order_key = order_key          # v2恰好一个OrderKey
        self.frame = frame
        self.measures = tuple(measures)

    def sort_signature(self):
        return (self.partition_by, (self.order_key.signature(),))


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _parse_boundary(raw, mode, side):
    if not isinstance(raw, dict):
        raise ValueError('帧%s边界必须是对象' % side)
    kind = raw.get('kind')
    if kind == 'UNBOUNDED':
        direction = raw.get('direction')
        _require(direction in ('PRECEDING', 'FOLLOWING'),
                 "UNBOUNDED边界的direction必须是PRECEDING或FOLLOWING，收到%r" % direction)
        if side == 'start':
            _require(direction == 'PRECEDING', '帧起点只能是UNBOUNDED PRECEDING')
        else:
            _require(direction == 'FOLLOWING', '帧终点只能是UNBOUNDED FOLLOWING')
        return Boundary('UNBOUNDED', direction)
    if kind == 'CURRENT ROW':
        return Boundary('CURRENT')
    if kind == 'OFFSET':
        direction = raw.get('direction')
        _require(direction in ('PRECEDING', 'FOLLOWING'),
                 "OFFSET边界的direction必须是PRECEDING或FOLLOWING，收到%r" % direction)
        raw_offset = raw.get('offset')
        if mode in ('ROWS', 'GROUPS'):
            offset = _parse_int_offset(raw_offset)
        else:
            offset = _parse_decimal_offset(raw_offset)
        return Boundary('OFFSET', direction, offset)
    raise ValueError('未知帧边界类型%r，合法值为UNBOUNDED/CURRENT ROW/OFFSET' % kind)


def _parse_int_offset(raw):
    if isinstance(raw, bool):
        raise ValueError('ROWS/GROUPS帧偏移必须是非负整数，布尔值不接受')
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.strip().isdigit():
        value = int(raw.strip())
    else:
        raise ValueError('ROWS/GROUPS帧偏移必须是非负整数，收到%r' % raw)
    _require(value >= 0, 'ROWS/GROUPS帧偏移不能为负，收到%d' % value)
    return value


def _parse_decimal_offset(raw):
    if isinstance(raw, bool):
        raise ValueError('RANGE帧偏移必须是非负十进制数，布尔值不接受')
    try:
        if isinstance(raw, int):
            value = Decimal(raw)
        elif isinstance(raw, str):
            value = Decimal(raw.strip())
        else:
            raise ValueError
    except (InvalidOperation, ValueError):
        raise ValueError('RANGE帧偏移必须是非负十进制数（字符串或整数），收到%r' % raw)
    _require(value.is_finite(), 'RANGE帧偏移必须有限，收到%r' % raw)
    _require(value >= 0, 'RANGE帧偏移不能为负，收到%s' % value)
    return value


def _parse_order_key(raw):
    if not isinstance(raw, dict):
        raise ValueError('order_by元素必须是对象')
    field = raw.get('field')
    _require(isinstance(field, str) and field, 'order_by.field必须是非空字符串')
    ascending = raw.get('ascending', True)
    nulls_first = raw.get('nulls_first', False)
    _require(isinstance(ascending, bool) and isinstance(nulls_first, bool),
             'ascending/nulls_first必须是布尔值')
    return OrderKey(field, ascending, nulls_first)


def _parse_frame(raw, order_count):
    if not isinstance(raw, dict):
        raise ValueError('frame必须是对象')
    mode = str(raw.get('mode', '')).upper()
    _require(mode in FRAME_MODES,
             '帧模式必须是ROWS/RANGE/GROUPS之一，收到%r' % raw.get('mode'))
    _require(order_count == 1,
             '当前契约仅支持单一ORDER BY数值列，收到%d个排序键；%s帧无法按多键定义对等组'
             % (order_count, mode))
    if mode == 'RANGE':
        # 值域帧的偏移要对排序键做十进制加减，多键或非数值键没有统一定义。
        _require(order_count == 1,
                 'RANGE带值偏移仅支持单一数值排序键，多ORDER BY组合被拒绝')
    start = _parse_boundary(raw.get('start'), mode, 'start')
    end = _parse_boundary(raw.get('end'), mode, 'end')
    _require(start.rank() <= end.rank(),
             '帧起点%s不得晚于帧终点%s' % (_describe_boundary(start), _describe_boundary(end)))
    exclude = str(raw.get('exclude', 'NO OTHERS')).upper()
    _require(exclude in EXCLUDE_RULES,
             'exclude必须是NO OTHERS/CURRENT ROW/GROUP/TIES之一，收到%r' % raw.get('exclude'))
    return Frame(mode, start, end, exclude)


def _describe_boundary(b):
    if b.kind == 'UNBOUNDED':
        return 'UNBOUNDED %s' % b.direction
    if b.kind == 'CURRENT':
        return 'CURRENT ROW'
    return '%s %s' % (b.offset, b.direction)


def _parse_measure(raw, window_name):
    if not isinstance(raw, dict):
        raise ValueError('measures元素必须是对象')
    fn = str(raw.get('fn', '')).upper()
    _require(fn in AGG_FNS, '聚合函数必须是SUM/COUNT/AVG之一，收到%r' % raw.get('fn'))
    field = raw.get('field')
    if fn == 'COUNT':
        _require(field is not None, "COUNT需要field；计行请显式使用field:'*'")
        if field != '*':
            _require(isinstance(field, str) and field, 'COUNT的field必须是非空字符串或*')
    else:
        _require(isinstance(field, str) and field,
                 '%s的field必须是非空字符串' % fn)
    alias = raw.get('alias')
    _require(isinstance(alias, str) and alias, 'measure.alias必须是非空字符串')
    return Measure(fn, field, alias)


def parse_window(raw):
    if not isinstance(raw, dict):
        raise ValueError('windows元素必须是对象')
    name = raw.get('name')
    _require(isinstance(name, str) and name, 'window.name必须是非空字符串')
    partition_by = raw.get('partition_by', ['partition'])
    _require(isinstance(partition_by, list) and all(isinstance(f, str) and f for f in partition_by),
             'partition_by必须是字符串数组')
    order_by = raw.get('order_by')
    _require(isinstance(order_by, list) and order_by,
             '带帧窗口必须声明order_by（当前契约为单一数值排序键）')
    order_key = _parse_order_key(order_by[0])
    frame = _parse_frame(raw.get('frame'), len(order_by))
    measures = [_parse_measure(m, name) for m in raw.get('measures', [])]
    _require(measures, '窗口%s至少需要一个measure' % name)
    aliases = [m.alias for m in measures]
    _require(len(aliases) == len(set(aliases)),
             '窗口%s内measure别名重复：%s' % (name, aliases))
    return WindowSpec(name, partition_by, order_key, frame, measures)


def parse_settings(raw):
    raw = raw or {}
    if not isinstance(raw, dict):
        raise ValueError('settings必须是对象')
    mem_rows = raw.get('mem_rows', 1000)
    if not isinstance(mem_rows, bool) and isinstance(mem_rows, int) and mem_rows > 0:
        pass
    else:
        raise ValueError('settings.mem_rows必须是正整数，收到%r' % mem_rows)
    tmp_dir = raw.get('tmp_dir')
    _require(tmp_dir is None or isinstance(tmp_dir, str),
             'settings.tmp_dir必须是字符串或null')
    debug_members = bool(raw.get('debug_members', False))
    return {'mem_rows': mem_rows, 'tmp_dir': tmp_dir, 'debug_members': debug_members}


def parse_payload(data):
    """返回(windows规格, settings)。rows的字段级校验在执行期逐行进行。"""
    if not isinstance(data, dict):
        raise ValueError('资料应为对象')
    raw_windows = data.get('windows')
    if not isinstance(raw_windows, list) or not raw_windows:
        raise ValueError('v2资料必须声明非空windows数组')
    specs = [parse_window(w) for w in raw_windows]
    names = [w.name for w in specs]
    if len(names) != len(set(names)):
        raise ValueError('窗口名称重复：%s' % names)
    return specs, parse_settings(data.get('settings'))
