"""对固定内容执行查询，并返回资料摘要。

保留既有v1入口inspect（baseline全分区前缀累计，输出语义不变）；
新增v2窗口帧入口inspect_windows，交付ROWS/RANGE/GROUPS查询算子结果、
实际排序计划与排序、缓冲、临时空间统计。
"""
import sys
import json
from .storage import freeze, read_json, digest
from .baseline import execute
from .plan import execute_windows


def inspect(data):
    data = freeze(data)
    identity = digest(data)
    result = execute(data)
    return {'input_digest': identity, 'result': result}


def inspect_windows(data):
    data = freeze(data)
    identity = digest(data)
    payload = execute_windows(data)
    return {'input_digest': identity, **payload}


def main():
    if len(sys.argv) != 2:
        raise SystemExit('请提供资料JSON路径')
    data = read_json(sys.argv[1])
    response = inspect_windows(data) if data.get('version') == 2 else inspect(data)
    print(json.dumps(response, ensure_ascii=False, allow_nan=False, indent=2))


if __name__ == '__main__':
    main()
