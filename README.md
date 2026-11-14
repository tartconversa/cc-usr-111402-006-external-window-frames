# 能源序列查询的外排窗口帧与同伴组语义

本项目提供碳预算资料的本地执行路径。当前业务行为见app/baseline.py，资料契约见[资料与接口](docs/data-contract.md)。
app/storage.py校验输入并产生确定内容摘要；app/semantics.py提供受限三值谓词；app/service.py绑定资料身份并运行现有代码。
fixtures/sample.json全部为合成资料，不对应真实机构的排放记录。Python 3.14，当前模块仅依赖标准库。

运行已有入口（v1，全分区前缀累计，输出语义不变）：

    python -m app.service fixtures/sample.json

运行v2窗口帧查询（ROWS/RANGE/GROUPS，CLI按version路由）：

    python -m app.service fixtures/window_sample.json

v2窗口算子模块：
- app/framespec.py：窗口/帧/度量规格解析与非法组合解释式拒绝
- app/sortio.py：内存额度下的外排run、级联多路归并、临时空间统计
- app/window.py：ROWS/RANGE/GROUPS流式帧算子、peer判定、EXCLUDE、调试侧车
- app/plan.py：多窗口排序复用判定、输入校验、执行编排与公开统计

执行现有行为检查：

    python -m unittest discover -s tests -v

编译检查：

    python -m compileall -q app tests
