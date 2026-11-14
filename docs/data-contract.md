# 资料与接口

## 内容身份

入口app.service.inspect(data)不修改调用方资料，返回input_digest及result。
input_digest是对完整输入JSON按键排序、UTF-8、无空白序列化的SHA-256。数组顺序保留；禁止NaN和无穷，磁盘JSON重复字段拒绝。
app.storage.freeze返回独立副本。输入快照和对象版本由业务字段明确声明，内容摘要不会替调用方创造版本。

## 现有业务约定

rows含唯一rid、partition、order、value，order/value为十进制字符串或null。query给ascending、nulls_first。baseline.execute按业务order排序，同值按rid稳定排，返回全分区前缀sum；count_value忽略null。新帧偏移整数用于ROWS/GROUPS，非负十进制用于RANGE；支持单ORDER BY数值列，null与非null不同peer，null当前行的偏移RANGE只纳入同null peer。空帧sum/avg为null、count为0。

## 受限谓词

app.semantics.evaluate(row, expression)返回True/False/None，None为unknown。
eq/lt/le/gt/ge含field和value，缺字段按null；比较双方必须同类型，null比较结果unknown。
is_null只有field；and/or含args数组，not含arg，按三值逻辑计算。空and为true，空or为false。
where只接受结果严格为true的行。expression为null表示不限制。

## 现有执行链

app.service.inspect → storage.freeze/digest → baseline.execute。sample.json描述的是现有可执行输入，服务输出附资料摘要。
新增能力可以增加入口和计划结构，保留现有调用及输出语义；扩展格式应单独声明版本。

## v2窗口帧查询（version=2）

入口app.service.inspect_windows(data)（CLI按version字段自动路由），在同一冻结/摘要链之后交付窗口查询算子结果，而不是预制年度表。返回input_digest、plan、windows、stats。

### 输入结构

- rows：同v1，rid唯一；partition_by字段可为任意可比较标量，order与聚合字段为十进制字符串或null。
- windows：非空数组，每项含name、partition_by（默认["partition"]）、order_by（当前契约恰好一个数值排序键）、frame、measures。
- frame.mode ∈ ROWS / RANGE / GROUPS；frame.start/end为边界对象：
  - {"kind":"UNBOUNDED","direction":"PRECEDING|FOLLOWING"}
  - {"kind":"CURRENT ROW"}
  - {"kind":"OFFSET","direction":"PRECEDING|FOLLOWING","offset":...}
- frame.exclude ∈ NO OTHERS / CURRENT ROW / GROUP / TIES，默认NO OTHERS。
- measures：fn ∈ SUM / COUNT / AVG；COUNT的field允许"*"计行，其余field忽略null；每项需唯一alias。
- settings：mem_rows（正整数，内存额度，默认1000）、tmp_dir（临时目录或null）、debug_members（布尔，默认false）。

### 偏移与RANGE范围

- ROWS/GROUPS偏移为非负整数（字符串数字也接受）；RANGE偏移为非负Decimal。负数、小数用于ROWS/GROUPS、布尔值等组合以中文ValueError解释拒绝。
- RANGE带值偏移仅支持单一数值排序键；多ORDER BY或多键RANGE在解析期拒绝。
- RANGE按排序键的值区间定义：值下界=当前键减PRECEDING偏移，值上界=当前键加FOLLOWING偏移；CURRENT ROW锚定当前对等组。值区间本身与ASC/DESC无关，ASC/DESC只决定物理行序方向。
- 对等组（peer）只由业务ORDER BY键决定：null与null同peer、null与非null互异。rid仅作物理稳定行序的tiebreak，用于ROWS定位与输出复现，绝不参与peer划分，不会拆散业务并列。
- null当前行的RANGE帧（含值偏移）只纳入同null peer。
- 静态校验帧起点不得晚于终点；空帧SUM/AVG为null、COUNT为0。

### 执行与外排语义

- 计划plan按(partition_by, ORDER BY字段/升降序/null次序)分组：签名相同的窗口shared_sort=true，共用一次外排与一遍流式计算；排序不同则分列sort_group并给出各自实际计划。复用只发生在排序层，每个窗口保留独立帧规格与独立帧状态，不存在为复用改写帧的路径。
- 外排：内存缓冲达mem_rows即写有序run到临时JSONL，run数超fan_in(8)时级联多路归并。归并流带(run_id, run内位置, 字节偏移)。
- 帧计算单遍流式：ROWS用物理行索引、GROUPS用peer组索引、RANGE在已排序peer序列上定位值区间；滑动聚合进入加/离开减，不为任一输出行重扫旧帧，整分区不重新载回。
- UNBOUNDED FOLLOWING在分区末输出，流式阶段聚合只延伸不回收；待发紧凑引用可外溢到backlog临时文件。帧外紧凑条目及时驱逐，当前peer条目保留供EXCLUDE调整。
- debug_members=true时每行附debug.frame（逻辑[lo,hi]）与debug.members（逐成员idx、peer、rid、excluded）；每分区写定长侧车索引，被驱逐成员按(run_id,offset)按需seek重读，不整分区载回。

### 公开统计stats

- sort_groups：每排序组的rows、runs_created、merge_rounds、peak_runs_merged、spill_bytes、buffer_peak_rows。
- windows：每窗口的partitions、rows_emitted、frame_evaluations、entries_evicted、peak_peer_records、peak_live_entries、backlog_spill_rows/bytes、debug_sidecar_records/bytes、debug_seek_reads。
- temporary_space：files_created、peak_bytes、bytes_remaining（正常清理后为0）；memory：mem_rows与window_peak_live_entries。

### 兼容性

v1 inspect与baseline.execute的输入输出保持不变。v1的全分区前缀累计等价于ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（EXCLUDE NO OTHERS）的SUM/COUNT，测试锁定该等价关系。
