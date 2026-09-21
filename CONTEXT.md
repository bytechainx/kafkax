# kafkax 上下文

本文件定义 `kafkax` 与其使用方共享的核心词汇。它只记录领域含义与能力边界，
不记录具体实现、API 签名、存储或部署决定。

## 角色与边界

**适配器**：把 Apache Kafka 的 wire 协议收敛成稳定 Rust API 的库；它只承载连接、生产、
消费与位点原语，不含领域模型、不做业务表结构假设，也不负责多来源编排。
_Avoid_: 客户端 SDK（SDK 常含重试编排、遥测与凭据供应链，超出本仓库边界）

**显式分区消费者**：由调用方指定 `(topic, partition)` 与起始 offset 的消费入口；它不参与
group coordinator、不做 rebalance，也不替调用方决定读哪个分区。
_Avoid_: consumer group（本仓库没有组协调语义，分区归属不由 broker 分配）

**位点自管**：offset 的持久化与推进完全由应用经 `OffsetCommitStore` 负责；broker 不保存
位点，库也不隐式提交。
_Avoid_: 自动提交（本仓库没有后台提交线程，位点只在显式调用时前进）

**消息总线位置（bus_id）**：`topic/partition/offset` 三元组的稳定字符串编码，用于跨进程
定位一条消息；它不是业务主键，也不含时间与内容信息。
_Avoid_: 消息 ID（Kafka 本身没有消息 ID，bus_id 只是位置坐标）

## 语义与生命周期

**next-to-read**：offset 存储中记录的不是「已处理到的位点」，而是「下一次应读取的位点」
（`offset + 1`）；两者差一，且提交是单调的。
_Avoid_: 已完成位点（会把 next-to-read 误当成最后一条消息的下标）

**投递回执（Delivery）**：`publish` 在 broker 确认后返回的 `(partition, offset)`；`Ok` 表示
已写入 broker，不表示下游已消费。
_Avoid_: 发送成功（「成功」的粒度是 broker 持久化，不是端到端投递）

**tombstone**：`payload` 为 `None` 的消息，是 Kafka null value 的表示，常见于 compacted
topic 的删除标记；它与零长 `Some(Bytes::new())` 语义不同。
_Avoid_: 空消息（零长载荷与 tombstone 是两回事，不能互相替换）

**未确认消息（pending）**：`AtLeastOnceConsumer` 已交付但尚未 `ack` 的消息；此期间不会向下游
再取新消息。
_Avoid_: 处理中消息（pending 只说明未确认，不表达业务是否在处理）

**ack**：把 pending 消息的 offset 写入 store 并清除 pending 的动作；只有 commit 成功才清除，
失败可重试。
_Avoid_: 消费确认（容易与 broker 侧确认混淆，本仓库的 ack 只作用于应用位点存储）

## 错误与可靠性

**可重试错误**：调用方可以安全重复施加该操作的失败（连接未建立、leader 变更、超时等）；
它与「永久错误」是一对互为补集的分类，经 `KafkaError::is_retryable()` 判定。
_Avoid_: 网络错误（网络只是成因之一，不能表达「重试是否安全」这一判据）

**fail-closed 配置校验**：`validate()` 在构造前拒绝不安全组合（远程明文、TOML 中的凭据、
凭据与机制不匹配）；没有「先构造、后补救」的路径。
_Avoid_: 配置默认值（校验是拒绝式的，不是补默认值的）

**健康检查**：对 broker 可达性的显式探测，与构造解耦；`connect` 成功**不代表**服务可达。
_Avoid_: 建连（`connect` 只做校验与构造，不发业务请求）

**原子位点提交**：`FileOffsetStore` 以「临时文件 + fsync + rename + 父目录 fsync」写入，
保证进程中断不会留下半个位点文件。
_Avoid_: 普通写文件（直接覆盖写会留下部分写入的中间态）
