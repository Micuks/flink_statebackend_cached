# CacheKit Prefetch：原生路径、优化路径、重叠窗口与正确性边界

## 原生 Flink/RocksDB 路径

每条记录到达 mailbox 后，依次执行：设置 current key、执行业务函数、同步读取 RocksDB 状态、更新状态、序列化并发送下游。状态 I/O 位于记录的关键路径上；出现下游背压时，mailbox 不能把这段等待转换成未来状态读取。

## CacheKit 优化路径

1. `StreamRecordBatchOutput` 保留一段只读 lookahead 记录窗口，不改变记录所有权。
2. `StatePrefetcher` 只提取未来 key；CacheKit backend 为 ValueState 构造异步任务。
3. 每个 TM JVM 的有界 worker 执行批量/分块 RocksDB 读取，结果写入 staging，而不是直接改变业务状态。
4. mailbox 仍按到达顺序消费每条记录。`ValueState.value()` 先查 L1/L2，再尝试消费 generation 匹配的 staging；miss 时执行权威 RocksDB get。
5. local pre-aggregation 若能消费整批记录，则优先处理，避免为不会逐条执行的记录制造 speculative 预取死工作。它在分组后可调用 `prefetchForImmediateUse` 做同步批读；这条路径减少读次数，但不提供异步 overlap，必须与 speculative worker 分开归因。

## 时间如何 overlap

```text
mailbox:  append k1..kN | flush/逐条业务处理 | 下游等待/序列化/发送
worker :                  MultiGet(k1..kN) ----> publish staging
                                      ^ 只有在 mailbox 真正 value() 前完成才隐藏延迟
```

有效 overlap 的必要条件不是“配置打开”或“提交过任务”，而是：worker 在对应记录调用 `value()` 之前完成；结果未因写入 generation 变化而过期；记录确实访问该状态；且没有被 L1/L2 提前命中。若 LocalPreagg 接管整批，时间线变成“分组 → 同步即时批读 → 按 key fold”，此时没有 worker/mailbox overlap。

## 背压与“同一消息是否会被处理两次”

- lookahead 中的 `StreamRecord` 仍只由 mailbox 分发一次；prefetch worker 不执行业务函数、不 emit、不推进 watermark，因此不会把消息消费两次。
- 同一个 key 可能发生“异步预取读取”和“mailbox 权威 point read”并发。这是重复状态 I/O，不是重复消息处理。现有 `inFlight` 只去重多个预取任务，不能阻止 live point read 与尚未完成的 prefetch 竞争。
- worker 只能发布 staging 值。mailbox 使用前会校验 `writeGen`；其间发生 update/clear/dirty flush 时，旧 generation 结果被丢弃，不能覆盖新值。
- 有界队列丢弃任务时必须执行 drop 回调，释放 in-flight reservation；否则 key 会永久误判为“已预取”。已有单测覆盖 reservation 释放和 stale generation 重新认领。
- watermark、watermark status、latency marker 到达时，batch 先 flush 再转发，保持记录先行。Checkpoint barrier 不经过这个 `DataOutput` API；其顺序由 Flink network input/barrier handler 保证，不能把 watermark 测试冒充 barrier 正确性证据。

## 可观测性与本次实测判据

当前 runtime 已记录以下每-leg 指标：

- `prefetch_tasks_submitted/executed/dropped/failed`
- `prefetch_keys_requested/deduplicated`
- `prefetch_multiget_calls/keys` 与 point-read fallback
- `staging_published/promoted/stale_discarded/admission_dropped`
- `live_read_raced_inflight`：mailbox 读取时同 key 仍在预取
- `staging_unused_at_eviction_or_close`：已读但未消费

由这些比率拆解：

```text
覆盖率 = promoted / requested
及时率 = promoted / published
重复读取率 = live_read_raced_inflight / requested
无效工作率 = (stale + unused + admission_drop) / requested
```

吞吐没有提升时，只有结合这些指标才能分别归因于：任务没触发、worker 太慢、队列丢弃、预取太晚、业务没有访问该状态、缓存已经命中或 local-preagg 让读取失去必要性。

## 已有正确性证据与待补测试

已有单测覆盖：key 顺序提取、缺失可选 hook、预取 key 去重与 drop 释放、write generation 拒绝陈旧结果、单次有序批读、惰性 materialization、容量限制、失败回退权威读、chunk 完成即发布、close 等待 in-flight、native key 碰撞回退、local-preagg 首见顺序，以及 record/watermark/status/latency 的 flush-before-forward 顺序。

待补：

1. live-read 与 in-flight prefetch 竞争计数和无错误结果测试。
2. 背压 gate 开/关、local-preagg 优先级与 async chunk 不重复提交测试。
3. 任务被有界队列替换时，所有 reservation 均释放的并发测试。

## 2026-08-19 触发链审计

50M q5/q15/q17 ABBA 中，Prefetch 配置虽为 true，但三者 `tasksBuilt=0`：

- q5 的状态带窗口 namespace；generic lookahead 只有未来 record key，不知道未来 namespace。`supportsRecordKeyPrefetch()` 因此 fail-closed，避免读错 namespace。
- q15/q17 的 GroupAgg 是 `BatchableKeyedFunction`。`StreamRecordBatchOutput.flushBatch()` 先调用 `LocalPreagg.dispatch()`；成功后整批已经被按 key fold 并返回，不再调用 speculative `StatePrefetcher.prefetch()`。
- 所以这三查询不能用于回答 Prefetch overlap 收益；它们的 off/on 吞吐小差异是运行漂移。
- q9 的历史同类日志有 `tasksBuilt≈6万/TM`、`tasksExecuted≈6万/TM`，是下一轮正确的 Prefetch 机制查询。新实验把 `tasksBuilt/tasksExecuted/promoted > 0` 写成硬 gate。

边界结论：lookahead 只持有引用；业务函数和下游 emit 仍只由 mailbox 执行一次。可能重复的是“worker 预读”与“mailbox 权威 point get”这两个状态 I/O，不是消息处理。`writeGen` 阻止过期 staging 被提升；watermark/status/latency 到达前先 flush；失败或迟到一律回退权威路径。
