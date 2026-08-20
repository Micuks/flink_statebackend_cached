# Flink CacheKit Snapshot Cache Native 优化

> 两页汇报大纲，只介绍当前最终实现和最终 Nexmark 结果。
>
> 本文的 snapshot 指 CacheKit 对 `MapState.entries()/iterator()/isEmpty()` 的
> EMPTY/SINGLE 快照缓存，不是 Flink checkpoint/snapshot。

## 第 1 页：Native Snapshot 优化

### 标题

Snapshot miss traversal Native 化

### 一句话说明

修改 JNI 接口，将 RocksDB prefix 范围查询和分页遍历完整下沉到 C++；Java 保留
Flink 序列化、结果物化和 LRU 热缓存。

### 调用链

```text
Java 收集 prefix
    ↓
nativeReadPrefixBatch
    ↓
C++ 创建 RocksIterator，完成 prefix seek / scan
    ↓
每批最多返回 128 条 raw key/value
    ↓
Java TypeSerializer 反序列化并更新 LRU
```

### 核心改动

- 新增 `nativeReadPrefixBatch` JNI 接口，一次返回一批 raw key/value。
- SINGLE 在同一次 Native traversal 中返回 key 和 value，不再额外执行 RocksDB point-get。
- MULTI 每批最多返回 128 条，并从上一批最后一个 raw key 继续，避免从 prefix 起点重复扫描。
- RocksIterator 在单次 JNI 调用内创建并释放，不向 Java 暴露需要额外 `close()` 的 Native handle。
- Java 继续负责 `TypeSerializer` 反序列化、MapState 语义以及 Java LRU 热缓存。
- Native 路径具有独立开关，默认关闭；同一实现可在鲲鹏 AArch64 和 x86_64 上编译运行。

### 正确性验证

- 覆盖 EMPTY、SINGLE、合法 null value 和 MULTI。
- 130 条数据按 `128 + 2` 两页返回，无重复、无遗漏。
- SINGLE 冷 miss 不再调用 point-get。
- MULTI 不调用 `delegate.entries()`。
- C++ 测试 `2/2`、Java/JNI 测试 `25/25` 通过。

### 页面结论

Native 化的收益来源不是单条 C++ 指令更快，而是将一次 miss 的权威范围读取合并为
单次、可分页延续的 Native traversal，减少 JNI 往返和重复 RocksDB 扫描。

**建议配图**：一条五段式流程图：
`Java prefix → JNI batch → C++ prefix scan → 128 条分页 → Java 物化`。

## 第 2 页：Nexmark 性能结果

### 测试口径

- 查询：q4、q9、q20。
- 对照：Java snapshot 基线与最终 Native prefix batch。
- 相同 JAR、相同镜像、固定 CPU。
- 执行顺序：`Java → Native → Native → Java`。
- 每次处理 20M events，以中位数 events/s 对比。

### 结果

| 查询 | Java 中位数 | Native 中位数 | 相对变化 |
|---|---:|---:|---:|
| q4 | 506245 | 496630 | -1.90% |
| q9 | 248355 | 244090 | -1.72% |
| q20 | 371555 | 364620 | -1.87% |

### 有效性确认

- 所有运行输入 JAR 哈希一致。
- Java 组没有 Native 初始化日志。
- Native 组所有 TaskManager 均确认加载 JNI。
- 没有 `UnsatisfiedLinkError`、JNI symbol 缺失、SIGSEGV 或 Flink fatal error。

### 页面结论

```text
功能目标：已完成
性能提升：未观察到
性能回退：1.72%～1.90%
默认策略：Java snapshot 开启，Native 实验开关默认关闭
```

当前 Native 版本已经具备完整语义、可验证性和跨架构可用性，可作为后续减少 JNI
byte[] 物化、扩大真实批量规模的优化基础，但现阶段不应宣称吞吐提升。

**建议配图**：q4、q9、q20 三组 Java/Native 并列柱状图，并在每组下方标注相对变化。

## 材料索引

- 完整 Native miss traversal：`0338d1df46 feat(cachekit): stream snapshot misses through native batches`
- 最终性能记录：`885b27a768 docs(cachekit): record final native batch q4 result`
- 详细设计与实验记录：`kunpeng-native-snapshot-cache-plan.md`
- q4 campaign：`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p6-final-q4-20260812/`
- q9/q20 campaign：`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p6-final-q9-q20-20260812/`
