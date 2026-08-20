# Kunpeng Native Snapshot 缓存开销归因

## 1. 结论先行

当前 `native.enabled=true + classifier.enabled=false` 的原始回退已经完成归因：key 数据量和
Java 序列化都不是主因，最大异常来自 Native 开放寻址表在高频 remove-miss 后累积 tombstone，
导致 lookup/remove/put 的探测链持续增长。重建 tombstone 后 q4、q9 恢复，剩余成本才主要是
每次缓存操作跨越 JNI 边界的固定开销。

在本机鲲鹏环境、32-byte key、75% 命中率的独立基准中：

| 路径 | ns/op | 相对 Java LRU |
| --- | ---: | ---: |
| Java access-order `LinkedHashMap` | 19.07 | 1.00x |
| 32-byte 内存复制 | 12.49 | 0.65x |
| 只传 `byte[]` 的空 JNI 往返 | 162.62 | 8.53x |
| Native scalar 完整查询 | 220.21 | 11.55x |
| Native NEON 完整查询 | 222.21 | 11.65x |
| Native SVE 完整查询 | 227.97 | 11.95x |

这组数据的第一性含义是：传 32B 本身很便宜，固定调用成本远大于数据复制成本。Native 表内检查确实只占完整调用的一部分，但目前不足以偿还 JNI 边界。

因此不应继续压缩 key，也不应重复 JVM near-cache、单结果 memo、去锁或小 key 栈复制等已失败方案。本轮已经依次完成：

1. 用 Nexmark q4 采样确认生产 key 为 100% raw `BinaryRowData`、平均仅 16 B。
2. 用严格成对测试确认鲲鹏上 `SCALAR` 比原 `AUTO/SVE` 快 3.14%。
3. 修复 tombstone 退化，q4 严格 A/B 提升 18.08%，q9 提升 6.50%，q20 每核持平。
4. 将鲲鹏 TSV110/HIP09 的 `AUTO` 改为 `SCALAR`，并用最终嵌入式 JAR 完成 q4/q9/q20 部署验证。

本轮进一步用精确 Java membership hint 跳过 96.34% 的无效 remove JNI，q4 严格配对提升
3.85%。后续只有发现可合并的自然批次或高比例连续同 key 调用时，才值得继续做 JNI 调用融合。

## 2. 第一性拆解

一次 Native snapshot lookup 的总成本拆成：

```text
T_total = T_key_encode
        + T_jni_transport
        + T_native_core
        + T_result_materialize
```

- `T_key_encode`：取得 `(key, namespace)` 的字节表示。Nexmark 常见路径直接引用单段 on-heap `BinaryRowData`，不序列化、不复制；其他类型才进入 serializer fallback。
- `T_jni_transport`：Java/Native 状态切换、数组 pin/unpin、JNI 参数和返回值处理，以及采样路径写回计时值的开销。
- `T_native_core`：字节哈希、探测、key 比较、命中判断和 LRU touch；SINGLE payload 是 Java `GlobalRef`，不是再次序列化 user key。
- `T_result_materialize`：JNI 返回后生成 `Lookup`；上层随后还会生成 `MapSnapshot`。

存在性检查的理论收益是命中 EMPTY/SINGLE 后跳过 RocksDB 范围扫描。其净收益必须满足：

```text
命中率 × 被跳过的 RocksDB 成本
  > 每次 probe 的固定 Native 成本 + Native 引起的额外 miss 成本
```

q4 的历史诊断显示 snapshot 命中率约 87.88%，说明缓存语义本身有效；但 Native 表替换 Java 表后 q4 仍回退，问题位于“如何完成 probe”，而不是“是否值得 probe”。

## 3. 本次实现

### 3.1 低扰动生产采样

沿用 `state.backend.cachekit.diagnostics.enabled`，仅在该配置为 `true` 时启用。lookup、put、remove 共用计数器，每 1024 次 Native 缓存操作采样一次；diagnostics 关闭时继续调用原 JNI 方法，生产正常路径没有 `nanoTime` 和 Native 计时。

新增的 REST metric suffix：

| 指标 | 含义 |
| --- | --- |
| `native_snapshot_cost_operations` | diagnostics 开启后的 Native 缓存操作总数 |
| `native_snapshot_cost_raw_key_samples` | 采样中直接引用 `BinaryRowData` 字节的次数 |
| `native_snapshot_cost_serialized_key_samples` | 采样中走 serializer fallback 的次数 |
| `native_snapshot_cost_input_bytes` | 所有采样调用输入 key 字节数之和 |
| `native_snapshot_cost_key_encode_ns` | 所有采样调用 key 编码耗时之和 |
| `native_snapshot_cost_{lookup,put,remove}_samples` | 分操作采样数 |
| `native_snapshot_cost_{lookup,put,remove}_jni_ns` | Java 侧测得的完整 JNI 调用耗时之和 |
| `native_snapshot_cost_{lookup,put,remove}_native_core_ns` | Native 内部表操作耗时之和 |
| `native_snapshot_cost_{lookup,put,remove}_jni_transport_ns` | `max(0, jni_ns - native_core_ns)` 之和 |
| `native_snapshot_cost_lookup_materialize_ns` | JNI 返回后构造 `Lookup` 的耗时之和 |

所有时间指标都是累计值，平均值用 `*_ns / *_samples` 计算。输入平均长度用 `input_bytes / (raw_key_samples + serialized_key_samples)` 计算。

注意：profiled JNI 会把 `native_core_ns` 写入复用的 `long[1]`，所以 `jni_transport_ns` 包含这次采样写回本身。它适合判断数量级和占比，不应被当作未插桩正常调用的精确纳秒值；空 JNI 基准用于校准固定边界成本。

### 3.2 独立基准扩展

Native C++ 基准增加了 32-byte key 的真实字节表路径；Java/JNI 基准增加了：

- Java LRU lookup；
- 32-byte copy；
- 空 JNI `byte[]` 往返；
- scalar/NEON/SVE 的完整 `byte[] -> jobject` 查询。

运行命令：

```bash
cmake --build flink-state-backends/flink-statebackend-cachekit/target/native-snapshot -j2
ctest --test-dir flink-state-backends/flink-statebackend-cachekit/target/native-snapshot --output-on-failure
flink-state-backends/flink-statebackend-cachekit/target/native-snapshot/cachekit_snapshot_bench

/home/wutb/opt/jdk-11.0.31+11/bin/javac \
  -d /tmp/cachekit-native-bench-classes \
  flink-state-backends/flink-statebackend-cachekit/src/native/snapshot-cache/bench/NativeSnapshotBench.java
/home/wutb/opt/jdk-11.0.31+11/bin/java \
  -cp /tmp/cachekit-native-bench-classes \
  org.apache.flink.contrib.streaming.state.cachekit.nativebench.NativeSnapshotBench \
  "$PWD/flink-state-backends/flink-statebackend-cachekit/target/native-snapshot/libcachekit_snapshot_jni.so"
```

### 3.3 正确性验证

- Native CTest：2/2 通过。
- `NativeMapSnapshotCacheTest` 与 `CachedInternalMapStateTest`：27 个测试，26 通过，1 个原有条件跳过。
- 新测试连续执行 1024 次 put、lookup、remove，确认普通路径与 sampled JNI 路径行为一致，三类采样均实际产生。
- JDK 11 reactor package 成功，生成包含 `.so` 的 CacheKit JAR。

## 4. 当前性能证据

### 4.1 Native core

本机 2026-08-17，capacity 4096、load 2048、75% hit：

| 内核 | primitive ns/op | 32-byte key ns/op |
| --- | ---: | ---: |
| scalar | 19.41 | 52.67 |
| NEON | 22.68 | 56.08 |
| SVE | 23.67 | 59.65 |

scalar 对 32-byte key 比 NEON 快 6.1%，比 SVE 快 11.7%。原因不是鲲鹏向量指令弱，而是此处工作负载由短 key 哈希、分支、随机访存和 LRU 指针更新主导，向量探测没有足够连续计算来摊销额外成本。

### 4.2 已完成的 Nexmark true+false A/B

结果目录：`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-table-q4-q9-q20-20260813`。顺序为 A、N、N、A，20M events；A 为 Java snapshot cache，N 为 Native snapshot table，二者 classifier 都为 false。

| Query | Java 中位/均值 | Native 中位/均值 | Native 差异 |
| --- | ---: | ---: | ---: |
| q4 | 601350 | 524455 | -12.79% |
| q9 | 287355 | 274010 | -4.64% |
| q20 | 438395 | 437680 | -0.16% |

q4 对 snapshot probe 最敏感，因此也最能暴露单次 JNI 调用成本；q20 基本持平不能证明 Native lookup 已经便宜，只说明该 query 对此热路径不敏感。

### 4.3 高频 remove 暴露的 tombstone 退化

q4 诊断中约 1600 万次 remove 只有 772108 次真正删除，约 95.2% 是 remove miss；同时
remove Native core 达到 3362.65 ns/op。检查 Native 开放寻址表后确认：删除槽一直保留为
tombstone，长期 put/remove 会逐步消耗所有 EMPTY 终止标记，miss 最终扫描很长的探测链。

新增 `remove-miss-after-churn` 基准后，修复前后结果为：

| 内核 | 修复前 ns/op | tombstone 重建后 ns/op | 降幅 |
| --- | ---: | ---: | ---: |
| scalar | 1530.40 | 90.52 | 94.1% |
| NEON | 1414.81 | 93.92 | 93.4% |
| SVE | 1632.84 | 93.99 | 94.2% |

修复在 tombstone 达到表容量的 1/8（最少 64）时重建索引，移动现有 key/payload 并按原
LRU 链顺序重新插入；不改变 JNI 接口、缓存容量、命中语义和 LRU 相对顺序。测试覆盖空表
持续 churn，以及重建时仍有 live payload 的场景。Native CTest 2/2 和 JDK 11 reactor
package 均通过；新 JAR SHA-256 为
`ea97dbf5ee2ef07abdba3c1ad874ee3d01d4fed59dc53687f0bc913ba43c2b60`。

## 5. 下一轮 Nexmark 归因步骤

先只跑 q4，不把 classifier 混入实验。

### 5.1 diagnostics 归因

已于 2026-08-17 启动第一轮，运行清单位于
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-cost-q4-20260817`，结果写入
`/home/wutb/nexmark-bench-v2/results/20260817T231723+0800_kunpeng-native-cost-q4-scalar-20m-20260817`。
该轮使用独立 Compose 项目 `cknativecost817`、端口和 CPU sets；注入的 CacheKit JAR
SHA-256 为 `60397d44207570e58e31b370ffab61b00295d47edebe940d6343e1ab8e223589`。
运行在 tmux 会话 `cknativecost817` 中，完成后从 q4 目录的 `cachekit-metrics/delta.csv`
读取指标。

该轮已经完成，q4 吞吐为 526470 events/s。诊断采样结果如下；采样比例实测为
1/1024.07，与设计一致：

| 项目 | 结果 |
| --- | ---: |
| raw key 比例 | 100% |
| 平均输入 key | 16.00 B |
| 平均 key encode | 64.07 ns/op |
| lookup JNI 总耗时 | 1803.20 ns/op |
| lookup Native core | 616.84 ns/op |
| lookup transport | 1186.26 ns/op，JNI 总耗时的 65.79% |
| lookup materialize | 44.65 ns/op |
| put JNI / core / transport | 9757.55 / 7496.67 / 2258.46 ns/op |
| remove JNI / core / transport | 5212.61 / 3362.65 / 1849.94 ns/op |

同时观测到 15969692 次 lookup probe、约 188 万次 store，以及约 1600 万次
remove/invalidation；remove 与 lookup 接近 1:1。由此确认：生产路径没有 serializer fallback，
16 B 数据量也不是根因；lookup 的首要固定开销在 JNI 边界，而总流程还受到高频 remove
调用影响。插桩路径包含时钟读取和 `long[1]` 写回，因此这里用于判断占比，吞吐不用于替代
diagnostics 关闭后的性能 A/B。

配置：

```yaml
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.classifier.enabled: false
state.backend.cachekit.map.snapshot.cache.native.kernel: SCALAR
state.backend.cachekit.diagnostics.enabled: true
```

跑 20M events，记录上表全部 `native_snapshot_cost_*` 指标，同时保留已有 probes、hits、misses、stores、invalidations、evictions。计算：

```text
raw_ratio = raw_key_samples / (raw_key_samples + serialized_key_samples)
avg_key_bytes = input_bytes / (raw_key_samples + serialized_key_samples)
avg_encode = key_encode_ns / sampled_operations
avg_jni = lookup_jni_ns / lookup_samples
avg_core = lookup_native_core_ns / lookup_samples
avg_transport = lookup_jni_transport_ns / lookup_samples
avg_materialize = lookup_materialize_ns / lookup_samples
hit_rate = hits / probes
```

判定：若 `raw_ratio > 95%` 且 `avg_key_bytes <= 64`，则数据序列化和传输量不是主因；若 `avg_transport` 显著大于 `avg_core`，则主因是调用次数和 JNI 固定成本。

### 5.2 AUTO 与 SCALAR 成对测试

固定其他配置，顺序：

```text
Java A -> Native AUTO -> Native SCALAR -> Native SCALAR -> Native AUTO -> Java A
```

只有当 scalar 的 Native core 至少快 3%，并且 q4 至少提升 1%，才修改 AUTO 的鲲鹏选择逻辑。微基准已经满足第一条，尚需 q4 满足第二条。

该 campaign 已在
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-auto-scalar-q4-20260817`
启动，顺序严格为上述六段，diagnostics 关闭。它固定使用 tombstone 修复前的
`60397d...589` JAR，以便只回答 kernel 选择问题，不与后续表结构修复混杂。
第一次编排因配置文件名映射错误而只跑了首尾 Java 段，Native 四段并未启动；原始记录保留为
`campaign-attempt1.log` 和 `runs-attempt1.csv`。修正映射后已从六段的第一段完整重启，不能把
第一次的两个 Java 数值混入最终配对结果。

修正后的六段均成功，结果为：

| 变体 | 两次吞吐 events/s | 均值 events/s | 相对 AUTO |
| --- | --- | ---: | ---: |
| Java | 806260 / 826790 | 816525 | +23.19% |
| Native AUTO | 658310 / 667290 | 662800 | 基线 |
| Native SCALAR | 692980 / 674290 | 683635 | +3.14% |

SCALAR 同时满足 Native core 快至少 3% 和 q4 快至少 1% 两个门槛，后续鲲鹏实验固定使用
SCALAR。该结论只针对当前短 key、分支和随机访存主导的 snapshot 表，不能外推为鲲鹏上的
通用向量化结论。Native SCALAR 相对 Java 仍回退约 16.28%，所以 kernel 选择只解决了小部分
问题。

### 5.3 tombstone 修复 A/B

AUTO/SCALAR campaign 完成后，固定其胜出 kernel，再使用完全相同的 q4 配置比较：

```text
old Native -> tombstone-fix Native -> tombstone-fix Native -> old Native
```

除 CacheKit JAR 外所有输入保持一致。若 q4 回退明显收窄，再跑一次带 diagnostics 的修复版，
验证 remove core 时间下降；若微基准下降但 q4 无改善，说明 remove core 在总流水线中不是主要
占比，不继续调整重建阈值。

该 campaign 已在
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-tombstone-q4-20260817`
完成。old JAR 为 `60397d...589`，fix JAR 为
`ea97db...b60`，四段均固定 SCALAR 且 diagnostics 关闭。

四段均已成功，结果为：

| 变体 | 两次吞吐 events/s | 均值 events/s | 每核均值 events/s | 相对 old |
| --- | --- | ---: | ---: | ---: |
| old Native | 683410 / 647500 | 665455 | 44275 | 基线 |
| tombstone fix | 796460 / 775070 | 785765 | 50500 | 总吞吐 +18.08%，每核 +14.06% |

修复版相对上一轮同机 Java 均值 816525 只低约 3.77%；跨 campaign 比较仅作参考，严格结论是
同一 `old -> fix -> fix -> old` 内 fix 有显著提升。这证明 q4 回退的主要来源不是 16 B key
传输量，而是高频 remove-miss 与 tombstone 累积共同放大的 Native 探测成本。下一步复跑
修复版 diagnostics，确认 remove core 指标同步下降，然后再验证 q9/q20 没有回退。

修复版 q4 diagnostics 位于
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-tombstone-diag-q4-20260818`
；q9/q20 的 `old -> fix -> fix -> old` campaign 位于
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-tombstone-q9-q20-20260818`。两者均已完成。

修复前后的采样平均耗时为：

| 操作 | 指标 | 修复前 ns/op | 修复后 ns/op | 降幅 |
| --- | --- | ---: | ---: | ---: |
| lookup | Native core | 616.84 | 132.57 | 78.51% |
| lookup | JNI 总耗时 | 1803.20 | 1094.23 | 39.32% |
| remove | Native core | 3362.65 | 134.56 | 96.00% |
| remove | JNI 总耗时 | 5212.61 | 1012.80 | 80.57% |
| put | Native core | 7496.67 | 850.55 | 88.65% |
| put | JNI 总耗时 | 9757.55 | 3243.89 | 66.76% |

这证明 tombstone 修复直接作用于预期的 Native core；JNI 总时间没有按同等比例归零，是因为
数组访问、状态切换和采样写回等固定边界仍然存在。

q9/q20 四段 A/B 结果：

| Query | old 均值 events/s | fix 均值 events/s | 总吞吐差异 | 每核差异 |
| --- | ---: | ---: | ---: | ---: |
| q9 | 336440 | 358300 | +6.50% | +6.02% |
| q20 | 540860 | 534280 | -1.22% | +0.10% |

q9 有明确收益；q20 的总吞吐变化来自平均使用核数波动，按核完全持平，因此没有证据表明修复
造成性能回退。

### 5.4 鲲鹏 AUTO 选择

严格 q4 已证明 SCALAR 比 AUTO 原先选择的 SVE 快 3.14%，因此 AUTO 在已验证的 HiSilicon
TSV110/HIP09 上改选 SCALAR；显式 `NEON`/`SVE` 不受影响，其他 ARM 与 x86 继续使用原有
能力探测。识别使用 MIDR implementer/part：Linux 定义 `0x48` 为 HiSilicon，`0xD01` 为
TSV110、`0xD02` 为 HIP09（[Linux arm64 cputype.h](https://github.com/torvalds/linux/blob/master/arch/arm64/include/asm/cputype.h#L49-L58)，
[part 定义](https://github.com/torvalds/linux/blob/master/arch/arm64/include/asm/cputype.h#L129-L133)）。
本机 MIDR 为 `0x480fd020`，命中 HIP09。识别优先读取 sysfs MIDR，容器未挂载该节点时回退
读取 `/proc/cpuinfo`。测试确认本机 `AUTO` 实际报告 `scalar`。

### 5.5 最终 AUTO 可部署路径验证

最终 JAR 以嵌入式 `META-INF/native/libcachekit_snapshot_jni.so` 交付，SHA-256 为
`6e07497efc2f04d97daa30638f1a265ca6a8bd0a6be2cbdd3448f425825b4dc3`。使用如下生产配置，
关闭 diagnostics，对 q4/q9/q20 各跑一轮 20M events：

```yaml
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.classifier.enabled: false
state.backend.cachekit.map.snapshot.cache.native.kernel: AUTO
```

campaign 位于
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-final-auto-q4-q9-q20-20260818`，结果位于
`/home/wutb/nexmark-bench-v2/results/20260818T010048+0800_kunpeng-native-final-auto-q4-q9-q20-20m-20260818`。
三个 query 均通过，所有 TaskManager 初始化日志均确认 `AUTO -> kernel=scalar`：

| Query | 最终 AUTO events/s | 上轮 fix 双次均值 | 差异 |
| --- | ---: | ---: | ---: |
| q4 | 803210 | 785765 | +2.22% |
| q9 | 359730 | 358300 | +0.40% |
| q20 | 534060 | 534280 | -0.04% |

这是一轮部署 smoke，不替代双次交错 A/B；但三项都与严格修复版结果一致，说明最终 JAR、
嵌入式 `.so`、配置读取和鲲鹏 AUTO 识别链路均已生效，且没有引入新的可见性能回退。q4
相对此前同机 Java 双次均值 816525 约低 1.63%，该数值跨 campaign，仅用于观察剩余差距。

### 5.6 精确 remove membership hint

第一性目标不是让 Native remove 更快，而是避免对确定不存在的 key 调用 Native。Java 维护
Native live key 的 64-bit hash multiplicity，Native put 返回插入/更新/淘汰状态以及被淘汰
key 的 hash，使计数与 C++ 表同步。只有计数为 0 时才跳过 JNI；hash 冲突最多产生额外 JNI，
不会漏删。

配置默认关闭：

```yaml
state.backend.cachekit.map.snapshot.cache.native.remove-hint.enabled: true
```

q4 diagnostics 窗口记录 14,094,832 次 remove request、13,579,379 次 hint skip、515,704
次 JNI call、515,700 次实际删除和 0 次 false positive，跳过率约 96.34%。各 REST gauge 的
抓取边界非原子，requests 与 skips + calls 有 251 次边界差异。

同 JAR、diagnostics 关闭、每项两次的 `OFF -> ON -> ON -> OFF`：

| Query | OFF events/s | ON events/s | 总吞吐差异 | 每核差异 |
| --- | ---: | ---: | ---: | ---: |
| q4 | 777460 | 807360 | +3.85% | +3.91% |
| q9 | 360960 | 364265 | +0.92% | -1.71% |
| q20 | 538075 | 535430 | -0.49% | +5.93% |

q4 明确受益；q9/q20 变化小于 1%，且总吞吐与每核方向不一致，没有结构性回退。campaign：
`/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-remove-hint-q4-20260818`。测试 JAR
SHA-256 为 `0b884cc29bb276158580bd33a7a863aa18e820dd80bfba71c64f48555682fd47`。

### 5.7 淘汰语义

Java LRU 允许 `maxEntries + 64` 后批量淘汰到 `maxEntries`；Native 当前达到 `maxEntries` 后每次插入严格淘汰一条。比较 A/N 的 hit rate、misses 和 evictions：

- Native hit rate 明显下降：先对齐 overflow/batch eviction，并做 Java/Native 差分序列测试。
- hit rate 基本一致：淘汰语义不是本轮 q4 回退主因。

### 5.8 CPU 与 allocation profile

对 q4 的 Java A 和 Native SCALAR 各采一轮 CPU、一轮 allocation：

```bash
/home/wutb/nexmark-bench-v2/tools/async-profiler/bin/asprof -d 30 -e cpu -f /tmp/q4-cpu.html PID
/home/wutb/nexmark-bench-v2/tools/async-profiler/bin/asprof -d 30 -e alloc -f /tmp/q4-alloc.html PID
```

重点看 `NativeMapSnapshotCache.get/nativeLookup`、`GetPrimitiveArrayCritical`、`Lookup.single/empty`、`MapSnapshot` 构造和 GC。若 `Lookup`/`MapSnapshot` 分配成为明确热点，再做不逃逸的复用或 EMPTY singleton；否则不增加对象生命周期复杂度。

## 6. 决策门槛

- `SCALAR`：core 快 ≥3% 且 q4 快 ≥1%，才调整 AUTO。
- 淘汰语义：Native hit rate 有可重复下降，才实现 overflow/batch eviction。
- 包装对象：allocation profile 明确显示为热点，才优化。
- JNI 融合：只有自然批次或连续同 key 比例 ≥30%，才设计合并接口。
- 若 raw key 已占绝对多数、key 很短、hit rate 一致，且没有自然批次/复用，则应得出工程结论：单次 Native cache-table lookup 不适合替换 JVM 内缓存；Native 下沉应放在一次调用能完成更多工作的边界，而不是继续微调 32B 传输。
