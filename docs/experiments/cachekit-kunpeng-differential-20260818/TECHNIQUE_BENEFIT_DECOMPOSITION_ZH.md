# CacheKit 技术收益拆解与证据边界（2026-08-19）

## 1. 项目主结论与平台差距

当前最新严格配对是 200M、无 checkpoint、2 containers × 4 TM/container、8 TM/16 slots、R1：

| 平台 | RocksDB → FullOpt 15Q 算术平均 | 结论 |
|---|---:|---|
| x86 | +61.71% | 最新同协议 R1 |
| Kunpeng | +47.15% | 最新同协议 R1 |
| Kunpeng - x86 | -14.56pp | 未达到 Kunpeng 高 10pp 的目标 |

FullOpt 的实际开关是：VCache、MapSnapshot、Mailbox、LocalPreAgg、SST/Mem Bloom 开；bypass=false；Chen COW/RYW/PQ=false；无 checkpoint。Prefetch 配置虽在历史 FullOpt 中打开，但最新机制审计表明其在常规 VCache 下常不触发，在强制触发时为负收益，因此不能把主栈提升归因给 Prefetch。

平台差距高度集中在 q15/q17：

| Query | x86 RDB | x86 FullOpt | x86 提升 | Kunpeng RDB | Kunpeng FullOpt | Kunpeng 提升 | 平台差 |
|---|---:|---:|---:|---:|---:|---:|---:|
| q15 | 5.59 | 26.10 | +366.91% | 10.74 | 36.15 | +236.59% | -130.32pp |
| q17 | 37.25 | 92.48 | +148.27% | 49.58 | 90.99 | +83.52% | -64.75pp |

这两项合计贡献约 `-13.00pp` 的 15Q 平均差距，约占总差距的 89%；其余 13Q 合计只差约 `-1.56pp`。Kunpeng q15 的 FullOpt 绝对吞吐比 x86 高 38.5%，q17 基本持平；相对提升较低主要因为 Kunpeng RocksDB 基线在 q15/q17 分别高约 92%/33%。后续差异化优化必须提高 q15/q17 的 FullOpt 上限，不能人为降低 x86 或污染基线。

同协议 profiling 进一步分开了两种机制。q15 上 Kunpeng 的 wall 加速 `10.80×` 高于 x86 的 `9.94×`，但 RDB→FullOpt cores 放大 `3.12×`，高于 x86 的 `2.15×`，所以 per-core 提升反而低。q17 上两平台 FullOpt 后 RocksDB 读热点都基本退场，Kunpeng 主要受更强 RDB 基线和相近 FullOpt 吞吐上限影响。详见 `Q15_Q17_CROSSHOST_PROFILE_REPORT_ZH.md`。

## 2. 各技术的当前最好独立证据

不同条目来自不同实验协议，不能相加得到 FullOpt 总提升；表中同时给出证据强度和不能回答的问题。

| 技术 | 当前最好独立结果 | 收益来源 | 证据边界 |
|---|---:|---|---|
| ValueState cache（VCache） | 第一阶段 ValueState 11Q 约 +30% | 热点值在 Java/CacheKit 层命中，绕过 JNI 与 RocksDB 点查 | 阶段验收口径；不是当前 200M FullOpt leave-one-out |
| MapSnapshot | 旧 50M 三轮 wutb-only 15Q +5.31%；native q4 preflight +2.17% | entries/单元素快照命中，减少反序列化和 JNI iterator | 协议较旧；native 15Q 复测未显示正收益 |
| Mailbox + LocalPreAgg | 旧 wuql-only 15Q +27.85% | lookahead 暴露批次，同 key N records 折叠为一次 state read/write/emit | bundle 结果，不能把 27.85% 拆成 Mailbox、PreAgg、Prefetch 三份 |
| SST Bloom only | Kunpeng 100M R1 15Q +9.36% | SST 负查在 data-block read 前拒绝 | 单轮；query 依赖 |
| MemTable Bloom only | Kunpeng 100M R1 15Q -1.36%；新 q5 50M ABBA +5.24% | q5 负查进入 SkipList 前拒绝；`FindGreaterOrEqual` cycles -77.91% | 只在 q5 有明确正收益；不能外推全 15Q |
| SST + Mem Bloom | Kunpeng 100M R1 15Q +8.59%；叠加 FullOpt 三轮增量 +11.08% | 同时减少 mutable/immutable MemTable 与 SST 无效访问 | 两个结果的 control 不同，不能相加 |
| Speculative Prefetch | 真实触发 q17 100M ABBA -3.71% | 理论上 overlap worker 读与 mailbox 处理；实测过晚 | 约 103 万任务仅 104 次 promotion，当前不计有效收益 |
| Native request plane | x86 200M 15Q -4.12%；Kunpeng -2.79% | ARM runtime dispatch 存在，但 JNI/序列化/批次编排未摊销 | 正确性通过，性能 gate 失败，默认关闭 |
| Native LocalPreAgg | 旧 200M：x86 -1.38%、Kunpeng -5.51%；q15 50M flat-slice -3.51%，4B hash-token -0.06%，buffer reuse -0.09%（均相对 ARM retained） | 只把 grouping 下沉，fold 仍在 Java；去掉完整 key 序列化后接近中性，但 JNI、plan 回传/校验和逐 group fold 仍在关键路径 | 最新腿 67.6 万 native batch、4,318 万 input key，AArch64 SVE/CRC32 真触发；连续修复仍未转正，默认关闭且不扩 15Q |
| tsv110/LSE 编译轴 | Kunpeng 三轮 -0.16% | 编译器/原子指令选择 | 单独换编译参数无稳定收益 |

## 3. 为什么 MemTable Bloom 在 q5 有收益

主吞吐使用无 profiler ABBA：off `40.94`，on `43.09` K/s/core，`+5.24%`。独立 30 秒 `cycles:u` ABBA 的符号均值：

| 热点 | off | on | 变化 |
|---|---:|---:|---:|
| `InlineSkipList::FindGreaterOrEqual` | 1.675% | 0.370% | -77.91% |
| `MemTable::KeyComparator::operator()` | 1.625% | 0.825% | -49.23% |
| libc `memcmp` | 1.165% | 0.810% | -30.47% |
| `MemTable::Get` self（含内联 Bloom） | <0.10% | 1.060% | 新增探测成本 |
| 读侧 self-cycle 集合 | 4.465% | 3.065% | -31.35% |

因此净收益不是“Bloom 本身更快”，而是约 1.06% 的 Bloom 探测替代了更贵的 SkipList 定位、状态 key 比较和字节比较。写侧 `RecomputeSpliceLevels` 没有下降，符合只优化负读的预期。

## 4. Prefetch 时间重叠与正确性

原生 Flink：mailbox 逐条执行 `setCurrentKey → 同步 state read → update → emit`，状态 I/O 在关键路径上。

CacheKit speculative 路径只读取未来 key，并由 worker 发布 staging；worker 不执行业务函数、不 emit、不推进 watermark。mailbox 仍拥有消息且只处理一次。可能重复的是“worker 状态预读”和“mailbox 权威 point read”，不是消息处理。

只有 worker 在 `ValueState.value()` 前完成、generation 仍匹配、业务确实读取该状态且 VCache 未命中，才能形成 overlap。背压不会让 worker 消费消息；barrier/watermark 前先 flush，旧 generation staging 丢弃，任何失败回退 RocksDB 权威读取。

最新真实触发结果说明当前实现不满足及时性：off `85.42`、on `82.25` K/s/core，`-3.71%`；约百万任务只产生 104 次 promotion，并有约 132 万次 live-read/in-flight race。下一版必须以窗口级任务、最小 lead、单窗口 in-flight 限制和历史 promotion 自适应准入重写。

## 5. Native/鲲鹏差异化的当前状态

Native C++ 已实现并暴露独立开关，包含：request plane、MapSnapshot、Mailbox compact、LocalPreAgg grouping、prepared-key probe/fill；AArch64 runtime 可选择 128B bucket、CRC32、NEON、SVE-256，x86 可选择 64B/SSE4.2。正确性修复包括：O(n²) 改预分配 open-addressing、每 key 只算一次 fingerprint、碰撞 exact compare、first-seen 连续 group id、MapState/MapSnapshot 长度前缀身份编码、精确 snapshot tombstone 和架构 dispatch。

这些修复已在 source `be83190c680b598bfeb10ad33331a0aa1c01552e` 上完成并做双平台 200M/15Q 复测；结果仍为 x86 -4.12%、Kunpeng -2.79%。因此当前 native 实现是正确但不高效的实验插件，不得作为 10pp 达标结论。

下一步只保留有机会影响 q15/q17 的两条路线：

1. 将 LocalPreAgg 从“仅 native grouping”推进到“可批量下沉的 accumulator kernel”，真正减少 Java `List<Object>`、逐组回调和序列化，而不是多付一次 JNI；
2. 将 Mem/SST Bloom 的 hash/probe 做 AArch64 runtime dispatch，并用 q5 和负查占比高的 query 筛选，但它不是弥合 q15/q17 差距的主手段。

第一项已经连续完成三次短筛：flat array slice 为 `-3.51%`；把 native grouping 输入从完整 generic key 序列化改为 4-byte Java hash token 后为 `-0.06%`；再复用 native-only extraction buffer 后为 `-0.09%`。碰撞由 Java equals/first-seen plan validation 检出并安全回退，正确性门槛通过，但性能始终未转正。因此不进入 15Q；下一次只能以“业务 accumulator 和 grouping 一起下沉、一次 JNI 完成整批 fold”为新设计，而不是继续微调 generic grouping helper。

## 6. 证据路径

- 双平台 FullOpt：`cachekit-idmg-fullopt-nockpt-200m-paired-2x4-r1-20260812`；`cachekit-kunpeng-fullopt-nockpt-200m-paired-2x4-r1-20260812`。
- Prefetch：`dse_results/cachekit-prefetch-overlap-q17-100m-20260819/final/PREFETCH_OVERLAP_RESULTS.json`。
- Mem Bloom profile：`dse_results/cachekit-membloom-q5-profile-20260819/MEMTABLE_BLOOM_Q5_PROFILE_REPORT_ZH.md`。
- Native 复测：`docs/experiments/cachekit-native-fullopt-200m-crosshost-20260817/RETEST_RESULTS_20260818.md`。
- 第三阶段流程图：`figures/stage3_full_workflow_v2.svg`。
