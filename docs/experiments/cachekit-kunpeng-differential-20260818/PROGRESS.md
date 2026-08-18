# CacheKit 双平台差异化推进记录（2026-08-18）

## 目标与不可变口径

- Golden benchmark：Nexmark 15q，2 containers x 4 TaskManager JVM/container，合计 8 TM、16 slots。
- 主结果：每 query 的 K/s/core；组结果是 query 提升百分比的算术平均。
- 目标：同一协议、同一功能栈下，Kunpeng 相对 RocksDB 的平均提升至少比 x86 高 10 个百分点。
- 不以关闭 x86 通用优化、伪造低基线或错误 cores 口径制造差异。差异必须来自仅在 AArch64/Kunpeng runtime dispatch 生效的实现。
- FullOpt 必须写出实际开关，不用名称代替配置；Chen 的 COW/RYW/PQ 默认关闭。

## 已审计的最新可用证据

| 证据 | 平台/规模 | 对比 | 15q 算术平均 | 可回答的问题 | 限制 |
|---|---|---|---:|---|---|
| `cachekit-kunpeng-fullopt-nockpt-200m-paired-2x4-r1-20260812/final/R1_R1_RESULTS.json` | Kunpeng, 200M, R1 | RocksDB vs FullOpt, bypass=false, no checkpoint | +47.15% | 当前最新严格 Kunpeng 栈收益 | 只有一轮 |
| `cachekit-idmg-fullopt-nockpt-200m-paired-2x4-r1-20260812` | x86, 200M, R1 | 同协议 RocksDB vs FullOpt | +61.71% | 与 Kunpeng 配对；30/30 腿已完成 | 只有一轮 |
| `cachekit-kunpeng-fullopt-bloom-matched-r123-20260804/final/THREE_ROUND_REPORT.md` | Kunpeng, 100M, R1-R3 | FullOpt control vs `+SST/Mem Bloom` | +11.08% 增量 | Bloom 叠加栈的收益 | 非 RocksDB 到 FullOpt 的端到端提升 |
| `cachekit-kunpeng-clean-bloom-full15-20260803/CLEAN_BLOOM_REPORT_ZH.md` | Kunpeng, 100M, R1 | RocksDB vs Bloom-only | SST +9.36%; Mem -1.36%; combined +8.59% | Bloom 组件拆解 | 单轮；说明 combined 的主要收益来自 SST Bloom，不支持“Mem Bloom 单独有收益” |
| `cachekit-kunpeng-fixed-prefetch-full15-20260803/FIXED_PREFETCH_REPORT.md` | Kunpeng, 100M, R1 | scalar vs MultiGet prefetch | total -0.14% | 旧 prefetch 实现的独立增量 | 旧实现没有证明吞吐收益，需用 overlap/消费指标重新归因 |
| `cachekit-native-fullopt-200m-crosshost-20260817/RETEST_RESULTS_20260818.md` | x86 + Kunpeng, 200M, 3 variants | Java FullOpt+mailbox vs native request plane | x86 -4.12%; Kunpeng -2.79% | native correctness和真实 runtime dispatch | always-on JNI/重建开销大于 kernel 收益，不能作为 10pp 结论 |
| `cachekit-kunpeng-compiler-direct-r1-20260805/final/THREE_ROUND_REPORT_ZH.md` | Kunpeng, 100M, R1-R3 | generic vs tsv110/LSE | -0.16% | 编译器/原子指令轴 | 单独换编译参数没有稳定收益；不能作为架构差异化主线 |
| `cachekit-kunpeng-dstl-p3/refine-logs/KUNPENG_FROCKSDB_BISHENG_RESULTS_20260810.md` | Kunpeng native screening | BiSheng、WholeKeyIndexedSkipList、Direct-PUT | 编译器 micro +1.26%；JDK 三查询 +5.36% 但不稳定；skiplist canary -2.56% | 已筛过的硬件/编译器方向 | 不足 10pp；Direct-PUT 没有有效吞吐结果，不能计入性能结论 |

## 当前明确结论

1. 最新可复用的 Kunpeng 端到端结果是 200M R1 `+47.15%`，不是旧 100M 的 `+42.79%`。
2. x86 的匹配 200M 结果已补齐为 `+61.71%`。当前平台差值为 `47.15% - 61.71% = -14.56pp`，与“Kunpeng 至少高 10pp”的目标相差 24.56pp，目标未达。
3. Memtable Bloom 的收益具有强 query 依赖。旧 15q clean ablation 为 `-1.36%`；本次同 binary 50M ABBA 短筛中，q5 `+5.24%`、q15 `-0.17%`、q17 `+1.65%`，三查询算术平均 `+2.24%`。q5 的独立四腿 profile 已证明收益来源：`InlineSkipList::FindGreaterOrEqual` cycles 从平均 `1.675%` 降至 `0.370%`（-77.91%），KeyComparator 从 `1.625%` 降至 `0.825%`（-49.23%），`memcmp` 从 `1.165%` 降至 `0.810%`（-30.47%）；新增 Bloom 检查内联在 `MemTable::Get`，约占 1.06%，读侧 self-cycle 集合净减少约 1.40 个采样百分点。即负点查在进入 SkipList 前被早拒绝，写侧成本不变。
4. 早期 q5/q15/q17 Prefetch 单变量因 `tasksBuilt=0` 无法归因。进一步审计发现 `async-chunks` 曾从静态全局配置读取，而不是 TaskManager 实际配置；修复后以 q17、VCache=64 做了真实触发 ABBA。off 均值 85.42、on 均值 82.25 K/s/core，Prefetch 增量为 `-3.71%`。两条 on 腿各构建约 103 万个任务、执行约 88--90 万个任务并产生约 131 万次 point get，但最终各只有 104 次 staging promotion，约 132 万次 live read 与 in-flight 预取竞争。结论是当前 overlap 实现虽能安全回退，但任务太细、领先时间不足，不能保留为有效性能项。
5. 已实现的 native request plane 在两平台均退化，Kunpeng 少退化 1.33pp，但离 10pp 不足。下一版必须以自适应门槛和 ARM-only kernel 为前提，不能默认全量过 JNI。
6. 已有 tsv110/LSE、BiSheng 和 indexed-skiplist 证据表明，“只换编译器/指令开关”不足以形成 10pp。后续 ARM-only 路径必须同时减少 Java 对象/JNI 边界次数，并用 runtime counter 证明命中率和摊销条件。
7. 代码审计发现 `native.request-plane.min-batch-size` 原先只约束 prepared-key prefetch，native mailbox compact 和 native LocalPreAgg 仍会对小批次跨 JNI。当前修复将三条 batch 路径统一 fail-open 到 Java，并分别记录 threshold fallback，避免把门槛回退混进异常/slot 耗尽计数。

## 待完成实验（按证据缺口排序）

1. Prefetch 仅在完成“窗口级批次、足够 lead、限制每窗口 in-flight、promotion/admission 自适应”后再复测；当前实现不计入有效技术收益。
2. 修复 native request-plane 的 O(n²) 去重/重复 CRC、MapSnapshot 组合 key 歧义、first-seen 顺序和 generation 粒度，并加入 ARM/x86 runtime counter。
3. 双平台做一轮短筛：Java request plane vs 自适应 native；x86 必须 runtime fallback，Kunpeng 仅在门槛满足时走 ARM 路径。
4. 候选在 Kunpeng 上有正增量且双平台差值方向正确后，跑 15q 100M R1；达到目标再扩 R3。

## 2026-08-19 归因短筛原始结果

远端证据目录：`/home/wuql/flink-cluster/experiments/cachekit-kunpeng-differential-20260818`。
本地紧凑副本：`/mnt/data2/wuql/flink-cluster/OmniStateStore/dse_results/cachekit-kunpeng-differential-20260818`。

| Query | Mem Bloom off | Full-A | Prefetch off | Full-B | Full 均值 | Mem Bloom 增量 | Prefetch 表面差值 | 机制判定 |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| q5 | 40.94 | 43.25 | 42.16 | 42.92 | 43.09 | +5.24% | +2.19% | Prefetch 未触发；Bloom 有正信号 |
| q15 | 21.03 | 21.33 | 20.57 | 20.66 | 20.99 | -0.17% | +2.07% | Prefetch 未触发；Bloom 中性 |
| q17 | 68.98 | 70.00 | 69.62 | 70.23 | 70.12 | +1.65% | +0.71% | Prefetch 未触发；Bloom 小正 |

上述 K/s/core 均来自完成的 50M measured job；8 TM、16 slots、正 cores、无 checkpoint，且通过 `raw throughput / cores` 公式复核。三查询 Mem Bloom 提升百分比算术平均 `+2.24%`。Prefetch 表面差值的算术平均 `+1.66%`，但因 `tasksBuilt=0` 明确作废。

## 2026-08-19 Prefetch 真实触发 ABBA

协议：Kunpeng q17、100M、8 TM/16 slots、无 checkpoint；为强制制造真实 miss，将 VCache 容量从规范 FullOpt 的 8000 临时缩小到 64。该实验只回答机制和增量，不替代 15q FullOpt 主结果。

| Leg | Prefetch | K/s/core | tasks built | tasks executed | point gets | staging promoted | live-read raced in-flight |
|---|---|---:|---:|---:|---:|---:|---:|
| on-a | on | 81.18 | 1,029,313 | 897,717 | 1,337,269 | 104 | 1,314,886 |
| off-a | off | 87.95 | 0 | 0 | 0 | 0 | 0 |
| off-b | off | 82.89 | 0 | 0 | 0 | 0 | 0 |
| on-b | on | 83.32 | 1,027,380 | 880,185 | 1,310,383 | 104 | 1,328,480 |

ABBA 算术均值：off `85.42`，on `82.25` K/s/core，增量 `(82.25 / 85.42 - 1) = -3.71%`。worker failure 为 0，结果正确完成；退化来自过晚预取和每 8 records 的任务风暴，而非 job failure。约百万级任务和百万级 point get 只换来 104 次真正消费，说明当前时间重叠窗口不足。

紧凑证据：`dse_results/cachekit-prefetch-overlap-q17-100m-20260819/final/PREFETCH_OVERLAP_RESULTS.json`；归档 SHA-256 `0c8f31838050320d046230a5128d7574e90a71353a74b8ef737ccd48f0e73d19`。

## 证据门槛

- 每腿：`real_job_completed=true`、无 `real_job_failed`、8 TM、summary cores > 0、原始 K/s/core 可追溯。
- 配置：保存最终渲染配置及 SHA-256；明确 checkpoint/bypass/mailbox/Chen 状态。
- 归因：性能结果和机制计数必须来自同一 binary、同一 leg，不能用旧日志解释新结果。
- 差异目标：`Kunpeng uplift - x86 uplift >= 10pp`；未达到时如实报告，不以选择性删 query 达标。

## 2026-08-19 q15/q17 双平台 profiling

使用与最新 200M FullOpt 主结果相同的 commit、配置、2×4 TM 拓扑和无 checkpoint 协议，只增加 30 秒 `cycles:u` 采样。四腿在两平台均完成且 lost samples=0。

| 平台 | Query | RDB | FullOpt | K/s/core 提升 | RDB→FullOpt cores | wall 加速 |
|---|---|---:|---:|---:|---:|---:|
| x86 | q15 | 5.59 | 25.82 | +361.90% | 11.50→24.74 | 9.94× |
| Kunpeng | q15 | 10.51 | 36.33 | +245.67% | 7.52→23.48 | 10.80× |
| x86 | q17 | 36.89 | 85.64 | +132.15% | 27.01→26.01 | 2.24× |
| Kunpeng | q17 | 49.21 | 89.89 | +82.67% | 27.04→26.27 | 1.77× |

q15 的 Kunpeng wall 加速更高，但 cores 放大为 3.12×，显著高于 x86 的 2.15×；这才是 per-core 提升偏低的直接原因。q17 则是 Kunpeng RDB 基线更强、FullOpt 绝对吞吐与 x86 接近，形成相对提升的分母效应。两平台 q17 FullOpt 后 RocksDB 读热点都基本退场，后续主线改为降低聚合/对象/GC/写侧 CPU，而不是继续只优化 Bloom 或 Get。完整报告见 `Q15_Q17_CROSSHOST_PROFILE_REPORT_ZH.md`。
