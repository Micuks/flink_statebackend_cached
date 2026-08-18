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

## 当前明确结论

1. 最新可复用的 Kunpeng 端到端结果是 200M R1 `+47.15%`，不是旧 100M 的 `+42.79%`。
2. x86 的匹配 200M 结果已补齐为 `+61.71%`。当前平台差值为 `47.15% - 61.71% = -14.56pp`，与“Kunpeng 至少高 10pp”的目标相差 24.56pp，目标未达。
3. Memtable Bloom 的收益具有强 query 依赖。旧 15q clean ablation 为 `-1.36%`；本次同 binary 50M ABBA 短筛中，q5 `+5.24%`、q15 `-0.17%`、q17 `+1.65%`，三查询算术平均 `+2.24%`。q5 同腿 ticker 显示约 1425 万次 memtable hit 和 6612 万次 miss，说明它有大量可被 Bloom 提前拒绝的 memtable 负查；但仍需 CPU profile 证明 comparator/skiplist 工作确实下降。
4. 本次选择 q5/q15/q17 做 Prefetch 单变量是无效归因：三者 `tasksBuilt=0`。q5 是窗口 namespace，被 `VoidNamespace` 正确性门控排除；q15/q17 被 LocalPreagg 整批消费，设计上优先走分组后即时批读，不进入 speculative worker。表面 `+1.66%` 只能记作漂移，不能记作 Prefetch 收益。下一轮改用历史同 runtime 日志已证明 `tasksBuilt>0` 的 q9，并强制要求 submitted/executed/promoted 均大于零。
5. 已实现的 native request plane 在两平台均退化，Kunpeng 少退化 1.33pp，但离 10pp 不足。下一版必须以自适应门槛和 ARM-only kernel 为前提，不能默认全量过 JNI。

## 待完成实验（按证据缺口排序）

1. q9 做 Prefetch off/on ABBA；必须 `tasksBuilt/tasksExecuted/promoted > 0`，否则该腿无效。
2. q5 做 Mem Bloom off/on ABBA，并对两腿各采 30 秒 ARM cycles 调用栈，验证 `KeyComparator/InlineSkipList` 或 RocksDB memtable 查找热点是否下降。
3. 基于 profile 和机制计数决定保留项；不把 q15/q17 的 Prefetch 空转差值写成技术收益。
4. 双平台做一轮短筛：Java request plane vs 自适应 native；x86 必须 runtime fallback，Kunpeng 仅在门槛满足时走 ARM 路径。
5. 候选在 Kunpeng 上有正增量且双平台差值方向正确后，跑 15q 100M R1；达到目标再扩 R3。

## 2026-08-19 归因短筛原始结果

远端证据目录：`/home/wuql/flink-cluster/experiments/cachekit-kunpeng-differential-20260818`。
本地紧凑副本：`/mnt/data2/wuql/flink-cluster/OmniStateStore/dse_results/cachekit-kunpeng-differential-20260818`。

| Query | Mem Bloom off | Full-A | Prefetch off | Full-B | Full 均值 | Mem Bloom 增量 | Prefetch 表面差值 | 机制判定 |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| q5 | 40.94 | 43.25 | 42.16 | 42.92 | 43.09 | +5.24% | +2.19% | Prefetch 未触发；Bloom 有正信号 |
| q15 | 21.03 | 21.33 | 20.57 | 20.66 | 20.99 | -0.17% | +2.07% | Prefetch 未触发；Bloom 中性 |
| q17 | 68.98 | 70.00 | 69.62 | 70.23 | 70.12 | +1.65% | +0.71% | Prefetch 未触发；Bloom 小正 |

上述 K/s/core 均来自完成的 50M measured job；8 TM、16 slots、正 cores、无 checkpoint，且通过 `raw throughput / cores` 公式复核。三查询 Mem Bloom 提升百分比算术平均 `+2.24%`。Prefetch 表面差值的算术平均 `+1.66%`，但因 `tasksBuilt=0` 明确作废。

## 证据门槛

- 每腿：`real_job_completed=true`、无 `real_job_failed`、8 TM、summary cores > 0、原始 K/s/core 可追溯。
- 配置：保存最终渲染配置及 SHA-256；明确 checkpoint/bypass/mailbox/Chen 状态。
- 归因：性能结果和机制计数必须来自同一 binary、同一 leg，不能用旧日志解释新结果。
- 差异目标：`Kunpeng uplift - x86 uplift >= 10pp`；未达到时如实报告，不以选择性删 query 达标。
