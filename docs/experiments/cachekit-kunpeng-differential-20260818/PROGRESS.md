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
| `cachekit-idmg-fullopt-nockpt-200m-paired-2x4-r1-20260812` | x86, 200M, R1 | 同协议 RocksDB vs FullOpt | 尚缺 q16 FullOpt、q17 两腿 | 与 Kunpeng 配对 | 正在原地补齐；不得用 27/30 腿算总结果 |
| `cachekit-kunpeng-fullopt-bloom-matched-r123-20260804/final/THREE_ROUND_REPORT.md` | Kunpeng, 100M, R1-R3 | FullOpt control vs `+SST/Mem Bloom` | +11.08% 增量 | Bloom 叠加栈的收益 | 非 RocksDB 到 FullOpt 的端到端提升 |
| `cachekit-kunpeng-clean-bloom-full15-20260803/CLEAN_BLOOM_REPORT_ZH.md` | Kunpeng, 100M, R1 | RocksDB vs Bloom-only | SST +9.36%; Mem -1.36%; combined +8.59% | Bloom 组件拆解 | 单轮；说明 combined 的主要收益来自 SST Bloom，不支持“Mem Bloom 单独有收益” |
| `cachekit-kunpeng-fixed-prefetch-full15-20260803/FIXED_PREFETCH_REPORT.md` | Kunpeng, 100M, R1 | scalar vs MultiGet prefetch | total -0.14% | 旧 prefetch 实现的独立增量 | 旧实现没有证明吞吐收益，需用 overlap/消费指标重新归因 |
| `cachekit-native-fullopt-200m-crosshost-20260817/RETEST_RESULTS_20260818.md` | x86 + Kunpeng, 200M, 3 variants | Java FullOpt+mailbox vs native request plane | x86 -4.12%; Kunpeng -2.79% | native correctness和真实 runtime dispatch | always-on JNI/重建开销大于 kernel 收益，不能作为 10pp 结论 |
| `cachekit-kunpeng-compiler-direct-r1-20260805/final/THREE_ROUND_REPORT_ZH.md` | Kunpeng, 100M, R1-R3 | generic vs tsv110/LSE | -0.16% | 编译器/原子指令轴 | 单独换编译参数没有稳定收益；不能作为架构差异化主线 |
| `cachekit-kunpeng-dstl-p3/refine-logs/KUNPENG_FROCKSDB_BISHENG_RESULTS_20260810.md` | Kunpeng native screening | BiSheng、WholeKeyIndexedSkipList、Direct-PUT | 编译器 micro +1.26%；JDK 三查询 +5.36% 但不稳定；skiplist canary -2.56% | 已筛过的硬件/编译器方向 | 不足 10pp；Direct-PUT 没有有效吞吐结果，不能计入性能结论 |

## 当前明确结论

1. 最新可复用的 Kunpeng 端到端结果是 200M R1 `+47.15%`，不是旧 100M 的 `+42.79%`。
2. x86 的匹配 200M 结果尚不完整；缺失腿补齐前，不能声称已达到或未达到 10pp 差异目标。
3. Memtable Bloom 目前没有独立正收益证据。旧 clean ablation 为 `-1.36%`；combined 的 `+8.59%` 主要由 SST Bloom 的 `+9.36%` 提供。
4. 旧 prefetch 独立实验近似中性。下一轮必须同时采集“提交、执行、去重、staging 命中、stale、live-read 重复”计数，才能区分没有 overlap、预取太早/太晚和重复 I/O。
5. 已实现的 native request plane 在两平台均退化，Kunpeng 少退化 1.33pp，但离 10pp 不足。下一版必须以自适应门槛和 ARM-only kernel 为前提，不能默认全量过 JNI。
6. 已有 tsv110/LSE、BiSheng 和 indexed-skiplist 证据表明，“只换编译器/指令开关”不足以形成 10pp。后续 ARM-only 路径必须同时减少 Java 对象/JNI 边界次数，并用 runtime counter 证明命中率和摊销条件。
7. 代码审计发现 `native.request-plane.min-batch-size` 原先只约束 prepared-key prefetch，native mailbox compact 和 native LocalPreAgg 仍会对小批次跨 JNI。当前修复将三条 batch 路径统一 fail-open 到 Java，并分别记录 threshold fallback，避免把门槛回退混进异常/slot 耗尽计数。

## 待完成实验（按证据缺口排序）

1. 完成 x86 200M R1 缺失 3 腿，生成严格的 15q 配对表。
2. 构建带 RocksDB Bloom/memtable ticker 与 Prefetch 生命周期计数的 runtime。
3. 双平台做一轮短筛：Mem Bloom off/on；保持 SST Bloom 与其余栈完全相同，优先 q5/q15/q17，再决定是否扩 15q。
4. 双平台做一轮短筛：Java request plane vs 自适应 native；x86 必须 runtime fallback，Kunpeng 仅在门槛满足时走 ARM 路径。
5. 候选在 Kunpeng 上有正增量且双平台差值方向正确后，跑 15q 100M R1；达到目标再扩 R3。

## 证据门槛

- 每腿：`real_job_completed=true`、无 `real_job_failed`、8 TM、summary cores > 0、原始 K/s/core 可追溯。
- 配置：保存最终渲染配置及 SHA-256；明确 checkpoint/bypass/mailbox/Chen 状态。
- 归因：性能结果和机制计数必须来自同一 binary、同一 leg，不能用旧日志解释新结果。
- 差异目标：`Kunpeng uplift - x86 uplift >= 10pp`；未达到时如实报告，不以选择性删 query 达标。
