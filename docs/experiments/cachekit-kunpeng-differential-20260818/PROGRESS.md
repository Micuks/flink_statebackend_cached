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

## 2026-08-19 Native LocalPreAgg flat-slice 快筛

为了验证 review 指出的 per-group `ArrayList` 分配，native 分组结果改成一次计数、一个扁平 `Object[]` 和每组零拷贝 slice。单测通过后在 Kunpeng q15 做 50M 三腿短筛；8 TM、16 slots、无 checkpoint，三腿均由 `raw throughput / cores` 复核。

| Leg | 实现 | raw K/s | cores | K/s/core | wall s |
|---|---|---:|---:|---:|---:|
| off-a | Java FullOpt | 792.40 | 24.50 | 32.34 | 63.099 |
| on-a | ARM retained native，不开 native preagg | 776.52 | 23.32 | 33.29 | 64.390 |
| on-b | ARM retained + flat-slice native preagg | 765.64 | 23.83 | 32.12 | 65.305 |

增量：ARM retained 相对 Java `+2.94%`；flat-slice 相对 Java `-0.68%`，相对 ARM retained `-3.51%`。on-b 日志中 16 个 backend 实例累计 715,889 个 native grouping batch、45,743,398 个输入 key、6,089,584 个 group、106 次安全回退，选择的 kernel 为 `aarch64-sve256-hybrid-crc32c`；因此退化不是开关未生效。

该轮还发现 runner 元数据写 50M、实际配置为 200M 的协议错误；公式门槛立即拒绝该结果，原始无效证据隔离在 `results/invalid-protocol-200m-metadata/`。纠正 `events.num=50M` 后从零重跑，上表只采用纠正后的完整结果。

根因是 grouping 前仍为每批复制 serializer、完整序列化 generic RowData key，prefetch 随后又序列化 unique key/namespace；native 只替换 grouping，fold 仍在 Java。flat-slice 只去掉一部分容器分配，未覆盖 JNI 和重复序列化成本，不能扩 15Q。

紧凑证据：`dse_results/cachekit-native-preagg-flat-q15-50m-20260819/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-preagg-flat-q15-50m-kunpeng-20260819`。

## 2026-08-19 Native LocalPreAgg 4B hash-token 复测

在 flat-slice 版本上进一步去掉每 batch 的 `TypeSerializer.duplicate()` 和完整 generic key 序列化。Native grouping 只接收 Java `hashCode` 的 4-byte token；equal key 必须有相同 token，碰撞产生的过度分组由现有 Java equals/first-seen 验证检出并安全回退。

| Leg | raw K/s | cores | K/s/core | wall s |
|---|---:|---:|---:|---:|
| Java FullOpt | 819.98 | 24.32 | 33.72 | 60.977 |
| ARM retained | 786.78 | 24.00 | 32.78 | 63.550 |
| ARM retained + hash-token native preagg | 782.03 | 23.87 | 32.76 | 63.936 |

Hash-token 相对 ARM retained `-0.06%`，相对 Java `-2.85%`。16 个 backend 实例累计 789,073 个 native batch、50,424,639 个输入 key、6,181,890 个 group，111 次安全回退，kernel 为 `aarch64-sve256-hybrid-crc32c`，三腿 8 TM/50M/no-checkpoint 审计全部有效。

结论：完整 key 序列化确实是上一版退化的一部分，去除后从 `-3.51%` 回到近中性；但 JNI、native plan→Java 重建、record key/value 抽取以及 accumulator fold 仍覆盖了 kernel 收益。该 generic grouping helper 不扩 15Q。紧凑证据：`dse_results/cachekit-native-preagg-hashtoken-q15-50m-20260819/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-preagg-hashtoken-q15-50m-kunpeng-20260819`。

## 2026-08-19 Native LocalPreAgg extraction-buffer reuse 复测

在 4B hash-token 版本上，仅为 `native.local-preagg.enabled=true` 的 treatment 增加线程本地 key/value extraction buffer；每批结束立即 `clear()`，避免长生命周期 task thread 持有 record。Java FullOpt 与 ARM retained 对照不进入复用分支。`LocalPreaggTest` 7/7 通过，模块打包通过；实现提交为 `ad651f0275286c57e6a26405d9f715ef2a52e073`。

| Leg | raw K/s | cores | K/s/core | wall s |
|---|---:|---:|---:|---:|
| Java FullOpt | 824.90 | 23.80 | 34.66 | 60.613 |
| ARM retained | 783.32 | 23.86 | 32.83 | 63.831 |
| ARM retained + hash-token native preagg + buffer reuse | 766.00 | 23.35 | 32.80 | 65.274 |

Buffer-reuse candidate 相对 ARM retained `-0.09%`，相对 Java FullOpt `-5.37%`。16 个 native summary 实例合计 675,739 batch、43,182,264 input key、5,844,170 group、110 次安全回退，kernel 为 `aarch64-sve256-hybrid-crc32c`；开关确实触发，三腿均通过 8 TM、50M、no-checkpoint、正 cores 与吞吐公式审计。

结论：复用两个 Java 容器仍不能覆盖 JNI、native plan 回传、Java plan 校验和逐 group accumulator fold 的成本。通用 JNI LocalPreAgg 路线连续三次由 `-3.51% → -0.06% → -0.09%` 收敛到中性但未转正，停止扩 15Q；只有能把业务 accumulator 语义一起下沉、显著减少 Java 回调次数的 operator-specific kernel 才值得重新立项。紧凑证据：`dse_results/cachekit-native-preagg-reuse-q15-50m-20260819/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-preagg-reuse-q15-50m-kunpeng-20260819`。

## 2026-08-20 Native grouping epoch table 与 Token32 packed plan

先把 native grouping 从每批清 64 KiB table、对已有 group 重算 fingerprint 的路径改为按 batch 上限分配的 epoch table；512-entry 配置下常规批次只递增 epoch，每 255 批才清 1 KiB epoch 区，并保证每个输入只计算一次 fingerprint。q16 50M、no-checkpoint 快筛结果为 `8.48 → 8.47 K/s/core`（`-0.12%`），wall throughput `-1.12%`。这证明 native microkernel 的 table clear/fingerprint 已不是主要瓶颈。紧凑证据：`dse_results/cachekit-native-group-epoch-50m-20260820/`。

随后实现 Token32 Packed Direct Plan V3：caller-owned direct token/plan buffers，packed plan 带 magic/version/sourceCount/groupCount、stable-first-source、offsets 与 source→group；Java 在处理任何 record 前逐 source 做 `Objects.equals` 身份验证，碰撞或畸形 plan 整批回退 Java。实现提交 `219f837010cd60cef77350ac8b418bd51a05098a`，分支 `codex/cachekit-native-token-plan-v3-20260820` 已推送。

正确性/ABI 门禁：native 2/2、JNI 2/2、ASan/UBSan 2/2，streaming Java 16/16，CacheKit/JNI Java 27/27；control/candidate 都通过 JDK 11 ABI probe，实际 kernel 均为 `aarch64-sve256-hybrid-crc32c`。

| Leg | Commit | raw K/s | cores | K/s/core | wall s |
|---|---|---:|---:|---:|---:|
| control | `20e545d13b` | 223.24 | 26.13 | 8.54 | 223.972 |
| Token32 V3 | `219f837010` | 223.68 | 26.17 | 8.55 | 223.536 |

Token32 V3 的 q16 K/s/core 增量为 `+0.12%`，wall throughput `+0.20%`，CPU `+0.15%`，低于 `+2%` 门槛，不扩轮、不作为独立性能项。两腿都是 50M、无 checkpoint、2 个物理 TM 容器/8 TM JVM/16 slots，每腿 43 个 CPU samples 均覆盖 8 TMs，吞吐公式审计有效。

runtime summary 中 control 为 1,407,555 native batches / 89,794,384 input keys / 44,416,777 groups，candidate 为 1,224,098 / 78,057,971 / 44,437,799。groups 几乎一致而进入新 grouping seam 的 batches/input keys 较少，说明该 activity counter 受 backend seam/批次分布影响，不能当作端到端 records 等价证明。功能正确性以差分、collision、malformed-plan、stable-order 测试为主；Nexmark 只证明真实作业未失败和性能口径有效。

下一主线按 profiling 排序转为：融合 Native Mailbox compact 与 prepared-key probe/MultiGet，复用 compact 的 selected source indexes 直接访问原 direct prepared arena，删除 `direct prepared key → heap byte[] → direct probe` 往返，仅为真正 RocksDB miss 物化 heap key。紧凑证据：`dse_results/cachekit-native-token-plan-q16-50m-20260820/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-token-plan-q16-50m-kunpeng-20260820`。

## 2026-08-23 Kunpeng chain-copy-elision 15Q 快筛

q17 CPU profile 显示 FullOpt 后主要热点已经从 RocksDB/state 转到 operator-chain 行复制：`RowDataSerializer.copy` inclusive 38.13%、`copyRowData` 36.04%、`StringDataSerializer.copy` 21.23%、`BinaryStringData.copy` 20.98%。全局 object reuse 的 q17 机制上限为 `77.17 → 106.58 K/s/core`（`+38.11%`），因此实现 AArch64-only、严格算子白名单的 chain-copy-elision；未知/用户算子保持 `CopyingChainingOutput` fail closed。

实现 commit 为 `f6ad18b95ad9049a19b89cfaf6951bd0f40729b3`。正式 15Q/50M/R1 同二进制 A/B 只切换 `state.backend.cachekit.arm.chain-copy-elision.enabled`，`pipeline.object-reuse=false`、无 checkpoint、2×4 TM/8 TM/16 slots。30/30 legs 和逐腿 SHA 审计通过。

| 分组 | 算术平均提升 |
|---|---:|
| 前八 | +24.60% |
| 后七 | +42.42% |
| ValueState-only | +24.29% |
| 任意 state | +27.92% |
| 15Q 总计 | **+32.91%** |

15Q 逐 query 提升为：q4 +24.58%、q5 +31.92%、q8 +38.82%、q9 +14.07%、q11 +12.10%、q18 +21.17%、q19 +35.35%、q20 +18.76%、q3 +69.50%、q7 +7.66%、q12 +39.12%、q13 +102.84%、q15 +28.77%、q16 +13.22%、q17 +35.81%。本轮只通过了系统级 Flink runtime 优化的 `>=+10%` 门槛；不得计入 CacheKit 状态访问链路的 Kunpeng/ARM native `>=+10%` 目标。

q3/q13 的 optimized 50M job 短于原 Nexmark source metric 注册窗口，因此 A/B 同时用从当前 Nexmark 源码构建的 event-count CPU collector：吞吐仍为 events/wall，CPU 仍来自 8-TM receiver，过滤冷样本并取 retained CPU 中位数；17 tests、0 failure/error。完整远端 expdir：`/home/wuql/flink-cluster/experiments/cachekit-kunpeng-arm-copy-elision-v3-15q-50m-r1-20260823`；本地紧凑证据：`OmniStateStore/dse_results/cachekit-kunpeng-arm-copy-elision-15q-50m-r1-20260823/v3-final/`。

### 2026-08-23 归因纠偏：chain-copy-elision 不属于状态访问链路优化

上述实现修改的是 `OperatorChain.wrapOperatorIntoOutput`：在上游 operator 与下游 operator 之间把 `CopyingChainingOutput` 换成 `ChainingOutput`，发生在记录进入 stateful operator 之前。它没有进入 `KeyedStateBackend`、CacheKit state wrapper、key/value serialization、JNI、RocksDB lookup/update 或状态值物化路径；`aarch64-only` 只是启用条件，也没有使用 SVE/LSE/CRC/PMULL 等 ARM 能力。

GitHub `feat/safe0-analyzer@4b99bbffc0` 已在同一 `OperatorChain` 接缝实现基于字节码安全证明的 per-edge zero-copy，因此当前白名单版还与既有工作重复。`+32.91%` 仍是有效的 Flink runtime record-forwarding 实测结果，也能证明 Kunpeng 上 operator-chain 深拷贝代价很高，但把它表述为“CacheKit Kunpeng native 状态访问链路达到 +10%”是 misleading，撤回该归因和达标判断。相关 `CacheKitArmChainCopyElision` 实现、测试及 `OperatorChain` 接线已从当前分支直接删除；历史结果仅保留作错误归因审计，不再作为候选或可配置功能。

后续严格计分边界：实验 A/B 的 chain-copy-elision 策略必须相同；候选差异只能位于 `State API -> CacheKit -> JNI/RocksDB -> value materialization` 范围。只有这个差分相对同配置 FullOpt 的 15Q 算术平均提升达到 `>=+10%`，才算完成状态访问链路 Kunpeng/ARM native 目标。

## 2026-08-23 q15 状态访问链路 profile 与 coherent native MapState 候选

在 chain-copy-elision 完全移除后，用 canonical FullOpt+mailbox、50M、无 checkpoint、2×4 TM 拓扑重新采集 q15 async-profiler。作业有效完成：8 TM、16 slots、800.52 K/s wall throughput、23.29 cores、34.37 K/s/core；98,337 个 CPU samples，8 份 folded 与 8 份 HTML 全部通过 SHA-256 校验。

caller-context 汇总表明，`RocksDB.get` inclusive 15.15% 几乎全部来自：

```text
GroupAggsHandler.accumulate
  -> StateMapView
  -> UserFacingMapState.get
  -> CachedInternalMapState.get
  -> RocksDBMapState.get
  -> RocksDB.get
```

因此此前 ValueState native shadow/direct-GET 方向没有命中 q15 的主要状态热点。紧凑 profile 证据：`dse_results/cachekit-kunpeng-native-state-path-20260823/q15-fullopt-state-profile/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-sve-fullopt-mailbox-15q-50m-r1-20260822/diagnostics/q15-fullopt-state-profile-v2`。

进一步代码审计发现现有 native MapState exact-entry cache 使用单一 state-wide `nativeGeneration`。每次 `put/remove/putAll` 都递增 generation，典型聚合的 `get(userKey) -> put(userKey, value)` 会立即作废刚由 read miss 填入的 native entry：支付 probe/JNI/fill 开销，却无法在后续访问形成命中。这解释了旧 q4 native map-cache 快筛 `-11.38%`，也不是 ARM kernel 本身的有效否定。

新候选改为 mutation-coherent native MapState：

- point cache 与 snapshot 使用独立 generation；
- `put/remove/putAll/iterator.remove` 在权威 mutation 成功后，通过现有 128B native request plane 原位发布 exact composite key 的新值或 negative entry；
- point mutation 只失效 snapshot，不冲掉无关 point entries；
- `clear()` 等集合级 mutation 仍同时推进两个 generation；
- 独立记录 mutation attempts/applied/negative/superseded/failures，native 发布失败不回滚或重做权威状态 mutation。

针对 `get→put→get`、remove tombstone、clear 全局失效的测试均通过；CacheKit 模块 229 tests、0 failure/error、9 个既有 native-environment skip。下一门槛是 Kunpeng q15 同二进制 A/B：先证明 native hits 和 mutationApplied 为正且没有 failure，再以 K/s/core 判断是否扩 15Q。

### q15 mutation-coherent native MapState ABBA 结果

同一 binary 仅切换 `state.backend.cachekit.native.map-cache.enabled`，按 `off_a/on_a/on_b/off_b` 顺序完成 50M、无 checkpoint、2×4 TM 的四腿 ABBA；四腿均为真实 q15、8 TM/16 slots、正 cores，最终摘要和紧凑证据通过 SHA-256 校验。

| Leg | raw K/s | cores | K/s/core | native useful hit rate | mutation applied |
|---|---:|---:|---:|---:|---:|
| off_a | 808.04 | 23.48 | 34.41 | — | 0 |
| on_a | 820.14 | 24.01 | 34.16 | 95.63% | 12,368,012 |
| on_b | 802.32 | 23.76 | 33.77 | 95.63% | 12,369,072 |
| off_b | 837.98 | 24.50 | 34.21 | — | 0 |

off 均值为 `34.31`，on 均值为 `33.97 K/s/core`，增量为 `-1.01%`；配对 A/B 分别为 `-0.73%` 和 `-1.29%`，最大顺序漂移仅 `1.14%`。两条 on 腿合计 202,400,000 probes、193,548,284 hits、8,851,716 misses/fills、24,737,084 mutation applied，failure/fallback/mutation failure 均为 0，实际 kernel 为 `aarch64-sve256-hybrid-crc32c`。因此本轮既不是开关未触发，也不是失效策略导致低 hit；mutation-coherent 机制已经成立，但性能 gate 明确失败，不扩 15Q。

结合 profile，根因是当前 shadow cache 为每个 `MapState.get` 额外增加一次 Java→JNI probe，并为每个 point mutation 再增加一次 Java→JNI publish；即使 95.63% 的 probe 避免了后续 RocksDB lookup，这两个新增边界仍抵消了收益。下一候选不得继续在 Java wrapper 外叠加 JNI：应把 ARM exact-entry probe/fill/update 融入原有 `RocksDB.get/put/delete` JNI 边界，复用已经发生的 native transition，并让非 snapshot point lookup 在同一次调用中完成 cache hit 或权威 RocksDB fallback。紧凑证据：`dse_results/cachekit-native-map-coherent-q15-50m-abba-20260823/`；远端：`/home/wuql/flink-cluster/experiments/cachekit-native-map-coherent-q15-50m-abba-20260823`。

## 2026-08-23 FrocksDB JNI-fused ARM point cache 候选

根据上述负结果，下一候选不再从 Java wrapper 额外调用 native cache，而是直接修改 FrocksDB 6.20.3 已存在的 byte-array `get/put/delete` JNI helper：

- 非 snapshot point `get` 在同一次 JNI transition 内先查 4-way exact-entry cache；miss 才进入权威 `DB::Get`，结果或 NotFound tombstone 原位填充；
- `put/delete/singleDelete` 仅在权威 mutation 成功后原位更新相同 entry；WriteBatch、merge、deleteRange 等无法逐项安全解释的 mutation 只做全 cache 失效；
- cache key 包含 column-family id 和完整 serialized key，命中必须做 exact compare，不以 hash 近似替代状态语义；
- Kunpeng binary 用 `-march=armv8.2-a+sve+crc` 构建，hash 使用 ARM CRC32C，完整 key compare 使用 SVE1，bucket 起点按 128B 对齐；非 AArch64 默认永久关闭；
- treatment 只由 TaskManager 环境变量 `CACHEKIT_ROCKSDB_ARM_FUSED_POINT_CACHE` 控制，control/candidate 的 Flink 配置、CacheKit jar、FrocksDB binary 和拓扑完全相同。

实现位于 FrocksDB 分支 `codex/cachekit-fused-arm-point-cache-20260823`，commit `82997c5b531cc95fe3041616e7c70a8e9ad5b58d`。C++ 3/3 单测、standalone smoke、x86 JNI 语法检查和完整 `rocksdbjava` link 均通过。由于现有运行协议会跳过 DB dispose，统计改为每 2^20 probes 输出一次累计进度，实验审计按 JVM/cache 只取最后快照，避免遗漏或重复相加。

快速 gate 已排队到 Kunpeng：q15、50M、无 checkpoint、2 个物理 TM 容器 × 每容器 4 个 TM JVM、16 slots，顺序 `off_a/on_a/on_b/off_b`。control 已与 q15 profile 的 canonical FullOpt 配置逐 key 比对为零差异：request-plane 保持 `false/scalar`，Bloom/VCache/MapSnapshot/prefetch/mailbox/local-preagg 开启，bypass 与 Chen 关闭。只有 fused point-cache 环境开关不同。远端 expdir 为 `/home/wuql/flink-cluster/experiments/cachekit-fused-arm-point-cache-q15-50m-abba-20260823`；只有 ABBA 两个配对都为正且均值至少 `+2%` 才继续 profiling/迭代，q15 达到 `+10%` 才直接扩 15Q。
