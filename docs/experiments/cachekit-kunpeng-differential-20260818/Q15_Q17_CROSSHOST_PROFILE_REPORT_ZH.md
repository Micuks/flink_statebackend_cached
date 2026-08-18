# q15/q17 双平台 FullOpt 归因（2026-08-19）

## 1. 为什么只抓 q15/q17

最新无 profiler 的 200M、无 checkpoint、2 containers × 4 TM/container 配对结果中，x86 FullOpt 15Q 提升为 `+61.71%`，Kunpeng 为 `+47.15%`。q15 与 q17 合计贡献约 `-13.00pp`，解释总差距 `-14.56pp` 的约 89%。因此本轮不重复全 15Q，而是用与主结果相同的 binary、配置和拓扑，仅给 q15/q17 增加 30 秒 `cycles:u` 采样。

所有腿均满足：200M measured job、8 TM/16 slots、正 cores、无 checkpoint、`raw K/s / cores = K/s/core`、零 lost samples。带 profiler 的吞吐只用于同轮机制归因，不替代无 profiler 主结果。

## 2. 原始结果

| 平台 | Query | RDB K/s/core | FullOpt K/s/core | K/s/core 提升 | RDB cores | FullOpt cores | cores 倍率 | wall 加速 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| x86 | q15 | 5.59 | 25.82 | +361.90% | 11.50 | 24.74 | 2.15× | 9.94× |
| Kunpeng | q15 | 10.51 | 36.33 | +245.67% | 7.52 | 23.48 | 3.12× | 10.80× |
| x86 | q17 | 36.89 | 85.64 | +132.15% | 27.01 | 26.01 | 0.96× | 2.24× |
| Kunpeng | q17 | 49.21 | 89.89 | +82.67% | 27.04 | 26.27 | 0.97× | 1.77× |

FullOpt 的实际开关保持为：VCache、MapSnapshot、Mailbox、LocalPreAgg、SST/Mem Bloom 开；bypass=false；Chen COW/RYW/PQ=false；无 checkpoint。历史配置中的 Prefetch 不作为收益项：独立真触发 ABBA 已得到 `-3.71%`。

## 3. 根因一：q15 不是 wall throughput 弱，而是 CPU 放大过大

q15 的 Kunpeng FullOpt wall throughput 为 `853.09 K/s`，高于 x86 的 `638.91 K/s`；Kunpeng 的 wall 加速 `10.80×` 也高于 x86 的 `9.94×`。但 Kunpeng 从 RDB 到 FullOpt 的 cores 从 `7.52` 增到 `23.48`，放大 `3.12×`；x86 只从 `11.50` 增到 `24.74`，放大 `2.15×`。因此 Kunpeng 的 K/s/core 提升少 `116.23pp`。

这说明 q15 的下一优化目标不是继续追求 wall throughput，而是在不降低 `853 K/s` 的前提下降低 FullOpt CPU。若保持 wall throughput，要让 q15 的 K/s/core 提升达到 x86+10pp，FullOpt cores 需从 `23.48` 降至约 `18.2`，即减少约 22.5%。

## 4. 根因二：q17 是更强 RDB 基线撞上相近 FullOpt 上限

q17 Kunpeng RDB wall throughput 为 `1330.80 K/s`，比 x86 的 `996.40 K/s` 高约 33.6%；FullOpt wall throughput 为 `2360.97 K/s`，也比 x86 的 `2227.97 K/s` 高约 6.0%。两平台 FullOpt 都接近同一吞吐上限，但 Kunpeng 的起点更高，故相对提升低 `49.48pp`。

q17 的 cores 比率在两平台几乎相同（x86 `0.96×`，Kunpeng `0.97×`），不是 cores 统计错误。要达到 x86+10pp，需要把 Kunpeng FullOpt K/s/core 从 `89.89` 提到约 `119.1`，即约 +32.5%；可以来自降低 FullOpt cores 至约 `19.8`，或等价提高 wall throughput。

## 5. 热点证据

### q17

- x86 RDB 的主要状态读热点包括两份 `MemTable::KeyComparator` self sample（合计约 9.41%）、`FindGreaterOrEqual` 3.22%、`BytewiseComparatorImpl::Compare` 3.04%、`FindSpliceForLevel` 2.36%。FullOpt 后这些读热点大幅下降，剩余以写侧 splice、GC、C2 和 datagen 为主。
- Kunpeng RDB 的 `FindGreaterOrEqual` 2.39%、KeyComparator 2.16%、`memcmp` 1.57%；FullOpt 后读侧同样基本退场，显著热点变为写侧 `RecomputeSpliceLevels` 1.23%、datagen `dpow` 1.27% 和 G1 copy/queue。
- 结论：CacheKit 已经在两平台消除了 q17 的大部分状态读成本。继续只优化 Bloom、MultiGet 或 RocksDB Get 不能补齐 10pp；应减少 FullOpt 的对象、聚合编排、写侧和 GC CPU。

### q15

- x86 RDB/FullOpt 都保留明显的 MemTable comparator/skiplist 热点；FullOpt 中 `FindGreaterOrEqual` 3.60%，两份 KeyComparator 合计 5.80%，Bytewise compare 2.02%。
- Kunpeng FullOpt 中 `memcmp` 2.69%、`FindGreaterOrEqual` 2.63%、KeyComparator 1.82%，仍说明大量聚合后状态访问落到 MemTable。
- 结论：q15 同时需要降低 LocalPreAgg/mailbox 的 Java CPU，并可把剩余 exact-key MemTable 比较作为 AArch64 kernel 的次级靶点。只做后者不足以解释 22.5% cores 目标。

## 6. 实现决策

1. **主线：低分配、批次级 native accumulator seam。** 当前 native LocalPreAgg 只在 C++ 做 key grouping，fold/accumulator 仍逐组回 Java，因此复测为 x86 `-1.38%`、Kunpeng `-5.51%`。下一版必须让 JNI 返回可直接消费的连续 group ranges，并复用 direct buffer；若不能把 accumulator 本身下沉，则不再默认启用。
2. **AArch64-only dispatch。** Kunpeng 使用 128B bucket、CRC32/NEON/SVE-256；x86 保持 Java fallback。必须由 runtime feature/counter 证明走到 ARM kernel，不能只靠配置名。
3. **q15 快速 gate。** 先看 FullOpt cores 是否在 wall throughput 不降的前提下下降；目标至少 -10%，有正信号再跑 q17 和 15Q。要弥合当前 q15 差距，最终需约 -22.5%。
4. **q17 gate。** 重点记录 GC CPU、allocation rate、LocalPreAgg input/groups、state reads/writes 和 output rate；读路径优化只作为次要实验。

## 7. 证据位置

- x86 expdir：`/mnt/data2/wuql/flink-cluster/experiments/cachekit-q15q17-profile-x86-20260819-v2`
- Kunpeng expdir：`/home/wuql/flink-cluster/experiments/cachekit-q15q17-profile-kunpeng-20260819`
- 紧凑副本：`dse_results/cachekit-q15q17-crosshost-profile-20260819/{x86,kunpeng}`
- source commit：`2f9582a24f98e1d7479b390206eca337c032d207`

