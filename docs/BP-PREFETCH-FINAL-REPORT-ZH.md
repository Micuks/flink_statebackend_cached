# CacheKit 背压预取方向:实现修复与最终验证报告

日期:2026-07-07
代码:`flink-statebackend-cached-bp-prefetch` @ `0e3ccb1571`(base:feat/cachekit-map-snapshot-cache)
部署:flink-dist / flink-table-runtime / flink-statebackend-cachekit 三 jar 挂载,1 JM + 2 TM 容器 × 4 TM JVM × 2 slots = 16 slots

## 一、结论摘要

| 指标 | 数值 |
|---|---|
| 15q throughput/core 均值(vs 纯 RocksDB,50M×3 轮交错,坏样本清洗后) | **+31.34%** |
| 剔除 q12(proc-time 非确定 query)后 | **+33.66%** |
| CDC 正确性 | **14/14 确定性 query PASS**(q12 为 proc-time 例外) |
| 预取机制单变量隔离增量(cache 承接层不变,仅开关 prefetch) | 约 +1.5%(小正) |

最终配置 = CacheKit ValueState 缓存(仅 VoidNamespace state)+ 真异步背压预取 + mailbox-batch + local-preagg + MapState 缓存全关。

## 二、逐 query 结果(50M,3 轮均值,throughput/core)

| query | rocksdb K/s/core | cachekit K/s/core | speedup | 备注 |
|---|---:|---:|---:|---|
| q3 | 440.5 | 408.6 | -7.3% | wall 全轮相同,cores 采样噪声带(三套数据里在 ±10% 摆动) |
| q4 | 78.6 | 104.1 | **+32.6%** | |
| q5 | 246.0 | 226.3 | -8.0% | 滑动窗口;唯一遗留真回退 |
| q7 | 92.0 | 93.3 | +1.4% | |
| q8 | 432.6 | 445.0 | +2.9% | VoidNamespace 门控修复(门控前 -10.2%) |
| q9 | 58.3 | 58.1 | -0.4% | |
| q11 | 125.1 | 124.3 | -0.6% | 门控放弃窗口缓存收益(门控前 +12.9%,但正确性优先) |
| q12 | 303.6 | 300.1 | -1.2% | proc-time query,历史口径剔除 |
| q13 | 125.5 | 130.7 | +4.2% | |
| q15 | 42.4 | 132.2 | **+304%** | wall 725s→121s、总 TPS ~12×,收益真实;rocksdb 腿 cores 采样偏低使 per-core 数值口径需注明 |
| q16 | 20.7 | 32.7 | **+57.5%** | wall 506s→271s |
| q17 | 206.4 | 321.4 | **+55.7%** | |
| q18 | 149.0 | 185.0 | **+24.2%** | |
| q19 | 139.5 | 142.2 | +1.9% | |
| q20 | 76.0 | 78.2 | +2.9% | |

清洗规则:剔除 cores≤1 或 >16 的采样轮(q15 rocksdb r2 cores=1);q12 按 proc-time 口径单列。

## 三、做了什么(与继承实现的差异)

继承的实现经 code review 证实预取为同步伪预取:`prefetchAsync` 直接同步执行;预取读发生在 mailbox 线程关键路径上,与算子随后的读完全相同,零延迟隐藏;LocalPreagg 短路使收益 query 根本不执行预取;MapState `prefetchSnapshots` 每次全量 flush + 逐 key prefix iterator,只能缓存单 entry map,对 join 型 state 纯负担。隔离 A/B 实测预取增量 ≈ 0,与上述结构一致。

本轮修复与新增:

1. **真异步预取**(`8d7b0a104e`):共享单 daemon 线程 `cachekit-bp-prefetch`(有界队列、丢旧保新);mailbox 侧只做 key+namespace 序列化与入队;worker 走 `getSerializedValue` 这一 queryable-state 级线程安全通道读 RocksDB,结果进 ConcurrentHashMap staging;读路径 L1/L2 miss 后按 `writeGen` 世代校验 promote(只有写穿 delegate 的三个点 bump,普通脏写由 L1 屏蔽,stale-safe 有推导);TM 内线程存在性经 `/proc/*/task/*/comm` 实测确认;背压门控保持"非触发零开销"(ungated 对照 cores 6→9 印证门控必要性)。
2. **MapState 缓存问题两级处理**:`prefetchSnapshots` 默认关闭(`8d7b0a104e`);归因实验(rocksdb/cache-only/all-on 三腿)证明 q4/q9/q13/q20 的 -17%~-42% 回退 96% 来自 MapState 缓存 wrapper 本身 → MapState 缓存全关(`NO_MAP_CACHE`,`2c147c5729`),四个 query 全部恢复(q4 -33%→+28%)。
3. **VoidNamespace 门控**(`46ccb11e83`):CDC 隔离(bp 栈全关仍 FAIL)暴露 value 缓存 wrapper 对窗口(namespaced)state 的 write-back 语义问题;门控后只包 VoidNamespace state(GroupAgg/dedup/TopN),窗口 state 直通 RocksDB。性能上同时修复 q8(-10%→+3%)、缓解 q5(-13%→-8%)。
4. **CDC harness 修复**:q13 side_input 挂载(`83ecfd8567`);确定性 filesystem source 支持(`0e3ccb1571`);发现并修复窗口 query oracle 的两层非确定性——wall-clock datagen(每次运行窗口边界不同)与多 part 数据文件事件时间重叠导致的结构性 late-drop(串行读完首个 part 后 watermark 已推至末尾,后续 part 事件几乎全部丢弃,丢弃量随读取顺序漂移)。修复 = 按事件时间归并排序为单文件;排序源下 q7 A/A 自洽、q7/q8/q11 A/B 全部一致(101=101、66047=66047、76429=76429)。

## 四、收益来源的诚实归因

- **主要收益是 work-reduction 与缓存,不是预取本身**:local-preagg(q15/q16/q17 的折叠)+ VoidNamespace ValueState 缓存构成大头;15q 三腿套件中 cache-only vs rocksdb ≈ +10.8%。
- **预取的单变量隔离增量约 +1.5%**(cache 承接层不变仅开关 prefetch,剔除 q15 的 cores 采样假象后),在 q9/q7/q13 上有一致的小正信号。作为对照,继承的同步实现为 0。预取的价值定位:真实、零非触发开销、可写入机制描述,但不是数值主力。
- q15 的 +304% 为 per-core 口径;wall 佐证(725s→121s)与总 TPS(~12×)表明方向与量级真实,但 rocksdb 腿 cores 采样塌缩(1-2 核)使该 per-core 数字偏高,报告场合建议并列 wall 数据。

## 五、已知限制与后续方向

1. **q5 -8.0%**:滑动窗口 query 的遗留回退(mailbox-batch 已排除;窗口 state 已直通)。候选:StatePrefetcher 在窗口算子上的 key 提取空转(backend 无可预取 wrapper 时仍做 batch key 提取),可加"无 wrapper 短路"消除。
2. **q3 -7.3%**:三套数据在 ±10% 内摆动、wall 恒定,cores 采样噪声带,可用更多轮次收敛。
3. **q11/窗口缓存**:门控放弃了窗口 state 缓存的 +13%;若要拿回需修 write-back wrapper 对 namespaced state 的语义(费力,优先级低)。
4. **q12**:需要移植 falcon 侧 guard-gap first-window proctime oracle 才能闭环 CDC。
5. **100M 档**:本轮仅 50M×3;100M 套件约 10-12h 机时,可按需补。
6. 预取的进一步空间:submitPrefetch 目前按提交时刻单一 namespace 序列化,已由 VoidNamespace 门控自然约束;若扩展到 MapState 需先解决其缓存正确性。

## 六、复现索引

- 最终 perf:`results-cachekit-bp-prefetch/final3r-v2_20260707_002010/`(COMPARE-cachekit-bp-prefetch.md/json,90/90 legs)
- 归因:`results-cachekit-attribution/20260706_130933/`(三腿表 + no-map 验证腿)
- 15q 三腿隔离:`results-cachekit-prefetch-only/nomap15q_20260706_143910/`
- CDC:`results-cachekit-bp-prefetch-cdc/`(cdc14q_*.summary、isolation.summary、aa_*.diff、sorted_cdc.summary)
- 排序数据源:`nexmark-flink/data_sources/nexmark-events-1000000-sorted/`
