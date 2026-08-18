# CacheKit 最新双平台 200M R1 配对结果（2026-08-19 审计）

## 协议与配置

- Nexmark 15Q，200M measured events，2 containers × 4 TaskManager JVM/container，8 TM、16 slots。
- checkpoint 关闭；K/s/core 逐腿由 `raw K/s / cores` 复核。
- FullOpt：VCache、MapSnapshot、Mailbox、LocalPreAgg、SST/Mem Bloom 开；bypass=false；Chen COW/RYW/PQ=false。
- 组提升是每个 query 提升百分比的算术平均，不是先平均吞吐再相除。
- x86 采用 30/30 条有效腿；Kunpeng `R1_R1_RESULTS.json` 的审计为 valid，30/30 条有效腿。

## 每 query 原始 K/s/core 与提升

| Query | x86 RDB | x86 FullOpt | x86 提升 | Kunpeng RDB | Kunpeng FullOpt | Kunpeng 提升 | Kunpeng-x86 |
|---|---:|---:|---:|---:|---:|---:|---:|
| q4 | 12.83 | 18.13 | +41.31% | 18.37 | 29.09 | +58.36% | +17.05pp |
| q5 | 24.35 | 34.67 | +42.38% | 35.45 | 46.35 | +30.75% | -11.63pp |
| q8 | 90.45 | 85.74 | -5.21% | 101.79 | 95.83 | -5.86% | -0.65pp |
| q9 | 8.86 | 12.47 | +40.74% | 10.47 | 13.95 | +33.24% | -7.51pp |
| q11 | 13.84 | 16.35 | +18.14% | 25.69 | 28.02 | +9.07% | -9.07pp |
| q18 | 25.29 | 52.02 | +105.69% | 27.83 | 54.55 | +96.01% | -9.68pp |
| q19 | 31.11 | 29.73 | -4.44% | 32.01 | 30.81 | -3.75% | +0.69pp |
| q20 | 13.03 | 19.55 | +50.04% | 14.19 | 20.80 | +46.58% | -3.46pp |
| q3 | 139.36 | 137.02 | -1.68% | 118.22 | 116.75 | -1.24% | +0.44pp |
| q7 | 13.36 | 22.01 | +64.75% | 14.43 | 24.25 | +68.05% | +3.31pp |
| q12 | 71.33 | 65.08 | -8.76% | 81.01 | 73.63 | -9.11% | -0.35pp |
| q13 | 127.13 | 125.61 | -1.20% | 99.70 | 103.02 | +3.33% | +4.53pp |
| q15 | 5.59 | 26.10 | +366.91% | 10.74 | 36.15 | +236.59% | -130.31pp |
| q16 | 2.81 | 4.74 | +68.68% | 4.44 | 7.18 | +61.71% | -6.97pp |
| q17 | 37.25 | 92.48 | +148.27% | 49.58 | 90.99 | +83.52% | -64.75pp |

## 分组算术平均

| 分组 | x86 | Kunpeng | Kunpeng-x86 |
|---|---:|---:|---:|
| 大状态8Q | +36.08% | +33.05% | -3.03pp |
| 小状态7Q | +91.00% | +63.26% | -27.73pp |
| ValueState11Q | +80.26% | +60.21% | -20.05pp |
| 15Q总计 | +61.71% | +47.15% | -14.56pp |

## 结论与下一步

当前差值不是均匀分布：q15 与 q17 合计贡献约 -13.00pp，占 15Q 总差距约 89%。q15 的同协议 profile 显示 Kunpeng wall 加速比 x86 更高，但 FullOpt cores 放大更严重；q17 则是 Kunpeng RocksDB 基线更强，而两平台 FullOpt 绝对吞吐接近。因而仅继续优化 RocksDB 负查或人为削弱 x86 都不能达标。

ARM native request plane、flat-slice preagg 和 4B hash-token preagg 已逐步修复 O(n²)、重复容器分配和完整 key 序列化，但最新 q15 hash-token 相对 ARM retained 仍为 -0.06%。下一步只保留能减少 q15/q17 Java fold、对象和写侧 CPU 的方案；未转正前不扩 15Q，也不把它计入阶段收益。

证据副本：`dse_results/cachekit-crosshost-fullopt-200m-r1-20260812/`；详细 profile：`Q15_Q17_CROSSHOST_PROFILE_REPORT_ZH.md`。
