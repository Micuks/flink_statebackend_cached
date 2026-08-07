# CacheKit CDC 正确性证据索引

## 已确认的历史证据

仓库历史中存在已经完成的 15-query adaptive bp-prefetch CDC gate，并非只有实验计划：

- `results-bp-prefetch-cdc-adaptive-15q-combined/STATUS` 内容为 `PASS`。
- 对应 `diff.txt` 记录 15/15 PASS：8 个 query 通过排序输出对比，6 个通过 CDC
  collapse，对 processing-time 的 q12 使用确定性、带 guard gap 的首窗口 oracle。
- 长期汇总已经提交在 `docs/BP-PREFETCH-FINAL-REPORT-ZH.md`；CDC runner 的确定性
  source 支持对应提交 `30d469d2763a377aceb943090c72476b320c78c0`。
- 汇总提交 `6806da57c4380fafb147df6049b8b1c0996ee56c` 是 Bloom 源码
  `5e39087b51124b3c9486b54b8035460233523528`、MultiGet 候选
  `6ab859944d4190ffc49d0be3d734e87c53f7cf64` 以及发布本索引时
  `wuql/cachekit/dev` 的祖先。

此外，`results-e2b-cdc-correctness/diff.md` 记录 E2b capacity 对照在 100k events
下 15/15 PASS。它是有效的历史缓存路径正确性证据，但不是所有后续性能候选的
exact-binary 证明。

## 证据范围

CDC 正确性与性能有效性是两类不同结论。以上证据证明指定的历史 CacheKit/adaptive
prefetch 路径通过了对应 CDC oracle，但不会追溯性地认证所有后续 overlay/native binary：

- MultiGet `6ab859...` 的实验包包含单测、机制和真实任务完成证据；此处不声称它另外跑过
  一次 exact-binary 15q CDC。
- Bloom filter 只允许 false positive，不应改变状态语义；Bloom 性能实验的直接证据是
  真实 Nexmark 完成和配置/hash 审计，而不是一轮新的 exact-binary CDC matrix。
- FullOpt 含多个已测试组件。历史 CDC PASS 支持其祖先和组件安全性；若要形成 release-grade
  的 exact-binary 声明，仍应给最终 FullOpt+Bloom binary 单独跑 CDC matrix。

## 可直接访问的证据

- 随本索引提交的原始副本：
  [`cdc-evidence/CDC_15Q_STATUS.txt`](cdc-evidence/CDC_15Q_STATUS.txt) 和
  [`cdc-evidence/CDC_15Q_DIFF.txt`](cdc-evidence/CDC_15Q_DIFF.txt)。
- `results-bp-prefetch-variants/CDC-15Q-COMBINED-ZH.md`
- `results-bp-prefetch-cdc-adaptive-15q-combined/STATUS`
- `results-bp-prefetch-cdc-adaptive-15q-combined/diff.txt`
- `results-bp-prefetch-cdc-adaptive/diff.txt`
- `results-bp-prefetch-cdc-q12-proctime-guarded-firstwindow-offon-12k-tps1000/diff.txt`
- `results-e2b-cdc-correctness/diff.md`

本索引刻意区分“证据存在且能找到”和“后续特定性能 binary 已重新通过 CDC”。
