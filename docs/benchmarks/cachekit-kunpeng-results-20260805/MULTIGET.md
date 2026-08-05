# CacheKit Access-guided MultiGet：鲲鹏 Nexmark 15q 报告

## 结论

审计后的 100M 15-query matrix 全部有效，但 combined optimized candidate 的逐 query
提升算术平均为 **-0.36%**，因此没有晋升到 `cachekit/dev`。这是单轮筛选结果，不是
三轮统计结论。

## 配置

| 配置项 | 基线 | 候选 |
|---|---|---|
| 源码 | `cachekit/dev_wutb@1ae90f358f` | `wuql/cachekit/dev@6ab859944d4190ffc49d0be3d734e87c53f7cf64` |
| CacheKit full option 栈 | 开 | 开 |
| bp-prefetch | 开，distance 64 | 相同 |
| Backpressure gate | 开 | 关 |
| MultiGet | 关 | 开；chunk 8；minimum batch 4 |
| Async chunks | 关 | 开；size 8 |
| Mailbox | 开；size 4096 | 相同 |
| Local preagg | 开 | 相同，并增加 exact-key bulk loading |
| Value cache | 8,000 LRU | 相同 |
| Map snapshot | 2,000 | 相同 |
| Chen COW/RYW/PQ | 开 | 相同 |
| 负载 | 100M，单轮 fresh cluster，8 TMs / 16 slots | 相同 |

这是明确的 **combined optimized candidate**，而不是单独切换 MultiGet Boolean：候选还改变
negative-result staging、prefetch lead time、分配行为、access-guided wrapper 选择和
local-preagg bulk load。

## 原始结果

| Query | 基线 K/s/core | 候选 K/s/core | 提升 |
|---|---:|---:|---:|
| q4 | 24.92 | 24.67 | -1.00% |
| q5 | 59.45 | 59.74 | +0.49% |
| q8 | 168.54 | 166.96 | -0.94% |
| q9 | 14.83 | 14.83 | +0.00% |
| q11 | 66.35 | 68.05 | +2.56% |
| q18 | 79.59 | 81.10 | +1.90% |
| q19 | 50.38 | 50.63 | +0.50% |
| q20 | 20.90 | 20.91 | +0.05% |
| q3 | 171.22 | 174.92 | +2.16% |
| q7 | 27.79 | 26.77 | -3.67% |
| q12 | 128.96 | 127.10 | -1.44% |
| q13 | 163.74 | 164.01 | +0.16% |
| q15 | 76.67 | 76.97 | +0.39% |
| q16 | 13.06 | 12.62 | -3.37% |
| q17 | 147.72 | 142.92 | -3.25% |

## 分组结果

| 分组 | 基线原始均值 | 候选原始均值 | 逐 query 提升算术平均 |
|---|---:|---:|---:|
| 前八 | 60.62 | 60.86 | +0.44% |
| 后七 | 104.17 | 103.62 | -1.29% |
| ValueState-only | 73.44 | 72.88 | -0.76% |
| 任意 state | 75.03 | 74.87 | -0.40% |
| 总计 15q | 80.94 | 80.81 | -0.36% |

## 审计

30/30 行均满足 `real_job_completed=true`、无 real job failure、cores/TPC 为正，并有
逐 query 的 8 TMs / 16 slots topology 证据。50M q17 canary 曾选择 chunk8，但短跑
正收益没有在 100M 复现，因此最终结论以 full matrix 而不是 canary 为准。

权威本地证据：
`dse_results/cachekit-multiget-optimized-15q-20260718/`.
