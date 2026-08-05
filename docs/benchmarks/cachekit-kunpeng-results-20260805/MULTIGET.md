# CacheKit access-guided MultiGet: Kunpeng Nexmark 15q report

## Verdict

The audited 100M 15-query matrix is valid, but the combined optimized candidate
has an arithmetic-mean uplift of **-0.36%**. It was therefore not promoted to
`cachekit/dev`. This is a single-round screening result, not a three-round claim.

## Configuration

| Field | Base | Candidate |
|---|---|---|
| Source | `cachekit/dev_wutb@1ae90f358f` | `wuql/cachekit/dev@6ab859944d4190ffc49d0be3d734e87c53f7cf64` |
| CacheKit full option stack | on | on |
| bp-prefetch | on, distance 64 | same |
| Backpressure gate | on | off |
| MultiGet | off | on; chunk 8; minimum batch 4 |
| Async chunks | off | on; size 8 |
| Mailbox | on; size 4096 | same |
| Local preagg | on | same, plus exact-key bulk loading |
| Value cache | 8,000 LRU | same |
| Map snapshot | 2,000 | same |
| Chen COW/RYW/PQ | on | same |
| Workload | 100M, one fresh-cluster round, 8 TMs / 16 slots | same |

This is explicitly a **combined optimized candidate**, not an isolated Boolean
MultiGet switch: it also changes negative-result staging, prefetch lead time and
allocation behavior, access-guided wrapper selection, and local-preagg bulk load.

## Raw result

| Query | Base K/s/core | Candidate K/s/core | Uplift |
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

## Group results

| Group | Base raw mean | Candidate raw mean | Arithmetic-mean uplift |
|---|---:|---:|---:|
| Front eight | 60.62 | 60.86 | +0.44% |
| Back seven | 104.17 | 103.62 | -1.29% |
| ValueState-only | 73.44 | 72.88 | -0.76% |
| Any-state | 75.03 | 74.87 | -0.40% |
| Total 15 | 80.94 | 80.81 | -0.36% |

## Audit

All 30/30 rows have `real_job_completed=true`, no real job failure, positive
cores/TPC, and per-query topology proof for exactly 8 TMs / 16 slots. The 50M
q17 canary had selected chunk8, but its positive short-run result did not
reproduce at 100M; this is why the full matrix, not the canary, controls the
verdict.

Authoritative local evidence:
`dse_results/cachekit-multiget-optimized-15q-20260718/`.
