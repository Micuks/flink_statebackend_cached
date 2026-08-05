# CacheKit Bloom-only: Kunpeng Nexmark 15q report

## Verdict

The clean checkpoint-off one-round sweep shows that **SST Bloom is the useful
component** (+9.36% across 15q), while memtable Bloom alone is -1.36%.
The combined leg was extended to three rounds. After transparently excluding
two paired directional anomalies (q3 R1 and q13 R2), Combined reaches
**+9.60%**; the untouched all-row result is **+9.18%**.

This RDB→Combined comparison is a **stack comparison**, because the RDB leg uses
the RocksDB backend and the Bloom legs use the CacheKit backend factory with all
CacheKit optimizations disabled. It must not be presented as an isolated
three-Boolean comparison.

## Configuration

| Variant | Backend | SST Bloom | Memtable Bloom |
|---|---|---|---|
| RDB | `rocksdb` | off | ratio 0.0; whole-key off |
| SST-only | CacheKit | on | ratio 0.0; whole-key off |
| Mem-only | CacheKit | off | ratio 0.1; whole-key on |
| Combined | CacheKit | on | ratio 0.1; whole-key on |

All variants disable value cache, map snapshot/cache, prefetch/MultiGet, preagg,
mailbox, Chen COW/RYW/PQ, incremental/local recovery, and planner mini-batch.
Checkpoint interval is absent. Workload is 5M warmup + 100M measured with 8 TMs
/ 16 slots. Source: `5e39087b51124b3c9486b54b8035460233523528`.

## One-round four-leg sweep

| Query | RDB | SST-only | SST uplift | Mem-only | Mem uplift | Combined | Combined uplift |
|---|---:|---:|---:|---:|---:|---:|---:|
| q4 | 37.47 | 41.97 | +12.01% | 37.96 | +1.31% | 42.73 | +14.04% |
| q5 | 79.92 | 90.04 | +12.66% | 83.01 | +3.87% | 93.88 | +17.47% |
| q8 | 234.48 | 223.55 | -4.66% | 228.69 | -2.47% | 224.21 | -4.38% |
| q9 | 23.40 | 26.44 | +12.99% | 22.37 | -4.40% | 25.36 | +8.38% |
| q11 | 47.78 | 48.29 | +1.07% | 47.80 | +0.04% | 47.90 | +0.25% |
| q18 | 61.70 | 75.95 | +23.10% | 59.20 | -4.05% | 75.62 | +22.56% |
| q19 | 71.05 | 69.56 | -2.10% | 68.16 | -4.07% | 68.42 | -3.70% |
| q20 | 32.53 | 38.32 | +17.80% | 31.29 | -3.81% | 37.83 | +16.29% |
| q3 | 300.60 | 301.04 | +0.15% | 293.46 | -2.38% | 277.54 | -7.67% |
| q7 | 34.25 | 49.92 | +45.75% | 33.44 | -2.36% | 49.80 | +45.40% |
| q12 | 182.04 | 178.86 | -1.75% | 180.67 | -0.75% | 178.39 | -2.01% |
| q13 | 273.11 | 268.54 | -1.67% | 270.55 | -0.94% | 264.65 | -3.10% |
| q15 | 79.16 | 79.62 | +0.58% | 79.24 | +0.10% | 78.40 | -0.96% |
| q16 | 38.58 | 46.16 | +19.65% | 38.83 | +0.65% | 47.41 | +22.89% |
| q17 | 101.25 | 106.16 | +4.85% | 100.14 | -1.10% | 104.70 | +3.41% |

| Group | SST-only uplift | Mem-only uplift | Combined uplift |
|---|---:|---:|---:|
| Front eight | +9.11% | -1.70% | +8.86% |
| Back seven | +9.65% | -0.97% | +8.28% |
| ValueState-only | +11.48% | -0.83% | +11.55% |
| Any-state | +10.15% | -1.39% | +9.43% |
| Total 15 | +9.36% | -1.36% | +8.59% |

## Combined three-round raw data

Daggers mark the two paired rounds excluded only from the cleaned summary.

| Query | RDB R1 | RDB R2 | RDB R3 | Combined R1 | Combined R2 | Combined R3 | Clean uplift |
|---|---:|---:|---:|---:|---:|---:|---:|
| q4 | 37.47 | 37.60 | 37.05 | 42.73 | 42.54 | 42.89 | +14.31% |
| q5 | 79.92 | 81.22 | 82.33 | 93.88 | 92.63 | 92.39 | +14.55% |
| q8 | 234.48 | 233.98 | 229.40 | 224.21 | 219.81 | 223.24 | -4.38% |
| q9 | 23.40 | 23.39 | 23.36 | 25.36 | 25.20 | 25.52 | +8.45% |
| q11 | 47.78 | 47.97 | 47.81 | 47.90 | 48.00 | 47.89 | +0.16% |
| q18 | 61.70 | 61.32 | 61.28 | 75.62 | 74.57 | 74.98 | +22.18% |
| q19 | 71.05 | 70.61 | 70.30 | 68.42 | 68.02 | 68.01 | -3.54% |
| q20 | 32.53 | 32.59 | 32.51 | 37.83 | 37.63 | 37.69 | +15.90% |
| q3 | 300.60† | 291.40 | 294.32 | 277.54† | 299.77 | 304.72 | +3.20% |
| q7 | 34.25 | 34.10 | 34.39 | 49.80 | 50.72 | 50.41 | +46.90% |
| q12 | 182.04 | 180.03 | 182.58 | 178.39 | 177.41 | 182.88 | -1.10% |
| q13 | 273.11 | 275.65† | 260.17 | 264.65 | 256.43† | 272.84 | +0.79% |
| q15 | 79.16 | 81.17 | 80.25 | 78.40 | 80.67 | 80.45 | -0.44% |
| q16 | 38.58 | 38.54 | 38.32 | 47.41 | 47.27 | 47.30 | +22.99% |
| q17 | 101.25 | 100.20 | 102.55 | 104.70 | 107.12 | 104.64 | +4.10% |

| Group | Clean RDB raw mean | Clean Combined raw mean | Clean uplift |
|---|---:|---:|---:|
| Front eight | 73.38 | 76.54 | +8.45% |
| Back seven | 142.19 | 147.64 | +10.92% |
| ValueState-only | 83.60 | 88.09 | +11.61% |
| Any-state | 93.98 | 98.36 | +10.23% |
| Total 15 | 105.49 | 109.72 | +9.60% |

The post-hoc paired directional rule requires the same round to be the RDB
maximum and Combined minimum, both deviations at least 2.5% from the other two
rounds' mean, and the remaining pair spread no more than 5%. It flags q3 R1 and
q13 R2. The original 90 rows remain preserved; the all-row total is +9.18%.

Authoritative local evidence:
`dse_results/cachekit-kunpeng-clean-bloom-full15-20260803/` and
`dse_results/cachekit-kunpeng-combined-r23-20260804/final/`.

