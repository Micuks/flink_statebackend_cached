# CacheKit FullOpt + SST/memtable Bloom: Kunpeng Nexmark 15q three-round report

## Verdict

With the FullOpt stack held constant, enabling SST and whole-key memtable Bloom
improves the arithmetic mean across 15 queries by **+11.08%** over three rounds.
This is the clean isolated Bloom increment on top of FullOpt.

## Configuration

| Field | FullOpt control | FullOpt + Bloom candidate |
|---|---|---|
| State backend | CacheKit | CacheKit |
| Value cache | 8,000 LRU | same |
| Map snapshot | 2,000; point/presence map cache off | same |
| Prefetch | on; distance 64; async on | same |
| MultiGet | on; chunk/min-batch 64 | same |
| Local preagg / mailbox | on / on (4096) | same |
| Chen COW/RYW/PQ | on | same |
| SST Bloom | off | on; 10 bits/key; full filter |
| Memtable Bloom | ratio 0.0; whole-key off | ratio 0.1; whole-key on |
| Checkpoint interval | absent | absent |
| Workload | 5M warmup + 100M measured; 8 TMs / 16 slots | same |

The audited config diff contains exactly three keys:
`use-bloom-filter`, `memtable-bloom.ratio`, and
`memtable-bloom.whole-key`.

## Raw three-round data

| Query | Ctrl R1 | Ctrl R2 | Ctrl R3 | Bloom R1 | Bloom R2 | Bloom R3 | Mean uplift |
|---|---:|---:|---:|---:|---:|---:|---:|
| q4 | 52.13 | 51.81 | 51.89 | 61.13 | 61.14 | 61.55 | +17.96% |
| q5 | 79.61 | 80.61 | 77.58 | 89.97 | 91.72 | 91.09 | +14.74% |
| q8 | 210.02 | 217.63 | 215.78 | 213.57 | 212.76 | 207.78 | -1.42% |
| q9 | 26.65 | 26.41 | 26.59 | 30.07 | 29.83 | 29.92 | +12.77% |
| q11 | 54.00 | 53.73 | 53.71 | 54.14 | 54.10 | 54.42 | +0.76% |
| q18 | 94.35 | 94.58 | 93.88 | 120.61 | 121.74 | 121.45 | +28.64% |
| q19 | 70.20 | 69.54 | 70.41 | 67.67 | 68.05 | 68.07 | -3.02% |
| q20 | 36.82 | 36.87 | 36.76 | 45.34 | 44.98 | 45.01 | +22.53% |
| q3 | 291.38 | 293.18 | 287.72 | 275.10 | 296.86 | 293.99 | -0.72% |
| q7 | 34.33 | 34.33 | 34.37 | 50.74 | 51.17 | 50.57 | +48.00% |
| q12 | 158.41 | 163.86 | 160.94 | 160.28 | 163.81 | 162.87 | +0.78% |
| q13 | 273.83 | 264.33 | 261.24 | 272.60 | 271.42 | 271.21 | +2.02% |
| q15 | 71.24 | 71.94 | 71.03 | 71.18 | 71.47 | 71.38 | -0.08% |
| q16 | 13.73 | 13.59 | 13.68 | 16.07 | 16.08 | 16.00 | +17.44% |
| q17 | 194.93 | 193.34 | 183.02 | 203.48 | 197.86 | 202.55 | +5.80% |

## Group results

| Group | Control raw mean | Candidate raw mean | Arithmetic-mean uplift |
|---|---:|---:|---:|
| Front eight | 78.40 | 85.25 | +11.62% |
| Back seven | 146.88 | 151.75 | +10.46% |
| ValueState-only | 90.11 | 97.35 | +13.22% |
| Any-state | 99.20 | 105.18 | +11.73% |
| Total 15 | 110.36 | 116.28 | +11.08% |

## Audit

All 90/90 legs and 180 warmup/measured jobs are present, hash-valid, real
FINISHED, positive-core, 8-TM, and checkpoint-count zero. R1/R3 use
control→candidate and R2 reverses order. The source identities frozen by the
manifest are Flink `2f9582a24f98e1d7479b390206eca337c032d207` and FrocksDB
`f8933473ef1eb22d070a199bfecbec55028491d7`.

Authoritative local evidence:
`dse_results/cachekit-kunpeng-fullopt-bloom-matched-r123-20260804/`.

