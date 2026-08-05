# CacheKit Kunpeng TSV110/LSE: Nexmark 15q three-round report

## Verdict

The Kunpeng-specific native build did **not** produce a stable overall gain.
Keeping every valid leg, the 15-query arithmetic-mean uplift is **-0.16%**.
R1 q16 is a valid but non-replicated low candidate point; excluding q16 only as
a sensitivity analysis gives +0.51% over the other 14 queries, still effectively
flat. No leg is silently removed from the official result.

## Configuration

| Field | Generic control | TSV110/LSE candidate |
|---|---|---|
| Native flags | `-march=armv8-a+crc+crypto -mno-outline-atomics` | `-mcpu=tsv110 -moutline-atomics` |
| Compiler | Kunpeng GCC 10.3.1 | same |
| FrocksDB source | `f8933473ef1eb22d070a199bfecbec55028491d7` | same |
| SST Bloom | on | on |
| Memtable whole-key Bloom | ratio 0.1, on | same |
| Value cache / map snapshot | off / off | same |
| Prefetch / MultiGet / preagg / mailbox | off | same |
| Chen COW/RYW/PQ | off | same |
| Checkpoint interval | absent | absent |
| Workload | 5M warmup + 100M measured, 8 TMs / 16 slots | same |

The configs are byte-identical. Both RocksJava builds passed 210 tests. ELF
audit found 0 LSE instructions in generic and 3,831 in TSV110, proving that the
mechanism entered the candidate binary.

## Raw three-round data

K/s/core values are raw. Uplift is the mean of the three paired per-round
`tsv110/generic - 1` percentages.

| Query | G R1 | T R1 | G R2 | T R2 | G R3 | T R3 | Mean uplift |
|---|---:|---:|---:|---:|---:|---:|---:|
| q4 | 37.84 | 43.53 | 42.71 | 43.26 | 43.03 | 43.35 | +5.69% |
| q5 | 95.22 | 94.83 | 93.99 | 92.52 | 94.83 | 95.71 | -0.35% |
| q8 | 228.11 | 229.02 | 230.22 | 229.81 | 228.31 | 227.33 | -0.07% |
| q9 | 25.07 | 25.36 | 25.07 | 25.08 | 25.30 | 25.22 | +0.29% |
| q11 | 48.14 | 48.62 | 48.26 | 48.14 | 48.39 | 49.01 | +0.68% |
| q18 | 73.93 | 75.03 | 74.66 | 75.05 | 74.07 | 75.29 | +1.22% |
| q19 | 67.93 | 67.84 | 68.27 | 68.53 | 68.50 | 68.13 | -0.10% |
| q20 | 37.70 | 37.94 | 37.74 | 37.93 | 37.63 | 37.89 | +0.61% |
| q3 | 298.13 | 297.01 | 300.76 | 293.93 | 300.71 | 294.70 | -1.55% |
| q7 | 50.48 | 50.26 | 50.55 | 50.77 | 50.76 | 50.90 | +0.09% |
| q12 | 180.74 | 178.23 | 179.59 | 179.56 | 180.83 | 178.95 | -0.82% |
| q13 | 270.29 | 272.94 | 264.89 | 266.36 | 266.48 | 267.62 | +0.65% |
| q15 | 81.72 | 81.10 | 78.66 | 79.18 | 80.82 | 79.57 | -0.55% |
| q16 | 46.74 | 32.36 | 46.86 | 47.81 | 47.16 | 47.19 | -9.56% |
| q17 | 105.10 | 106.19 | 104.96 | 105.99 | 103.63 | 105.74 | +1.35% |

## Group results

| Group | Generic raw mean | TSV110 raw mean | Arithmetic-mean uplift |
|---|---:|---:|---:|
| Front eight | 77.29 | 77.68 | +1.00% |
| Back seven | 147.14 | 146.02 | -1.48% |
| ValueState-only | 88.66 | 88.48 | -0.18% |
| Any-state | 98.65 | 98.19 | -0.22% |
| Total 15 | 109.88 | 109.57 | -0.16% |

## Audit and outlier note

All 90/90 legs are real 100M FINISHED jobs with exactly 8 TMs, positive cores,
zero checkpoint histories/counts, and verified leg/round hashes. R1 q16 TSV110
(32.36) is about 32% below its R2/R3 values, while its generic control is stable.
It remains in the official result because every validity check passed. The 14q
sensitivity result is reported only to show that the compiler route remains flat
even without that point.

Authoritative local evidence:
`dse_results/cachekit-kunpeng-compiler-direct-r1-20260805/final/`.

