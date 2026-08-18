# Native hot-path corrections: audited 200M retest

## Verdict

The corrected native implementation passed its correctness, topology, checkpoint, metric, and
runtime-dispatch gates on both hosts. It did not pass the 15-query performance gate.

| Host | Treatment vs Java FullOpt+mailbox | Before fixes | After fixes |
|---|---|---:|---:|
| x86 | retained native trio | -2.08% | -4.12% |
| x86 | retained trio + Native LocalPreAgg | -7.43% | -1.38% |
| Kunpeng | retained native trio | -2.62% | -2.79% |
| Kunpeng | retained trio + Native LocalPreAgg | -0.19% | -5.51% |

The values are arithmetic means of per-query K/s/core uplift. The implementation remains an
experimental, fail-closed plug-in. The correctness fixes should be retained, but the native
treatments must not be enabled by default.

## Protocol and provenance

- Source: `be83190c680b598bfeb10ad33331a0aa1c01552e`.
- One round; 5M warmup plus 200M measured events per leg.
- 15 queries: q4, q5, q8, q9, q11, q18, q19, q20, q3, q7, q12, q13, q15, q16, q17.
- Three adjacent legs per query: Java FullOpt+mailbox control, retained native trio, and retained
  trio plus Native LocalPreAgg.
- Two TaskManager containers, four TaskManager JVMs per container, 8 TMs, 16 slots.
- Checkpoint scheduling disabled: `execution.checkpointing.interval` is absent. A checkpoint path
  and `EXACTLY_ONCE` mode alone do not schedule checkpoints; all 90 accepted legs have zero REST
  checkpoint counts.
- `value.bypass.enabled=false`; Chen COW/RYW/PQ remain disabled.
- Control keeps Java mailbox, prefetch, local preaggregation, ValueState cache, MapSnapshot, and
  SST/Mem Bloom enabled. Native request-plane switches are off.
- Retained enables native request-plane, MapSnapshot and mailbox-batch.
- Retained+preagg additionally enables Native LocalPreAgg.
- x86 runtime selected the SSE4.2 CRC32C path; Kunpeng used runtime ARM dispatch.
- Each host has 45/45 `VALIDATION.json`, `CHECKPOINT_DISABLED.json`, `TOPOLOGY_2X4.json`,
  `NATIVE_RUNTIME_AUDIT.json`, and `LEG.sha256` records.

Experiment directories:

- x86: `/mnt/data2/wuql/flink-cluster/experiments/cachekit-native-hotpath-retest-200m-x86-20260818`
- Kunpeng: `/home/wuql/flink-cluster/experiments/cachekit-native-hotpath-retest-200m-kunpeng-20260818`
- Kunpeng compact local copy:
  `/mnt/data2/wuql/flink-cluster/OmniStateStore/dse_results/cachekit-native-hotpath-retest-200m-20260818/kunpeng`

Runtime hashes:

| Artifact | x86 SHA-256 | Kunpeng SHA-256 |
|---|---|---|
| CacheKit JAR | `fccb3db31f8042012fe456a57c1cec808510079de970cf5e8fbc348aa43ef971` | same |
| Native JNI library | `3c357999c3ab4890ee15d5c8dd4150c0b39e16642ee055681247a8c7c2ce15be` | `ccea6547871d67fef9a6886c1fb5ee4a71536555d2949b8a791a681d23fdd2bb` |
| Flink dist | `88f13727ffd2fd3d7c8eb5281802298ee1941e3e89abf79ea22696a0fa56023d` | `be654dc8fb15fdaa34e24ae17ab23bc80a3ff44838c7530d2beb54dbd5fa3c01` |

## x86 per-query results

All throughput values are K/s/core. Percentages compare the treatment in the same row with its
adjacent Java FullOpt+mailbox control.

| Query | Control | Retained | Uplift | Retained+PreAgg | Uplift |
|---|---:|---:|---:|---:|---:|
| q4 | 9.68 | 11.28 | +16.53% | 16.63 | +71.80% |
| q5 | 29.22 | 29.03 | -0.65% | 28.96 | -0.89% |
| q8 | 63.67 | 62.20 | -2.31% | 61.83 | -2.89% |
| q9 | 10.25 | 9.99 | -2.54% | 10.01 | -2.34% |
| q11 | 14.75 | 13.67 | -7.32% | 13.52 | -8.34% |
| q18 | 42.07 | 41.93 | -0.33% | 41.47 | -1.43% |
| q19 | 24.89 | 23.14 | -7.03% | 23.23 | -6.67% |
| q20 | 16.09 | 15.10 | -6.15% | 15.01 | -6.71% |
| q3 | 80.05 | 79.33 | -0.90% | 78.98 | -1.34% |
| q7 | 18.36 | 15.92 | -13.29% | 15.86 | -13.62% |
| q12 | 49.89 | 48.46 | -2.87% | 49.35 | -1.08% |
| q13 | 74.25 | 75.14 | +1.20% | 74.18 | -0.09% |
| q15 | 21.17 | 20.46 | -3.35% | 19.88 | -6.09% |
| q16 | 5.10 | 3.40 | -33.33% | 3.16 | -38.04% |
| q17 | 64.72 | 65.10 | +0.59% | 62.84 | -2.90% |

| Group | Retained uplift | Retained+PreAgg uplift |
|---|---:|---:|
| 大状态 8Q | -1.23% | +5.32% |
| 小状态 7Q | -7.42% | -9.02% |
| ValueState 11Q | -4.44% | -0.53% |
| 任意 State 14Q | -4.50% | -1.47% |
| 总计 15Q | **-4.12%** | **-1.38%** |

## Kunpeng per-query results

| Query | Control | Retained | Uplift | Retained+PreAgg | Uplift |
|---|---:|---:|---:|---:|---:|
| q4 | 25.52 | 25.97 | +1.76% | 20.68 | -18.97% |
| q5 | 46.67 | 43.94 | -5.85% | 44.31 | -5.06% |
| q8 | 96.80 | 95.01 | -1.85% | 97.41 | +0.63% |
| q9 | 13.77 | 12.88 | -6.46% | 12.92 | -6.17% |
| q11 | 29.40 | 25.38 | -13.67% | 25.41 | -13.57% |
| q18 | 53.96 | 54.53 | +1.06% | 53.61 | -0.65% |
| q19 | 31.04 | 28.71 | -7.51% | 28.85 | -7.06% |
| q20 | 20.35 | 19.19 | -5.70% | 19.25 | -5.41% |
| q3 | 113.80 | 116.55 | +2.42% | 111.91 | -1.66% |
| q7 | 24.02 | 20.22 | -15.82% | 19.81 | -17.53% |
| q12 | 74.89 | 76.96 | +2.76% | 76.83 | +2.59% |
| q13 | 102.08 | 102.71 | +0.62% | 103.66 | +1.55% |
| q15 | 35.34 | 35.14 | -0.57% | 33.23 | -5.97% |
| q16 | 7.21 | 7.68 | +6.52% | 6.86 | -4.85% |
| q17 | 88.79 | 89.22 | +0.48% | 88.28 | -0.57% |

| Group | Retained uplift | Retained+PreAgg uplift |
|---|---:|---:|
| 大状态 8Q | -4.78% | -7.03% |
| 小状态 7Q | -0.51% | -3.78% |
| ValueState 11Q | -2.88% | -6.37% |
| 任意 State 14Q | -3.03% | -6.02% |
| 总计 15Q | **-2.79%** | **-5.51%** |

## Interpretation

The changes removed the quadratic grouping path, ambiguous key encoding, coarse snapshot
invalidation, and missing x86 SIMD dispatch. Those are real correctness and engineering
improvements. They do not remove the dominant end-to-end costs:

1. Native grouping still pays JNI orchestration and direct-buffer coordination on every eligible
   batch, while many queries do not expose enough duplicate-key work to amortize the boundary.
2. q7, q11, q19, and q20 regress on both hosts, which points to workload-shape overhead rather than
   an ARM-only kernel issue.
3. Native LocalPreAgg is not consistently beneficial. It improves the volatile x86 q4 leg but
   worsens most state-heavy queries and the Kunpeng 15Q mean.
4. q4 has large wall-throughput and core-count movement between adjacent legs on both hosts. The
   rows pass the formal measurement gates, but a single q4 value should not be used as the primary
   method claim without repeated paired runs.

The next performance iteration should be selective activation based on observed batch size,
duplicate ratio, and serialized-byte volume, with a Java fast path below the amortization threshold.
It should not add more always-on native components.
