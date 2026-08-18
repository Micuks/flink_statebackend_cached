# CacheKit Native request-plane 200M cross-host progress (2026-08-17)

The corrected hot-path retest is documented in
[RETEST_RESULTS_20260818.md](RETEST_RESULTS_20260818.md). It supersedes the first-pass performance
conclusion for commit `be83190c680b598bfeb10ad33331a0aa1c01552e`, while preserving the earlier
campaign below as pre-fix evidence.

## Verdict

This checkpoint records one validated 15-query round on x86 (`idmg-monitor`) and
Kunpeng 920.  The retained native trio did not improve the arithmetic mean of
per-query K/s/core uplifts.  Adding native LocalPreAgg recovered part of the loss
on Kunpeng, but regressed badly on x86.  These results therefore keep all native
items experimental and motivate the O(n^2), identity, ordering, materialization,
and architecture-layout fixes in the next source revision.

The control is **not plain canonical FullOpt** under the reserved Nexmark label:
the rendered configuration has Java mailbox batching enabled.  It is reported
below as `Java FullOpt+mailbox` so that the effective stack is explicit.

## Experiment configuration

| Leg | Effective treatment |
|---|---|
| Control | Java FullOpt stack plus Java mailbox batching; all native feature switches off |
| Retained native trio | Control + native request plane + native MapSnapshot + native mailbox compact |
| Retained + native preagg | Retained native trio + native LocalPreAgg grouping |

Common stack: SST Bloom, memtable whole-key Bloom, ValueState cache (8,000 LRU),
MapSnapshot (2,000), async MultiGet prefetch (`distance=64`, chunk/min-batch 64),
Java mailbox batch (4,096), Java LocalPreAgg, bypass off, and Chen COW/RYW/PQ off.

Workload and topology: 15 Nexmark queries; one adjacent/interleaved round; 5M
warmup plus 200M measured events per leg; checkpoint scheduling disabled; 2 TM
containers x 4 TM JVMs/container = 8 TMs; 2 slots/TM = 16 slots; parallelism 16;
8 GiB process memory.  Both platforms used source commit
`a03aedcc156a709b5a4217d3c2504b5e8cc4ba6f`.  Exact rendered configs, runtime
manifests, hashes, and audit outputs are stored beside this report.

## x86 results

| Query | Java FullOpt+mailbox K/s/core | Retained native trio K/s/core (uplift) | +Native PreAgg K/s/core (uplift vs control) |
|---|---:|---:|---:|
| q4 | 13.34 | 11.59 (-13.12%) | 15.50 (+16.19%) |
| q5 | 27.40 | 27.29 (-0.40%) | 27.07 (-1.20%) |
| q8 | 58.71 | 59.03 (+0.55%) | 59.38 (+1.14%) |
| q9 | 9.42 | 9.64 (+2.34%) | 9.44 (+0.21%) |
| q11 | 13.42 | 13.43 (+0.07%) | 13.45 (+0.22%) |
| q18 | 39.56 | 39.12 (-1.11%) | 31.98 (-19.16%) |
| q19 | 25.08 | 24.58 (-1.99%) | 24.68 (-1.59%) |
| q20 | 16.53 | 15.97 (-3.39%) | 16.12 (-2.48%) |
| q3 | 92.17 | 86.22 (-6.46%) | 87.31 (-5.27%) |
| q7 | 18.96 | 16.95 (-10.60%) | 17.09 (-9.86%) |
| q12 | 51.88 | 51.20 (-1.31%) | 51.32 (-1.08%) |
| q13 | 82.53 | 78.78 (-4.54%) | 81.13 (-1.70%) |
| q15 | 21.14 | 21.16 (+0.09%) | 14.19 (-32.88%) |
| q16 | 3.77 | 4.12 (+9.28%) | 2.56 (-32.10%) |
| q17 | 60.79 | 60.41 (-0.63%) | 47.44 (-21.96%) |

| Group | Coverage | Control mean | Retained mean | Retained uplift | +PreAgg mean | +PreAgg uplift vs control |
|---|---:|---:|---:|---:|---:|---:|
| Large-state | 8/8 | 25.43 | 25.08 | -2.13% | 24.70 | -0.83% |
| Small-state | 7/7 | 47.32 | 45.55 | -2.02% | 43.01 | -14.98% |
| ValueState | 11/11 | 28.94 | 28.54 | -1.35% | 26.31 | -9.13% |
| All 15Q | 15/15 | 35.65 | 34.63 | -2.08% | 33.24 | -7.43% |

## Kunpeng results

| Query | Java FullOpt+mailbox K/s/core | Retained native trio K/s/core (uplift) | +Native PreAgg K/s/core (uplift vs control) |
|---|---:|---:|---:|
| q4 | 17.18 | 14.02 (-18.39%) | 19.65 (+14.38%) |
| q5 | 45.01 | 44.18 (-1.84%) | 44.70 (-0.69%) |
| q8 | 94.98 | 96.37 (+1.46%) | 95.53 (+0.58%) |
| q9 | 13.87 | 13.47 (-2.88%) | 13.47 (-2.88%) |
| q11 | 29.32 | 29.24 (-0.27%) | 28.84 (-1.64%) |
| q18 | 53.73 | 54.38 (+1.21%) | 53.42 (-0.58%) |
| q19 | 30.66 | 30.16 (-1.63%) | 30.57 (-0.29%) |
| q20 | 20.62 | 20.54 (-0.39%) | 20.50 (-0.58%) |
| q3 | 112.25 | 112.66 (+0.37%) | 111.65 (-0.53%) |
| q7 | 24.21 | 21.59 (-10.82%) | 21.79 (-10.00%) |
| q12 | 77.78 | 78.29 (+0.66%) | 77.25 (-0.68%) |
| q13 | 101.53 | 101.96 (+0.42%) | 103.30 (+1.74%) |
| q15 | 35.95 | 34.74 (-3.37%) | 34.42 (-4.26%) |
| q16 | 7.08 | 6.67 (-5.79%) | 7.14 (+0.85%) |
| q17 | 87.80 | 89.58 (+2.03%) | 89.29 (+1.70%) |

| Group | Coverage | Control mean | Retained mean | Retained uplift | +PreAgg mean | +PreAgg uplift vs control |
|---|---:|---:|---:|---:|---:|---:|
| Large-state | 8/8 | 38.17 | 37.80 | -2.84% | 38.34 | +1.04% |
| Small-state | 7/7 | 63.80 | 63.64 | -2.36% | 63.55 | -1.60% |
| ValueState | 11/11 | 44.26 | 43.87 | -3.46% | 44.14 | -0.29% |
| All 15Q | 15/15 | 50.13 | 49.86 | -2.62% | 50.10 | -0.19% |

Group uplifts are arithmetic means of the constituent per-query uplift
percentages; the raw means are context only.  All 90 rows passed the experiment
gates: real job completed, no real job failure, 8-TM metric coverage, positive
cores and throughput, exact 200M event count, formula checks, checkpoint-disabled
audit, runtime/config identity, and per-leg SHA-256 verification.  The x86 host
was not treated as a quiet absolute-throughput reference, so the defensible claim
is the adjacent same-host treatment ratio rather than cross-host raw throughput.
