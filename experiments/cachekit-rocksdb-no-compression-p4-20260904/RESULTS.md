# CacheKit RocksDB write-path optimization result

Date: 2026-09-04 (Asia/Singapore)

## Outcome

The P6 global candidate passes the requested gate on Kunpeng: the arithmetic mean of the five paired per-query K/s/core uplift percentages is **+11.95%**. All five queries are included, including the nearly neutral q15; no query was removed after observing its result.

The candidate changes exactly three configuration keys relative to the frozen control:

```yaml
state.backend.rocksdb.compression.type: NO_COMPRESSION
state.backend.rocksdb.memory.fixed-per-slot: 2048m
state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
```

The code default remains `SNAPPY_COMPRESSION`; P6 is an explicit experiment configuration, not a global default change.

## Final Kunpeng effective-5 result

All candidate legs use the same ARM artifact SHA-256 `3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723`, 100M events, 8 TaskManagers, 16 slots, NUMA node 0, even CPUs 38 through 74, and `cpuset.mems=0`. The q9 control is reused from the frozen P4 q9 pair; q5/q11/q15/q18 controls are reused from the frozen P4 effective-4 campaign on the same host and artifact.

| Query | Control K/s/core | P6 K/s/core | P6 raw K/s | P6 cores | Uplift | Captured SST files | Captured SST bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| q5 | 78.39 | 83.99 | 1010.00 | 12.04 | +7.14% | 3 | 1,169,366,440 |
| q9 | 25.54 | 34.60 | 525.91 | 15.20 | +35.47% | 119 | 23,639,085,775 |
| q11 | 46.73 | 46.77 | 725.79 | 15.52 | +0.09% | 64 | 438,888,296 |
| q15 | 53.06 | 53.04 | 682.97 | 12.88 | -0.04% | 0 | 0 |
| q18 | 95.91 | 112.30 | 1720.00 | 15.28 | +17.09% | 32 | 4,822,707,496 |

- Arithmetic mean: **+11.95%**
- Median: +7.14%
- Minimum: -0.04%
- Maximum: +35.47%
- Performance gate, mean >= +10.00%: **PASS**
- Activation gate: **PASS**

The committed machine-readable result is `remote-evidence/kunpeng-p6/final/P6_EFFECTIVE5_RESULT.json`.

## Activation interpretation

Every P6 leg records the exact `NO_COMPRESSION` mode in TaskManager logs and passed measurement integrity. q5, q9, q11, and q18 also contain independently captured live SST inventories.

q15 is deliberately not disguised as an SST-compression win. Its frozen control produced 20 SST files (49,168,385 bytes), while the 2 GiB/slot HIGH_MEM candidate produced no SST during the same 100M-event workload. Its valid result is therefore accepted through the separately recorded control-to-memtable-residency audit, not through an SST-compression claim. q15 remains in the five-query mean and is slightly negative (-0.04%).

SST inventories are ten-second live snapshots plus post-leg inventories, not guaranteed peak disk-usage measurements. `NO_COMPRESSION` materially increases storage demand, especially for q9. Any production adoption must treat memory and storage capacity as part of the configuration contract.

## Optimization path

The CPU-time profile attributed 28.36% of sampled CPU time to RocksDB background flush/compaction work, while the CacheKit worker accounted for only 0.14%. This redirected optimization from speculative asynchronous prefetch toward the RocksDB write path.

1. P4 made SST compression configurable, preserving Snappy as the default. q9 improved by +26.86% on Kunpeng and +27.10% on x86 with `NO_COMPRESSION`, but broader expansion failed: Kunpeng q5/q11/q15/q18 averaged +4.85%, and x86 q5/q11/q18 averaged +7.68%.
2. P5 combined no compression with an 8K snapshot cache, a two-entry small-map threshold, and snapshot-owned-key reuse. It activated strongly on q9 but the full five-query mean was only +8.80%, so it was rejected.
3. P6 screened two LSM configurations on idle x86. HIGH_MEM reached 37.03 K/s/core (+35.99% versus the frozen x86 control), beating the large-LSM variant at 36.58 K/s/core (+34.34%). The HIGH_MEM winner was then confirmed on Kunpeng across all five queries and passed at +11.95% mean.

Cross-host numbers are not pooled: the x86 campaign is selection evidence only because host storage differs. The final claim is the within-Kunpeng effective-5 result.

## Integrity and recovery notes

- P5 initially inherited a cleanup path for `variants/control`; q9 had not been submitted when the stale TaskManager Docker exec stalled. The incomplete pre-start directory is preserved under `results/failed`, the runner hashes and repair are recorded, and q5 was not rerun.
- P6 q15 completed measurement and all mode checks but stopped at the old “SST required” P4 gate. The result was finalized only after a separate audit proved valid measurement, exact mode activation, 2 GiB HIGH_MEM configuration, zero candidate SST, and nonzero frozen-control SST. Runner hashes are preserved in `Q15_NO_SST_GATE_REPAIR.txt`.
- Thirty copied leg manifests were independently rehashed after evidence transfer.
- Both experiment hosts were clean after completion; the final Kunpeng campaign has `CAMPAIGN_COMPLETE`, `HOST_RESULT_COMPLETE`, and no remaining Compose containers.

## Code and verification

- RocksDB compression implementation commit: `a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4`
- P6 x86 screen harness commit: `083d25345f`
- P6 Kunpeng harness commit: `707a502116`
- q15 activation-policy repair commit: `9ed0d4652c`
- Current `RocksDBResourceContainerTest`: 16 tests, 0 failures
- Earlier P4 targeted RocksDB backend suite: 26 tests, 0 failures
- Earlier P3/CacheKit verification: 309 tests, 0 failures

Evidence directories:

- `remote-evidence/kunpeng-p6/`: final passing result and five raw candidate legs
- `remote-evidence/x86-p6-screen/`: independent HIGH_MEM versus large-LSM selection
- `remote-evidence/kunpeng-p5/`: rejected global snapshot-cache candidate
- `remote-evidence/kunpeng-effective4/` and `remote-evidence/x86-effective3/`: rejected P4 expansions
- `remote-evidence/kunpeng/` and `remote-evidence/x86/`: paired P4 q9 controls and no-compression candidates
