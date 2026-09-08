# FullOpt + P29 + P30, historical controls — 2026-09-08

User scope: run only FullOpt+P29+P30 on Kunpeng and 114 x86, 100M, no periodic checkpoints. Reuse prior FullOpt and RocksDB; no baseline reruns or tuning sweep. MultiGet minimum is not a hard invariant; retain the reused FullOpt configuration (64 in these particular controls). Wait if wutb has an experiment on Kunpeng, even on another NUMA node.

## Final outcome — 14:15 +08

Both platforms finished 15/15 strict-valid new legs (30/30 total). Against reused FullOpt, the equally weighted query uplift is **x86 +13.26% / Kunpeng +15.60%**. P30 is observed in all 30 legs; P29 batch keys are zero in all 30, so this does not establish P29 benefit. Against historical RocksDB, the combination is **x86 -11.34% / Kunpeng +65.48%**; do not omit the x86 negative comparison or present this as a contemporaneous causal ablation.

- Final tables: [x86](x86-RESULT.md), [Kunpeng](kunpeng-RESULT.md), each with all 15 queries, large8/small7/full15 groups, raw K/s, cores, per-core values and activation/memory/GC evidence.
- Both final reports sent to Feishu as three root-level native tables per platform; code 0 acknowledgments, not rendered visual readback.
- x86 finished 13:52:51, SSH 30879 exit 0. One NUMA-conflicted q19 attempt remains archived and excluded; successful retry is included. Kunpeng finished 12:53:11, SSH 27957 exit 0.
- The final foreground sleep 85215 / exec cell 1192 finished normally. No benchmark or loop remains active for this task.
- Source tests: 501 executions passed. Reporting/ZIP regression suite: 3 tests passed; Python compile and diff checks passed.
- Source pin remains `7b95de31e51a6ee6fec4e4b1f625dc6538960d2c`; any later harness/report-only branch commit does not change the tested runtime identity.

## Source publication

- Branch: `wuql/p29-p30-fullopt-100m-20260908`, remote `Micuks/flink_statebackend_cached`.
- Commits: `702aac9e7a` MapState correctness/experimental gates, `b530e9fc40` ValueState gates and synchronous eviction batch, `7b95de31e5` immutable-Java-string lazy copy.
- Remote branch HEAD `7b95de31e51a6ee6fec4e4b1f625dc6538960d2c` verified by SSH `ls-remote`. HTTPS lookup failed with GnuTLS; SSH succeeded. No force push, main changes, or Co-Authored-By trailer.
- Fresh source tests passed: P29 128 distinct / 255 executions, P30 123 distinct / 246 executions. JVM CDS warnings did not fail tests. Source files match the archived tested-class source map and the pushed commit.

## Experiment identity and boundaries

- Roots: `/home/wuql/flink-cluster/experiments/cachekit-fullopt-p29-p30-100m-{x86,kunpeng}-20260908`.
- Projects: `ckp31x860908`, `ckp31kp0908`; ports 12980 / 12981 / 12982 on each host.
- One new variant `p29p30`, one 15-query round; 8 TM JVMs / 16 slots. Default large8 then small7 order.
- FullOpt controls: `/home/wuql/flink-cluster/experiments/cachekit-native-stage23-fullopt-100m-r1-{x86,kunpeng}-20260907`, Java R1 only. Config bytes and NUMA0 CPU/memory binding unchanged in new legs. Both sets of 15 raw FullOpt hashes revalidated.
- RocksDB controls: `cachekit-native-stage13-fullopt-15q-100m-{x86,kunpeng}-r1-20260902`, RDB R1 only. All 15 raw hashes, actual driver event counts, completion and CPU ownership revalidated; raw K/s and cores retained, not just inherited normalized numbers.
- These are **non-contemporaneous historical comparisons**, not fresh causal P29/P30 ablations. RocksDB NUMA/runtime differences remain explicitly bounded as secondary reference. New classes include current committed dormant experimental code, with every unrelated experimental environment gate explicitly OFF.
- Overlay preserves all non-target entries in the archived FullOpt runtime. Current CacheKit state classes, RocksDB ValueState/interface, and BinaryStringData are overlaid; native libraries and other engine/Flink classes remain from the reference. `identity.json`, `FULLopt_CONFIG_AUDIT.json`, `RUNTIME_VALIDATION.json`, payload map and `SOURCE_DELTA.patch` record exact provenance.
- P29 batch ON, P30 copy ON; P25 access-guided and other exploratory gates OFF. All native and Chen stages OFF. P30 attribution is Flink copy, not CacheKit state-access gain.
- CPU remains the existing receiver rule: two disjoint recursive container PID1 trees, eight sender identities for coverage. Both hosts passed new 35s steady-q4 tree/cgroup checks: all four tree discrepancies below 0.01%, cgroup capacities below 8.05 each. Not eight physically disjoint TM trees.

## Latest monitoring — 13:45 +08

- x86 13/15 strict-valid, q16 running since 13:38:18; foreground SSH 30879 alive. All completed queries effective through P30, batch keys zero. Current 13-query mean versus FullOpt +13.1409450913%; not full15. Refreshed partial table sent successfully (code 0).
- Kunpeng remains complete, no new experiments started there. Sleep 16251 / exec cell 1189 completed normally.

## Earlier monitoring — 13:14 +08

- Kunpeng finished 15/15 at 12:53:11, SSH 27957 exit 0. Re-exported strict raw audit: complete=true, effective 15/15, mean versus FullOpt +15.5993559936%. P29 batch keys zero for every query; P30 positive throughout. Final `kunpeng-RESULT.md` delivered as three native tables, code 0 (receipt, not visual readback).
- x86 reference NUMA0 is idle again: no running Docker containers, sampled CPUs at most 1.01%. Resumed the same runner at 13:13:44, foreground SSH session 30879; it skipped the six completed legs and restarted q19. Failed conflict leg was preserved under `results/failed/` by the runner. No configuration or NUMA changes.
- Foreground sleep 23606 / exec cell 1182 finished normally. New reporting tests cover requested native table order, partial placeholders, common-set group averages and negative values.

## Earlier monitoring — 12:42 +08

- x86 session 99622 exited 79 after 6 strict-valid queries. q19 rejected for NUMA conflict at 12:31:56–12:32:01; foreign project `ckx86wk1off0907` started on overlapping CPUs 0–15. Its containers remain untouched. The owned campaign cleaned itself up. Wait for reference NUMA0 to become idle before resuming; do not mix NUMA identities within this comparison.
- Kunpeng session 27957 remains running: 12 strict-valid queries, now q15. Effective mean versus reused FullOpt is +15.84% for these 12 only; x86 +10.72% for its 6 only. Neither is final full15.
- Previous 30-minute foreground sleep 97023 / exec cell 1176 completed normally. Re-exported both strict raw audits and fetched their JSON snapshots. q19 conflict remains pending and excluded.

## Earlier launch handoff — 12:09 +08

- x86 foreground SSH session 99622: started 12:04:16, q4 strict valid 36.15 K/s/core at 12:08:26, now q5. Kunpeng session 27957: started 12:04:19, q4 strict valid 55.50 at 12:07:18, now q5.
- Both q4 copy lower bounds 549453824; P29 batch keys 0. Do not infer batch activation from lazy-copy counters. Both variants effective via observed P30.
- CPU sessions 24407 / 90641 complete exit 0. Baseline audit sessions 20277 / 69399 complete exit 0. No profiler attached. No foreground sleep active at this handoff.
- Kunpeng continuously checks foreign Docker experiments and wutb experiment processes; do not restart while occupied. At launch no foreign jobs; target x86 CPUs at most 0.50%, Kunpeng target CPUs all 0.00%.
- Initial staging on both hosts failed before launching: ZipInfo object mutation invalidated still-open source offsets during byte-identical validation. Preserved roots with `-stage-failed-1202` suffix. Fixed via `zip_overlay.copy_entry(copy.copy(info))`; minimal regression test passes. Complete restaging and all non-overlay comparisons passed. No runtime correctness gate relaxed.
- Temporary created-only image extraction container `ckp31-table-extract-20260908` removed after copying table base. Its SHA matched the tested base; no existing workload was removed.

## Reporting and continuation

1. Poll the two foreground experiment sessions; diagnose failures before recovery.
2. Run `export.py <remote-root>`, fetch `final/RESULTS.json` into `collected/<platform>/`.
3. Run `report.py collected/<platform>/RESULTS.json --output <platform>-RESULT.md --send` for native Feishu tables. Only primary configured webhook entrypoint is used; never publish credentials.
4. Table order is explicitly user-requested: RocksDB | FullOpt | FullOpt+P29+P30 | FullOpt vs RocksDB | combination vs RocksDB | combination vs FullOpt. Include large8/small7/15Q; query percentage arithmetic means, all negative rows retained. Partial groups use common available-query sets.
5. Continue foreground 30-minute loop until both full15 complete; do not claim final from partial or reuse prior hot2 campaign mean.
6. Commit/push the scoped experiment harness and final concise reports on the same new branch; exclude local-artifacts, collected raw logs, old unrelated untracked research files, build output and credentials. The monitoring steps above are retained as reproduction guidance; this campaign is complete.

No new goal tool was created. The earlier +14.81% hot2 goal is already complete and separate from this requested FullOpt comparison.
