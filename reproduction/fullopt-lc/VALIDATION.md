# Integration validation — 2026-09-08

The 43/43 byte-identical result below describes integration commit `0e8c22c175`.
The subsequent CacheKit option rename changes two UTF8 constants in the outer
BinaryStringData class only. The rebuild verifier checks precisely that migration
and requires the other 42 target classes and all non-target entries to remain
unchanged. Historical performance results were not rerun.

Namespace migration validation: seven string-copy tests passed for new JVM ON,
new environment ON, JVM OFF overriding environment ON, and old JVM option ON
being ignored (LC remains OFF). The 116 row/serializer tests passed in each new
JVM ON/OFF mode. Five Python helper tests passed, including archived environment
migration, source identity update, rejection and non-overlay preservation.
The rebuild verifier accepted exactly the two UTF8 constant changes, with all
other compiled target bytes and non-overlay entries unchanged. No performance run.

No performance experiment, Docker launch, foreign-job modification or baseline
rerun was performed. The +65.48% figure remains the historical measurement.

## Source integration

Based on `cachekit/dev@335ab826b3353519d3257d43cbe6b49408281a28` in a new isolated
`wuql/cachekit-dev-lc-reproduce-20260908` worktree. Imported only five target
source families and their tests from `7b95de31e5`, plus compile prerequisites
`PrefetchExecutor`, `MapSnapshotCacheMetrics`, `NativeMapSnapshotOptions` and the
executor tests. No wholesale experiment branch merge.

Exact source hashes and compiled class hashes are in `lock.json`. The five-family
compiled outputs match **43/43 historical class hashes**:

| Group | Classes | Mismatches |
|---|---:|---:|
| CacheKit MapState/ValueState | 37 | 0 |
| RocksDB ValueState/batch interface | 2 | 0 |
| BinaryStringData | 4 | 0 |

Rebuilt JAR verification passed for all entries, including non-target resources.
ZIP archive hashes differ from the reference because of archive serialization;
this is not a class/resource difference. `rebuild.py` fails if any entry differs.

## Tests executed

| Suite | Gate modes | Tests per mode | Result |
|---|---|---:|---|
| CachedInternalMapStateTest + CachedInternalValueStateTest + PrefetchExecutorTest | P29 true / false | 131 | PASS both |
| BinaryStringDataLazyCopyTest | P30 true / false | 7 | PASS both |
| BinaryStringDataTest + StringDataSerializerTest + LazyStringRowCopyTest | P30 true / false | 116 | PASS both |
| RocksDBValueWriteBatchTest | supported synchronous write | 1 | PASS (Maven unit/integration executions) |
| Python reproduction helpers | ZIP equality/rejection; isolated prepare/repeat rejection | 4 | PASS |

Commands use `./mvnw -pl MODULE -DskipITs -Dcheckstyle.skip -Drat.skip
-Dspotless.check.skip=true -Dtest=SUITES -DGATE=MODE test`.
The RocksDB interface was installed first with `install` so dependent module
compilation resolves the new method. Java: OpenJDK 11.0.23; Maven wrapper: 3.2.5.
Formatting/license checks are not claimed as run: those Maven checks were skipped.
`git diff --check` and Python syntax checks passed.

The prepare-only tool was tested with an offline fixture that verifies remounts,
checksum regeneration, refusal to reuse an output, preservation of the reference,
and exclusion of historical results. The generated full archived campaign was
not launched or end-to-end performance tested.

## Interpretation

This verifies missing implementation is now present and its selected compiled
runtime classes equal those from the measured experiment. It does not verify
future throughput, a newly built whole-tree distribution, checkpoint/CDC safety
beyond these tests, or clean-machine bootstrap. All other runtime dependencies
must remain pinned according to the archived campaign and documented recipe.
