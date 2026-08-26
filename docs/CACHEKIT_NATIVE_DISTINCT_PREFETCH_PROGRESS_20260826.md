# CacheKit native exact-DISTINCT MapState prefetch progress

## Objective

Move q15/q16's exact `COUNT(DISTINCT ...)` MapState read set from repeated
single-key JNI/RocksDB lookups to one batch-scoped RocksDB `MultiGet`, while
preserving the generated aggregation order and MapState read-your-write
semantics. The performance target remains a valid 15-query arithmetic-mean
uplift of at least 10% over the otherwise identical native FullOpt control.

## Implemented path

1. The generated DISTINCT handler exposes the exact user keys that will be
   consumed for the current outer key.
2. `DistinctBatchStateMapView` collects and de-duplicates those keys before the
   generated aggregation body begins.
3. `GroupAggFunction` starts one scoped prefetch batch and guarantees abort/end
   cleanup on all exits.
4. `CachedInternalMapState` serializes the current outer key, namespace and
   exact user keys once, then calls `RocksDBMapState` through the public internal
   `RocksDBBatchMapReader` contract.
5. `RocksDBMapState` issues one `multiGetAsList`; found and missing results are
   staged explicitly. Subsequent `get` calls consume the staging map, while
   mutations update or invalidate staged entries so no stale result can escape.

The feature is separately gated by:

```yaml
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: false
```

It is default-off and does not alter the existing direct128 native request
plane, mailbox, prefetch, preaggregation, Bloom, ValueState cache, or
MapSnapshot settings.

## Correctness and unit evidence

- Source commit after activation repair: `b322bcd2dfa8fb7a762c074055ca412bcdc90d44`
- Initial implementation commit: `ce7501a4a78062d124c4ed1fc531e75ea6ef2d19`
- Base commit: `0f89be98af6cda0271ef7ea0262d4d3b44bb1335`
- FrocksDB commit: `335ecf7ce5156a3dd8ac5350e90832036065bba7`
- Focused tests: 62 passed, 0 failures, 0 errors, 0 skipped under JDK 11
- Covered behavior: configuration default/plumbing, exact key collection,
  duplicate collapse, found/missing staging, read-your-write mutation handling,
  outer-key/namespace isolation, abort cleanup, planner code generation, and
  no-op fallback when the backend batch reader is unavailable.

Runtime close counters are fail-closed:

```text
attempts batches inputKeys uniqueKeys found missing stagingHits
fallbacks failures batchReaderAvailable
```

Candidate legs require positive batches and staging hits, exact
`found + missing == uniqueKeys` closure, zero failures and an available batch
reader. Control legs require no candidate summary lines.

## Activation failure and repair

The first q15 candidate screen was rejected rather than reported as a result.
Three completed candidate attempts all had an available backend batch reader,
but every mechanism counter remained zero. The control leg was valid at
`29.48 K/s/core`; candidate throughput from the invalid attempts is not used.

Historical runtime logs prove that LocalPreagg itself was active: q15 executed
more than 200,000 `KeyedProcessOperator` batch dispatches, with an observed
collapse ratio around 22x. The fault was the next activation boundary:

- the public treatment key enabled the CacheKit backend reader;
- `PerKeyStateDataViewStore` only created `DistinctBatchStateMapView` when the
  separate legacy `local-preagg.distinct-overlay` key was enabled;
- the audited candidate configuration intentionally differed from control only
  in the public treatment key, so the required view was never constructed and
  generated prefetch calls were no-ops.

Commit `b322bcd2dfa8fb7a762c074055ca412bcdc90d44` makes either public feature key
activate the required batch-scoped exact-DISTINCT view. The treatment therefore
remains a single default-off switch. The repair adds an explicit configuration
test; the two affected view test classes pass 11/11. Together with the planner
and CacheKit tests, the post-repair focused audit is 62/62. A temporary dispatch
probe was removed before performance packaging, so the repaired candidate has
no diagnostic atomic operation on its batch hot path.

## Benchmark protocol

- Platform: Kunpeng aarch64
- Queries: q15 and q16 initial screen
- Events: 50M/query
- Checkpointing: disabled
- Topology: 2 physical TaskManager containers x 4 JVMs/container = 8 TMs,
  16 slots
- Order: `off_a`, `on_a`, `on_b`, `off_b`
- Sole A/B difference: the feature key above
- Throughput: original Nexmark K/s/core with positive measured cores and a
  real completed job

Authoritative remote expdir:

```text
/home/wuql/flink-cluster/experiments/
cachekit-native-map-distinct-prefetch-q15q16-50m-abba-20260826
```

At the time of this update, the audited payload had been staged and hash
verified on the target. Runtime assembly and benchmark launch were deliberately
held while an unrelated experiment payload was actively transferring to the
same host. No performance result is claimed yet.
