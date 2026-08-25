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

- Source commit: `ce7501a4a78062d124c4ed1fc531e75ea6ef2d19`
- Base commit: `0f89be98af6cda0271ef7ea0262d4d3b44bb1335`
- FrocksDB commit: `335ecf7ce5156a3dd8ac5350e90832036065bba7`
- Focused tests: 61 passed, 0 failures, 0 errors, 0 skipped
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
