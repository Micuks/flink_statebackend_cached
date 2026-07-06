# D1 (object-resident hot ValueState) migrated into CacheKit

Source optimization: OmniStateStore `OmniStateStore-ValueState-ObjResident`
(http://10.101.160.224:17878/micuks/OmniStateStore-ValueState-ObjResident).
Target: CacheKit state backend, branch `feat/cachekit-map-snapshot-cache`.

## What D1 is

D1 keeps **live, deserialized accumulators on the JVM heap** for hot keyed
`ValueState` and mutates them by reference, eliminating per-record JNI + (de)serialization
on group-aggregation queries (q15/q16/q17). Dirty values are written back to the delegate
lazily (on eviction / before checkpoint). It is guarded by three correctness rules; the
decisive one is a **VoidNamespace gate**: the object-resident cache is only sound for
non-windowed keyed ValueState. Windowed ValueState (namespace = a window) has purge/merge
semantics the cache does not model, and caching it corrupts windowed output.

## What CacheKit already had

`CachedInternalValueState` is already an object-resident live-value cache: it holds the
deserialized `V` in an L1/L2 policy, marks it dirty on `update()`, and flushes dirty entries
to the delegate in `flush()` before serialization/snapshot. In CacheKit the delegate is plain
RocksDB (there is no Falcon byte-cache), so this decorator IS D1's mechanism — the migration
is not re-implementing the cache, it is bringing across D1's **correctness discipline**.

## The migration (single change)

`CachedInternalValueState` cached **all** namespaces, including windowed state. Ported D1's
gate: `value()` / `update()` / `clear()` bypass the object cache to the delegate whenever
`currentNamespace != VoidNamespace.INSTANCE`. The gate is per-access, so a query mixing
windowed and VoidNamespace value state still caches the VoidNamespace portion. This is a pure
correctness fix — windowed value caching was empirically wrong (see below).

## CDC correctness (nexmark 15q, CacheKit vs RocksDB, terminal-state CDC replay)

Config: `CacheKitStateBackendFactory`, `value.cache.max-entries=8000` (D1 value cache ON),
`map.cache.max-entries=0`. Deterministic single-split 100k source (see note).

| stage | PASS | notes |
|---|---|---|
| baseline (no gate, value+map cache on) | 6/15 | windowed + join all fail |
| + D1 VoidNamespace gate, map cache off | **15/15** | q15/q16/q17 byte-identical output |

Group-agg queries (q13/q15/q16/q17/q18/q19) — D1's target — pass; joins (q3/q4/q9/q20) pass;
windowed queries (q5/q7/q8/q11) pass (value gate bypasses their windowed ValueState).

### Two findings separated from D1 during isolation

1. **Windowed-query non-determinism was a harness artifact, not a cache bug.** The 1M source
   is 4 part files; at parallelism 1 the FileSource assigns splits in a non-deterministic
   order, so watermark progression (and hence late-event dropping under the `-4s` watermark)
   varies run-to-run — plain **rocksdb-vs-rocksdb** diverges on q5/q7/q8/q11. The single-split
   100k source removes this; windowed queries then reproduce and pass.
2. **The map-snapshot-cache has a separate join-correctness bug, outside D1's scope.** With the
   value cache alone (map cache off) joins pass; with the map cache on they fail. This is
   CacheKit's own WIP feature, not the D1 value optimization — left disabled here, to fix
   separately.
