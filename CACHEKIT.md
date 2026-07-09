# CacheKit

Flink 1.16.3 state-backend fork with a RocksDB-backed **CacheKit** layer:

- **D1 object-resident ValueState** (VoidNamespace-gated hot accumulator cache)
- **Truly-async backpressure prefetch** (off-mailbox RocksDB MultiGet → staging → L1)
- **Mailbox-batch lookahead** (prefetch / local-preagg plumbing)
- **Runtime local pre-aggregation** (same-key fold for GroupAgg + RowTimeDeduplicate)

Base: [`CacheKit-ValueState-ObjResident`](http://10.101.160.224:17878/micuks/CacheKit-ValueState-ObjResident) @ `cf18552ab6`  
Feature stack: bp-prefetch adaptive (this repo) rebased onto that HEAD.

## Results (final, quiet host)

| Metric | Value |
|---|---|
| 15q throughput/core mean vs pure RocksDB (50M × 3) | **+32.43%** |
| Excluding q12 (proc-time) | **+34.72%** |
| CDC correctness | **14/14** deterministic queries PASS |
| Prefetch-only isolation delta (cache held constant) | ~**+1.5%** (small positive) |

Honest attribution: most of the gain is **ValueState cache + local-preagg work-reduction**, not prefetch itself. Prefetch is real, zero cost when not firing, and contributes a small consistent lift.

Full write-up: [`docs/BP-PREFETCH-FINAL-REPORT-ZH.md`](docs/BP-PREFETCH-FINAL-REPORT-ZH.md)  
D1 migration notes: [`D1-MIGRATION.md`](D1-MIGRATION.md)

## Quick start

1. Build the CacheKit artifacts (dist + table-runtime + cachekit jars) with the scripts under `scripts/`.
2. Drop the jars into `$FLINK_HOME/lib/` (or your compose mount).
3. Use the recommended config:

```bash
cp conf/flink-conf-cachekit-example.yaml $FLINK_HOME/conf/flink-conf.yaml
```

Key switches in that example:

```yaml
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory
state.backend.cachekit.cache.void-namespace-only: true
state.backend.cachekit.map.cache.max-entries: 0          # MapState caches OFF
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: 64
state.backend.cachekit.bp-prefetch.backpressure-gated: true
state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: 4096
state.backend.cachekit.local-preagg.enabled: true
```

## Branches

| Branch | Role |
|---|---|
| `main` | Stable integration of D1 + bp-prefetch final stack |
| `dev` | Same tip as `main` (day-to-day development head) |

## Benchmark harness

```bash
# Perf (interleaved RocksDB vs CacheKit), final config uses NO_MAP_CACHE=1
NO_MAP_CACHE=1 ./scripts/run_cachekit_bp_prefetch_perf.sh

# CDC correctness
NO_MAP_CACHE=1 ./scripts/run_cachekit_bp_prefetch_cdc.sh
```

## Module layout

- `flink-state-backends/flink-statebackend-cachekit/` — CacheKit state backend
- `flink-streaming-java/` — `StreamRecordBatchOutput`, `StatePrefetcher`, `LocalPreagg`, mailbox hooks
- `flink-table/` — `BatchableKeyedFunction` folds (GroupAgg, RowTimeDeduplicate)
- `conf/flink-conf-cachekit-example.yaml` — recommended production-ish config
- `scripts/` — build / perf / CDC runners
