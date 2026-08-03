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
state.backend.rocksdb.use-bloom-filter: true
state.backend.rocksdb.bloom-filter.bits-per-key: 10.0
state.backend.rocksdb.bloom-filter.block-based-mode: false
state.backend.cachekit.rocksdb.memtable-bloom.ratio: 0.1
state.backend.cachekit.rocksdb.memtable-bloom.whole-key: true
```

### Bloom miss-rejection profile

The SST Bloom filter remains a RocksDB feature, so its three settings keep the
standard `state.backend.rocksdb.*` names. CacheKit's Memtable Bloom integration
is configured explicitly under `state.backend.cachekit.rocksdb.*`:

| Layer | Option | Default | Recommended |
|---|---|---:|---:|
| SST | `state.backend.rocksdb.use-bloom-filter` | `false` | `true` |
| SST | `state.backend.rocksdb.bloom-filter.bits-per-key` | `10.0` | `10.0` |
| SST | `state.backend.rocksdb.bloom-filter.block-based-mode` | `false` | `false` |
| Memtable | `state.backend.cachekit.rocksdb.memtable-bloom.ratio` | `0.0` | `0.1` |
| Memtable | `state.backend.cachekit.rocksdb.memtable-bloom.whole-key` | `false` | `true` |

The old `state.backend.rocksdb.memtable-bloom.*` names remain accepted as
deprecated aliases. Use the CacheKit-prefixed names in new configurations. If
both forms are set to different values, startup fails instead of silently
choosing one.

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
