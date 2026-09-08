# CacheKit Optimization Definitions

Use this reference whenever a Nexmark configuration or result uses one of the
reserved labels `fullopt`, `FullOpt+LC`, `native fullopt`, `stage1`, `stage2`, or `stage3`.

## Label boundaries

- `fullopt` is the portable Java CacheKit optimization stack. It includes RocksDB
  Bloom, ValueState cache, MapState snapshot cache, prefetch, local preaggregation,
  and mailbox batching. It excludes all native stages and Chen COW/RYW/PQ.
- `stage1`, `stage2`, and `stage3` are isolated native-treatment labels. They are
  not cumulative.
- `FullOpt+LC` is portable FullOpt plus P29 synchronous eviction batching and
  P30 lazy string copy. LC means Lazy Copy; it is not native FullOpt or adaptive
  compression. See [configuration and usage](fullopt-lc.md). Preserve the older
  `FullOpt+P29+P30` campaign labels as provenance, rather than rewriting raw data.
- `native fullopt` is the frozen combined native stack. It enables Stage1, Stage2,
  and Stage3 capabilities, but it is not a literal concatenation of the three
  standalone templates: its Stage1 is ValueState-only, its Stage2 cache is 2000,
  and its Stage3 uses the frozen FullOpt settings below.
- `fullopt+chen` is a distinct variant. Never include Chen COW, RYW, or priority
  queue optimization in plain `fullopt` or `native fullopt`.
- Preserve historical names together with their actual effective configurations.
  Do not retroactively rewrite an old treatment identity to match this contract.

## Launch and audit contract

For a newly launched reserved treatment:

1. Render every key below explicitly; do not rely on defaults.
2. Reject duplicate keys and stop on any missing or mismatched invariant.
3. Save the rendered configuration, its SHA-256, source commit, JAR SHA-256, and,
   for Stage1/Stage3, the request-plane `.so` SHA-256 and architecture.
4. Copy the exact rendered configuration into every raw leg directory and verify
   that same-treatment legs have the expected identical hash.
5. Fully restart the TaskManagers after changing the config, JAR, or native library.
6. Verify activation from current-leg logs. Configured-but-inactive native paths
   must be reported as such.

For a no-checkpoint performance campaign, omit
`execution.checkpointing.interval`. Checkpoint/correctness campaigns are separate
protocols and must say so explicitly.

## Portable `fullopt`

The canonical portable stack uses the CacheKit factory and these exact treatment
settings:

```yaml
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory

state.backend.rocksdb.use-bloom-filter: true
state.backend.rocksdb.bloom-filter.bits-per-key: 10.0
state.backend.rocksdb.bloom-filter.block-based-mode: false
state.backend.rocksdb.memtable-bloom.ratio: 0.1
state.backend.rocksdb.memtable-bloom.whole-key: true

state.backend.cachekit.value.cache.max-entries: 8000
state.backend.cachekit.value.cache.policy: LRU
state.backend.cachekit.value.cache.lru.overflow: 1024
state.backend.cachekit.value.bypass.enabled: false
state.backend.cachekit.value.hit-rate.threshold: 0.03
state.backend.cachekit.value.hit-rate.window: 5000

state.backend.cachekit.map.cache.max-entries: 0
state.backend.cachekit.map.snapshot.cache.max-entries: 2000
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.map.iteration.cache-fill.enabled: false
state.backend.cachekit.map.bypass.enabled: false

state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: 64
state.backend.cachekit.bp-prefetch.backpressure-gated: false
state.backend.cachekit.bp-prefetch.commutative-key-sort: false
state.backend.cachekit.bp-prefetch.async.enabled: true
state.backend.cachekit.bp-prefetch.multiget.enabled: true
state.backend.cachekit.bp-prefetch.multiget.chunk-size: 64
state.backend.cachekit.bp-prefetch.multiget.min-batch-size: 64
state.backend.cachekit.bp-prefetch.async-chunks.enabled: false
state.backend.cachekit.bp-prefetch.async-chunks.size: 64
state.backend.cachekit.bp-prefetch.staging.max-entries: 8192
state.backend.cachekit.value.sticky-update-in-place.enabled: true
state.backend.cachekit.value.lazy-staging.enabled: true
state.backend.cachekit.value.lazy-staging.max-retained-bytes: 67108864
state.backend.cachekit.local-preagg.enabled: true

state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: 4096
state.backend.cachekit.mailbox-batch.timeout-us: 0
state.backend.cachekit.mailbox-batch.commutative-key-sort: false

state.backend.cachekit.native.request-plane.enabled: false
state.backend.cachekit.native.value-cache.enabled: false
state.backend.cachekit.native.map-cache.enabled: false
state.backend.cachekit.map.snapshot.cache.native.enabled: false
state.backend.cachekit.native.mailbox-batch.enabled: false
state.backend.cachekit.native.prefetch.enabled: false
state.backend.cachekit.native.local-preagg.enabled: false
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: false

state.backend.cachekit.list-state.cow: false
state.backend.cachekit.list-state.ryw: false
state.backend.cachekit.list-state.cleared-keys.capacity: 200000
state.backend.cachekit.priority-queue.opt: false
table.exec.mini-batch.enabled: false
table.optimizer.distinct-agg.split.enabled: true
```

Mailbox batching is part of plain `fullopt`; do not call the mailbox-on stack
`fullopt+mailbox` under this contract. `value.bypass.enabled` and
`map.bypass.enabled` are the portable wrapper bypasses and remain off.

The MultiGet `min-batch-size: 64` above is the recorded reference setting, not
a hard minimum or label invariant. Other supported positive values are allowed;
record the actual value and keep it matched across comparison arms. Do not
silently retune a reused historical control.

## Native `stage1`

Stage1 is the native hot point-cache treatment over the common CacheKit/RocksDB
base. Its standalone treatment enables both ValueState and MapState point caches:

```yaml
state.backend.cachekit.value.cache.max-entries: 8000
state.backend.cachekit.map.cache.max-entries: 8000
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.native.request-plane.enabled: true
state.backend.cachekit.native.request-plane.library: /absolute/path/to/libcachekit_native_request_plane_jni.so
state.backend.cachekit.native.request-plane.kernel: auto
state.backend.cachekit.native.value-cache.enabled: true
state.backend.cachekit.native.map-cache.enabled: true
state.backend.cachekit.native.request-plane.write-through-mutations: true

state.backend.cachekit.map.snapshot.cache.max-entries: 0
state.backend.cachekit.map.snapshot.small.max-entries: 0
state.backend.cachekit.map.snapshot.cache.native.enabled: false
state.backend.cachekit.bp-prefetch.enabled: false
state.backend.cachekit.bp-prefetch.async.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.local-preagg.enabled: false
state.backend.cachekit.native.prefetch.enabled: false
state.backend.cachekit.native.mailbox-batch.enabled: false
state.backend.cachekit.native.local-preagg.enabled: false
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: false

state.backend.cachekit.list-state.cow: false
state.backend.cachekit.list-state.ryw: false
state.backend.cachekit.priority-queue.opt: false
```

An isolated Stage1 leg does not imply adaptive bypass unless its five
`native.value-cache.adaptive-bypass.*` keys are present. Use a label such as
`stage1+bypass` when adding it to an older Stage1 protocol.

## Native `stage2`

Stage2 is the standalone EMPTY/SINGLE MapSnapshot cache. It is the sole native
MapSnapshot path and does not require the external Stage1/Stage3 request-plane
library.

```yaml
state.backend.cachekit.native.request-plane.enabled: false
state.backend.cachekit.native.value-cache.enabled: false
state.backend.cachekit.native.map-cache.enabled: false

state.backend.cachekit.map.snapshot.cache.max-entries: 8000
state.backend.cachekit.map.snapshot.small.max-entries: 1
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.classifier.enabled: false
state.backend.cachekit.map.snapshot.cache.native.remove-hint.enabled: false
state.backend.cachekit.map.snapshot.cache.native.library-path: ""

state.backend.cachekit.bp-prefetch.enabled: false
state.backend.cachekit.bp-prefetch.async.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.local-preagg.enabled: false
state.backend.cachekit.native.prefetch.enabled: false
state.backend.cachekit.native.mailbox-batch.enabled: false
state.backend.cachekit.native.local-preagg.enabled: false
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: false

state.backend.cachekit.list-state.cow: false
state.backend.cachekit.list-state.ryw: false
state.backend.cachekit.priority-queue.opt: false
```

The empty `native.library-path` selects the architecture-matched snapshot library
embedded in the CacheKit JAR. Verify actual activation: this path is expected to
be configured-but-inactive on unsupported platforms.

## Native `stage3`

Stage3 is cross-record cooperation over the shared native request plane. Stage1
point caches and Stage2 snapshot caching remain off.

```yaml
state.backend.cachekit.value.cache.max-entries: 0
state.backend.cachekit.map.cache.max-entries: 0
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.native.value-cache.enabled: false
state.backend.cachekit.native.map-cache.enabled: false

state.backend.cachekit.map.snapshot.cache.max-entries: 0
state.backend.cachekit.map.snapshot.small.max-entries: 0
state.backend.cachekit.map.snapshot.cache.native.enabled: false

state.backend.cachekit.native.request-plane.enabled: true
state.backend.cachekit.native.request-plane.library: /absolute/path/to/libcachekit_native_request_plane_jni.so
state.backend.cachekit.native.request-plane.kernel: auto
state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: 4096
state.backend.cachekit.mailbox-batch.timeout-us: 0
state.backend.cachekit.mailbox-batch.commutative-key-sort: false
state.backend.cachekit.local-preagg.enabled: true
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: 64
state.backend.cachekit.bp-prefetch.backpressure-gated: true
state.backend.cachekit.bp-prefetch.async.enabled: true
state.backend.cachekit.bp-prefetch.multiget.enabled: true
state.backend.cachekit.native.mailbox-batch.enabled: true
state.backend.cachekit.native.prefetch.enabled: true
state.backend.cachekit.native.local-preagg.enabled: true
state.backend.cachekit.native.local-preagg.indexed-fold.enabled: true
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: true

state.backend.cachekit.list-state.cow: false
state.backend.cachekit.list-state.ryw: false
state.backend.cachekit.priority-queue.opt: false
table.exec.mini-batch.enabled: false
```

## `native fullopt`

Native FullOpt uses the common Bloom and portable hot-path settings from
`fullopt`, then applies the frozen native combination below. These values take
precedence over the standalone stage values.

### Stage1 inside Native FullOpt

Only the ValueState native point cache is enabled. The MapState point cache is
off. The adaptive native cache bypass is required and enabled explicitly.

```yaml
state.backend.cachekit.value.cache.max-entries: 8000
state.backend.cachekit.value.cache.policy: LRU
state.backend.cachekit.value.cache.lru.overflow: 1024
state.backend.cachekit.value.bypass.enabled: false
state.backend.cachekit.value.hit-rate.threshold: 0.03
state.backend.cachekit.value.hit-rate.window: 5000
state.backend.cachekit.map.cache.max-entries: 0
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.map.iteration.cache-fill.enabled: false
state.backend.cachekit.map.bypass.enabled: false

state.backend.cachekit.native.request-plane.enabled: true
state.backend.cachekit.native.request-plane.library: /absolute/path/to/libcachekit_native_request_plane_jni.so
state.backend.cachekit.native.request-plane.kernel: auto
state.backend.cachekit.native.request-plane.capacity-entries: 8192
state.backend.cachekit.native.request-plane.key-arena-bytes: 2097152
state.backend.cachekit.native.request-plane.value-arena-bytes: 8388608
state.backend.cachekit.native.request-plane.batch-entries: 512
state.backend.cachekit.native.request-plane.batch-key-arena-bytes: 262144
state.backend.cachekit.native.request-plane.batch-value-arena-bytes: 1048576
state.backend.cachekit.native.request-plane.min-batch-size: 4
state.backend.cachekit.native.request-plane.batch-slots: 1
state.backend.cachekit.native.request-plane.aarch64-only: true
state.backend.cachekit.native.value-cache.enabled: true
state.backend.cachekit.native.map-cache.enabled: false
state.backend.cachekit.native.request-plane.write-through-mutations: true

state.backend.cachekit.native.value-cache.adaptive-bypass.enabled: true
state.backend.cachekit.native.value-cache.adaptive-bypass.window-probes: 4096
state.backend.cachekit.native.value-cache.adaptive-bypass.zero-windows: 2
state.backend.cachekit.native.value-cache.adaptive-bypass.resample-interval-probes: 4096
state.backend.cachekit.native.value-cache.adaptive-bypass.sample-slots: 64

state.backend.cachekit.native.value-cache.read-activated-write-through.enabled: false
state.backend.cachekit.native.value-cache.resident-mutation-batch.enabled: false
```

Do not confuse this adaptive native point-cache bypass with
`state.backend.cachekit.value.bypass.enabled`; the latter remains `false`.

### Stage2 inside Native FullOpt

```yaml
state.backend.cachekit.map.snapshot.cache.max-entries: 2000
state.backend.cachekit.map.snapshot.small.max-entries: 1
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.classifier.enabled: false
state.backend.cachekit.map.snapshot.cache.native.remove-hint.enabled: true
state.backend.cachekit.map.snapshot.cache.native.library-path: ""
```

### Stage3 inside Native FullOpt

```yaml
state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: 4096
state.backend.cachekit.mailbox-batch.timeout-us: 0
state.backend.cachekit.mailbox-batch.commutative-key-sort: false
state.backend.cachekit.local-preagg.enabled: true
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: 64
state.backend.cachekit.bp-prefetch.backpressure-gated: false
state.backend.cachekit.bp-prefetch.commutative-key-sort: false
state.backend.cachekit.bp-prefetch.async.enabled: true
state.backend.cachekit.bp-prefetch.multiget.enabled: true
state.backend.cachekit.bp-prefetch.multiget.chunk-size: 64
state.backend.cachekit.bp-prefetch.multiget.min-batch-size: 64
state.backend.cachekit.bp-prefetch.async-chunks.enabled: false
state.backend.cachekit.bp-prefetch.async-chunks.size: 64
state.backend.cachekit.bp-prefetch.staging.max-entries: 8192
state.backend.cachekit.value.sticky-update-in-place.enabled: true
state.backend.cachekit.value.lazy-staging.enabled: true
state.backend.cachekit.value.lazy-staging.max-retained-bytes: 67108864
state.backend.cachekit.native.mailbox-batch.enabled: true
state.backend.cachekit.native.prefetch.enabled: true
state.backend.cachekit.native.local-preagg.enabled: true
state.backend.cachekit.native.local-preagg.indexed-fold.enabled: true
state.backend.cachekit.native.map-distinct-batch-prefetch.enabled: true
state.backend.cachekit.native.fastlocal.runtime-dispatch.enabled: false
```

Native FullOpt also retains the five RocksDB Bloom settings from portable
`fullopt`, disables the unrelated ARM-point memtable paths, and keeps Chen and
Flink mini-batch off:

```yaml
state.backend.rocksdb.memtable.arm-point.enabled: false
state.backend.rocksdb.memtable.arm-point.map-flat-authority: false
state.backend.rocksdb.memtable.arm-point.map-keyhead-point-index.enabled: false
state.backend.cachekit.list-state.cow: false
state.backend.cachekit.list-state.ryw: false
state.backend.cachekit.list-state.cleared-keys.capacity: 200000
state.backend.cachekit.priority-queue.opt: false
table.exec.mini-batch.enabled: false
table.optimizer.distinct-agg.split.enabled: true
```
