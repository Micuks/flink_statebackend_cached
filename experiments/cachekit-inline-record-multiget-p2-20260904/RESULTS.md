# CacheKit inline record-key MultiGet P2 result

The valid Kunpeng q9 pair used one AArch64 artifact and differed only in
`state.backend.cachekit.bp-prefetch.immediate-record.enabled`.

| Query | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Activation |
| --- | --- | ---: | ---: | ---: | --- | --- |
| q9 | control | 25.60 | 398.01 | 15.55 | yes | control |
| q9 | immediate | 25.69 | 398.16 | 15.50 | yes | yes |

Paired per-core uplift: **+0.3515625%**.

Activation was substantial: 32 positive active series, 384,935 RocksDB MultiGet batches,
3,884,602 keys, 3,884,745 staged values, 3,850,785 consumed staged values and zero backend
failures. The arithmetic anomaly of 143 more staged values than keys is the aggregation of
monotonic per-state gauges sampled at slightly different instants; it is immaterial to the
activation gate. The staged-consumption rate was 99.13%.

Decision: **REJECT**. The implementation removes the async queue and performs useful MultiGets,
but it does not materially improve q9. Profile attribution now moves to the MapState snapshot-entry
copy path (`copyUserKey`, `SnapshotMapEntry`, and `BinaryRowData.copy`), which dominates the join
allocation scope.

One pre-result treatment attempt is preserved under `results/failed`: its compose file mounted the
control config, live activation remained zero, and it was stopped before producing a result. It is
not included in the paired comparison.

Frozen identity:

- runtime source commit: `a7aa558791becb34164b27c787ab6cffdebf0dc2`
- AArch64 artifact SHA-256: `164411a963b78ccd508524c9ca4e1a0d50b497830394a59c016ed3ef644fd741`
- control config SHA-256: `26f3a7e46a67e6c104a749e21fd7814fe87cf60a9104793de7ad8034387c894f`
- immediate config SHA-256: `8064e4cc3f3a147d593050bb4207e9f54821efca81393025bb2e8756c0fa9634`
- host isolation: NUMA node 0, CPUs 38,40,...,74 and `cpuset-mems=0`; the foreign campaign
  remained on disjoint NUMA node 1 CPUs 118,120,...,154.
