# P12 indexed MapState delta on Kunpeng

## A + B

- A: P7 hot-level compression (`uncompressed-hot-levels: 2`) with the existing CacheKit
  configuration.
- B: the source-level Flink MapState integration of RocksDB `WriteBatchWithIndex`, enabled by
  `state.backend.rocksdb.write-batch-with-index.enabled: true`.
- Control and treatment use the same ARM artifact
  `de935deec5eb709c8afbae79e1161a9ecaadc30c5e7cc89602b04404ca2c3e29`; the indexed flag is the
  only rendered configuration difference.

B defers bounded MapState puts/deletes, serves point reads through `GetFromBatchAndDB`, and uses
merged iterators for read-your-writes. It fences the delta before snapshot, savepoint, bulk-read,
clear, migration, key enumeration, and entry-count operations. This combines the deferred
in-memory-delta direction from TRIAD/Accordion with Flink-specific state and checkpoint semantics;
it is not a RocksDB option-only treatment.

## Correctness and activation gates

- `RocksDBBatchMapReaderTest` covers pending-write visibility through `get`, `keys`, `values`,
  `entries`, `iterator`, and `isEmpty`, while proving the iterator paths do not silently flush.
- The treatment must emit the indexed-write-batch terminal marker with nonzero MapState puts,
  nonzero flushes, more than one entry per flush, and useful pending point-read or merged-iterator
  activity. The control must not emit the marker.
- Dirty overlay and snapshot maintenance remain disabled in both legs.

## Screen

Run q9 at 100M events on Kunpeng NUMA0 using physical CPUs 38--74 (even CPUs), memory node 0,
and runner CPU 76. Foreign workloads are allowed only with saved proof of disjoint CPU sets and
memory nodes. Promote only if A+B improves same-artifact A by at least 10% in valid K/s/core.

If promoted, run fresh same-artifact q5/q9/q11/q15/q18 controls and treatments and calculate the
arithmetic mean of per-query K/s/core uplifts. Cross-host throughput is never pooled.
