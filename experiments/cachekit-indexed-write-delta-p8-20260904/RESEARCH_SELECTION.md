# P8 source-mechanism selection

## Profile constraint

The q9 CPU profile attributed 28.36% to RocksDB background flush/compaction, 20.14% to MapState
put (19.01% in RocksDB put), 29.62% to RocksDB get, and 19.33% to MapState iteration (14.92% in
RocksIterator seek). CacheKit's worker accounted for only 0.14%. A useful B therefore needed to
act on the synchronous MapState/RocksDB boundary and remain independent of A's SST compression
policy.

## Primary-source directions

- TRIAD retains hot data in memory and defers/batches storage work to reduce write amplification:
  <https://www.usenix.org/conference/atc17/technical-sessions/presentation/balmau>
- Accordion compacts a pipeline of mutable and immutable in-memory segments before data reaches
  disk: <https://www.vldb.org/pvldb/vol11/p1863-bortnikov.pdf>
- ADOC controls LSM pipeline imbalance online:
  <https://www.usenix.org/conference/fast23/presentation/yu>
- WiscKey separates values from the LSM tree:
  <https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu>
- RocksDB 6.20.3 exposes `WriteBatchWithIndex`, `GetFromBatchAndDB`, and
  `NewIteratorWithBase` in the frozen dependency:
  <https://github.com/facebook/rocksdb/blob/v6.20.3/include/rocksdb/utilities/write_batch_with_index.h>

P8 borrows the deferred, indexed in-memory-delta direction from TRIAD/Accordion, but is a new
Flink MapState integration rather than a reimplementation of either system. In particular,
RocksDB's `overwrite_key` setting affects the indexed view; it does not promise to remove every
older operation from the underlying write batch. P8's bounded claim is fewer synchronous DB writes
through deferred batch submission plus indexed read-your-writes.

## Deterministic selection

One point was assigned for each condition: direct match to the profile, exact support in frozen
RocksDB 6.20.3, independence from A, default-off bounded correctness surface, own runtime
activation counters, and a quick same-artifact q9 screen.

| Candidate | Profile | 6.20.3 | Orthogonal | Bounded | Counters | Quick screen | Total |
|---|---:|---:|---:|---:|---:|---:|---:|
| Indexed MapState pending-write delta | 1 | 1 | 1 | 1 | 1 | 1 | 6 |
| State-scoped Fluid-LSM controller | 1 | 1 | 1 | 1 | 1 | 0 | 5 |
| Ribbon filter binding | 1 | 0 | 1 | 1 | 1 | 1 | 5 |
| Adaptive packed MapState authority | 1 | 1 | 1 | 0 | 1 | 1 | 5 |
| WiscKey/Parallax-style value separation | 0 | 0 | 1 | 0 | 1 | 0 | 2 |

The indexed delta wins because it targets every q9 state-path hotspot with an API already present
in the exact runtime, while leaving the on-disk state representation, checkpoint representation,
and default behavior unchanged. Fluid control remains the next candidate if the delta's activation
is real but its throughput effect is insufficient.

The literature metadata helper named by the research skill was unavailable, so titles and
mechanisms were checked against the linked primary-source pages rather than a generated metadata
report.
