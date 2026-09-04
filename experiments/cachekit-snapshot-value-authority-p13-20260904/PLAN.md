# P13 value-bearing tiny MapState authority

## Profile-grounded hypothesis

The fresh P11 A leg recorded 22.48M MapSnapshot hits, including 18.78M SINGLE short circuits.
Those SINGLE hits avoid an iterator but still issue a RocksDB point get for the value. P12 proved
that a native indexed pending-write lookup on almost every read is net negative, so P13 moves the
authority to the already-hot bounded Java snapshot instead.

## A + B

- A: P7 hot-level compression with the existing key-only EMPTY/SINGLE/SMALL MapSnapshot cache.
- B: after a complete bounded traversal, store copied values beside the cached user keys. A
  value-bearing hit returns copied values without a RocksDB/JNI point get.
- Any `put`, `putAll`, `remove`, iterator mutation, or clear follows the existing exact snapshot
  invalidation/replacement path. Partial traversals never publish authority. Standalone native
  snapshots and incremental snapshot maintenance are excluded from this screen.
- The feature is default-off and enabled only by
  `CACHEKIT_MAP_SNAPSHOT_VALUE_AUTHORITY_ENABLED=true`.

This is an A+B source change inspired by packed small-map/row-store authority: it reuses the
existing admission and completeness proof, but adds Flink-specific value copying, mutation
invalidation, iterator semantics, and terminal activation counters.

## Gate

Run fresh q9 A/A+B on the same Kunpeng NUMA0 physical cluster and P13 artifact. Require nonzero
fills, short circuits, and point gets elided in A+B, with no activation marker in A. Promote only
at at least 10% K/s/core uplift, then run fresh same-artifact q5/q9/q11/q15/q18 and calculate the
arithmetic mean of per-query K/s/core uplifts.
