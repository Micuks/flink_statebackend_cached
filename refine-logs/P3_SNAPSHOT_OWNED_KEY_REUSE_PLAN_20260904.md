# P3 MapSnapshot owned-key reuse plan

## Trigger and evidence

P2 is active but improves q9 by only 0.35%. The prior q9 allocation profile attributes 23.28% of
the join scope to `CachedInternalMapState.copyUserKey`, 15.78% to `SnapshotMapEntry` construction,
and large overlapping shares to `BinaryRowData.copy` and `RowDataSerializer.copy`. Both source and
Rank scopes are smaller or outside CacheKit's state path.

## Change

Add a default-off `state.backend.cachekit.map.snapshot.owned-key-reuse.enabled` gate. When enabled:

- a user-key copy made for snapshot ownership is also the entry's private mutation key;
- cached snapshot keys remain private and are reused internally instead of copied twice per hit;
- the defensive key returned by `Map.Entry.getKey()` is copied lazily, so values-only iteration
  never pays for it;
- `setValue`, iterator `remove`, arrival order, snapshot invalidation and fallback remain unchanged.

The ordinary path remains byte-for-byte behaviorally unchanged with the gate off. The treatment
must expose active-state, internally reused-key, avoided-copy, deferred-copy and materialized-copy
counters. A query is effective only if the independent metric audit proves the gated path active.

## Screen

Build one same-source artifact and run a fresh paired q9 100M screen on the idle Kunpeng NUMA node.
The only config difference is the new gate. Advance only if correctness tests pass, activation is
positive, all legs are valid, and K/s/core improves enough to remain credible toward the >10%
effective-query arithmetic-mean goal.
