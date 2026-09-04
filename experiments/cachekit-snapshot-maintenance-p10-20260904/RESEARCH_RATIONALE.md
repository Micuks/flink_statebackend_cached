# Research-to-Mechanism Rationale

P9/P10 are adaptations built from the measured q9 state path, not ports of a published system.
The q9 profile put 28.36% of samples in RocksDB background flush/compaction and showed foreground
MapState `put`, point-read, and iterator work. The implementation therefore targets the specific
`put -> entries()` cycle that was forcing dirty values through RocksDB and reopening a base
iterator for every traversal.

An operator-level Top-1 rewrite is explicitly rejected as a candidate. Runtime logs prove that q9
already selects Flink's `FastTop1Function`, whose implementation uses an LRU plus `ValueState`;
the same profile attributes only 3.54% of CPU samples to Rank but 46.13% to the streaming join.
Reimplementing scalar Top-1 state would therefore replicate an active leading path while missing
the measured join-side MapState bottleneck.

The q9 SQL joins `auction.id = bid.auction`. Nexmark generates one auction row per auction id, but
the benchmark DDL exposes `auction` and `bid` as views without a declared primary key. Flink's
`JoinUtil.analyzeJoinInput` therefore selects `withoutUniqueKey` for both inputs, and runtime logs
confirm that both `left-records` and `right-records` are MapState rather than the existing
join-key-contains-unique-key ValueState fast path. This creates a safe, narrow opportunity for
P10: after the first complete traversal proves that a keyed MapState has exactly one member,
later bid-to-auction probes can reuse that exact singleton membership without reopening a RocksDB
iterator. Correctness does not assume auction uniqueness: a second distinct member exceeds the
configured exact capacity of one, invalidates the view, and falls back to the original MapState
iterator path.

## Leading ideas considered

- [Accordion](https://www.vldb.org/pvldb/vol11/p1863-bortnikov.pdf) reapplies LSM organization in
  memory to absorb updates and reduce disk flush/compaction work. P9 adopts only that general
  direction: retain per-key MapState deltas in the existing CacheKit value cache and merge them
  with a base iterator. It does not reproduce Accordion's HBase memstore layers.
- RocksDB's official
  [WriteBatchWithIndex](https://github.com/facebook/rocksdb/wiki/Write-Batch-With-Index) combines
  an indexed write delta with point reads and a base-plus-delta iterator. P8 directly screened
  that available abstraction. It activated strongly but regressed q9 by 5.73%, so it was rejected
  rather than presented as a new contribution.
- [DBToaster](https://www.vldb.org/pvldb/vol5/p968_yanifahmad_vldb2012.pdf) and
  [Differential Dataflow](https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf) demonstrate
  the broader principle of maintaining derived results from changes instead of recomputing them.
  P10 applies that principle to a deliberately narrow derived object: the exact membership of a
  small MapState. Once a complete traversal proves the set, later puts/removes maintain it by
  delta; `entries()` resolves those known members through point reads or dirty values and can
  avoid the RocksDB base iterator.

## What is new in the A+B combination

The implementation combines three existing CacheKit capabilities in a new state-specific path:

1. P9 keeps read-your-own-write values as a scoped dirty overlay instead of flushing on iteration.
2. P10 incrementally maintains only a *proven exact* membership set; an unknown set remains a
   miss and an insertion beyond exact capacity invalidates the view immediately.
3. The exact membership view names keys only. Values still come from the normal cache/point-read
   path, so the mechanism does not create a second value-consistency protocol.

Both mechanisms are default-off runtime gates and have independent terminal counters. The x86
screen uses one P10 artifact in all six legs, separating the fresh baseline, hot2 A, map-cache
capacity, P9 overlay, P10
incremental maintenance, and the capacity of the exact-membership data structure. Overlay and
maintained legs are both run at 2K and 64K, so capacity cannot be mistaken for the source effect.
The 64K capacity is aligned with the existing MapState value cache and is predeclared because the
prior 2K q9 screen recorded substantial exact-snapshot eviction churn. Promotion depends on
measured K/s/core and nonzero execution counters, not on resemblance to the cited designs.
