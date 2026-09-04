# P10 Incrementally Maintained Exact MapState Snapshot

## Hypothesis

P9 avoids flushing dirty MapState entries before iteration, but q9 still opens a RocksDB
base iterator on every `entries()` request. P10 treats the small exact membership snapshot as
an incrementally maintained materialized view: after a fully consumed traversal proves the
member set, a put/remove updates that set exactly while it remains within the configured exact
capacity. A following traversal can resolve the known keys through point reads or the dirty value
cache and skip the RocksDB iterator entirely.

This is a source-level A+B mechanism, not an additional RocksDB option. The runtime gate is
`CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED`; it is effective only when the P9 dirty overlay,
MapState value cache, and MapSnapshot cache are also enabled.

## Screen

Run q9 on x86 with one P10 artifact in every leg:

- `baseline`: the same high-memory configuration and P10 artifact as all other legs, with hot-level
  compression policy at zero and both source gates off.
- `hot2-a`: map value cache off, dirty overlay off, snapshot maintenance off.
- `hot2-overlay`: map value cache on, dirty overlay on, snapshot maintenance off.
- `hot2-maintained`: map value cache on, dirty overlay on, snapshot maintenance on, with the
  inherited 2,000-entry exact-membership LRU.
- `hot2-overlay-64k`: dirty overlay on and snapshot maintenance off, with a 65,536-entry
  exact-membership LRU; this controls for capacity without B.
- `hot2-maintained-64k`: the predeclared A+B treatment; identical to `hot2-maintained` except
  that the exact-membership LRU is aligned with the 65,536-entry MapState value cache.

The headline integrated comparison is `hot2-maintained-64k` versus `baseline`; the incremental
B-over-A comparison is `hot2-maintained-64k` versus `hot2-a`. The isolated P10 source effect is
measured twice: `hot2-maintained` versus `hot2-overlay` at 2K and `hot2-maintained-64k` versus
`hot2-overlay-64k` at 64K. Capacity effects are separately measured with maintenance both off and
on. The 64K treatment is declared before execution because prior q9 evidence showed about 335K
exact-snapshot evictions at the inherited 2K capacity. Every maintained leg must have nonzero
maintenance activation counters and must reduce dirty-overlay base iterator requests relative to
its same-capacity overlay control.

## Promotion gate

Promote only if all legs pass the frozen CPU/topology/artifact gates, the P9 and P10 activation
markers agree with the rendered treatment, the 64K source effect is positive, and q9 A+B improves
over the fresh same-artifact baseline. The final effective-five mean will use fresh same-artifact
baseline/A/A+B legs; older P4 rows remain screening references only.
