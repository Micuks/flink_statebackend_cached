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

- `hot2-a`: map value cache off, dirty overlay off, snapshot maintenance off.
- `hot2-overlay`: map value cache on, dirty overlay on, snapshot maintenance off.
- `hot2-maintained`: map value cache on, dirty overlay on, snapshot maintenance on.

The primary comparison is `hot2-maintained` versus `hot2-a`; the isolated P10 source effect is
`hot2-maintained` versus `hot2-overlay`. Every valid P10 leg must have nonzero maintenance
activation counters and must reduce dirty-overlay base iterator requests relative to P9.

## Promotion gate

Promote only if all legs pass the frozen CPU/topology/artifact gates, the P9 and P10 activation
markers agree with the rendered treatment, and q9 K/s/core improves enough for the effective-five
arithmetic mean versus the frozen RDB controls to remain above 10%.
