# P14 snapshot point authority

P14 extends the effective P13 value-bearing tiny-map snapshot (+7.02% on q9) to direct MapState
point operations. After a complete bounded traversal proves the exact key set and stores copied
values, `get` and `contains` use that snapshot for both positive and negative lookups. UNKNOWN
snapshots still fall through to the existing cache/delegate path, and all mutation invalidation
rules remain unchanged.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Both legs contain the P14
source; only `CACHEKIT_MAP_SNAPSHOT_VALUE_AUTHORITY_ENABLED` differs. Activation requires nonzero
snapshot point probes, point snapshot hits, positive or negative point hits, and total elided
point gets. Promotion requires at least 10% K/s/core uplift before the effective-five expansion.
