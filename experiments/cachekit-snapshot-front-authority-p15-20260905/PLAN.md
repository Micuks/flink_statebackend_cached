# P15 snapshot front authority

P15 keeps P14's exact value-bearing tiny-map authority but separates direct point lookups from
the bounded MapSnapshot LRU. The most recently completed tiny-map traversal publishes one copied
key/namespace snapshot into a front buffer. Direct `get` and `contains` compare against that row
before doing any LRU lookup; mutations, EMPTY/key-only replacement, and `clear` invalidate it.

P14 established +8.29% K/s/core on q9, but only 1.62M of 25.76M direct probes reached an
authoritative snapshot. P15 targets the remaining lookup overhead: front-buffer misses are simple
key/namespace comparisons and no longer perturb or probe the LRU.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Both legs contain the P15
source; only `CACHEKIT_MAP_SNAPSHOT_VALUE_AUTHORITY_ENABLED` differs. Activation requires nonzero
snapshot point probes, point snapshot hits, positive or negative point hits, and total elided
point gets. Promotion requires at least 10% K/s/core uplift before the effective-five expansion.
