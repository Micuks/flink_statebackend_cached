# P9 MapState dirty-overlay screen

## Hypothesis

q9 performs a MapState update followed by an iteration for nearly every record. The existing
write-back cache flushes the current key before iteration, so the workload cannot retain a mutable
in-memory delta. P9 keeps dirty entries in the Java cache and merges them over the RocksDB base
iterator. This follows the mutable-component principle used by Accordion and TRIAD, but implements
it inside CacheKit's existing consistency and lifecycle boundary.

## Same-artifact variants

- `hot2-a`: P7 hot-level control, MapState value cache disabled, overlay disabled.
- `hot2-cache`: MapState cache capacity 65,536, old flush-before-iteration behavior.
- `hot2-overlay`: the same capacity with the new dirty-overlay iterator enabled.

The A+B headline uses `hot2-overlay` versus `hot2-a`. Source attribution uses
`hot2-overlay` versus `hot2-cache`; a result driven only by cache capacity is not accepted as a
source-mechanism win.

## Gates

- All legs use the same P9 JAR on the same idle host and storage path.
- Config and compose differences are audited before execution.
- All q9 legs must pass the existing 8-TM/16-slot, CPU accounting, completion, and artifact gates.
- Treatment logs must contain nonzero iterator, avoided-flush, dirty-snapshot, and merged-entry
  counters. Both controls must contain no overlay marker.
- If q9 passes, expand to the effective-query set and compute the arithmetic mean of per-query
  K/s/core uplifts.

## Research basis

- Accordion: https://www.vldb.org/pvldb/vol11/p1863-bortnikov.pdf
- TRIAD: https://www.usenix.org/conference/atc17/technical-sessions/presentation/balmau
- ADOC: https://www.usenix.org/conference/fast23/presentation/yu
