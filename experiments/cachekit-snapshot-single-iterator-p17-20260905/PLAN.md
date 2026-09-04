# P17 authoritative SINGLE iterator specialization

P17 targets the dominant P16 hot path rather than changing RocksDB knobs or cache capacity. q9
executed about 17.87M authoritative iterator short-circuits, and the configured snapshot size is
one. The generic path built intermediate value/key lists, a removed-slot array, an iterable, an
iterator, and an entry while eagerly copying exposed keys and values.

For exact value-bearing SINGLE snapshots, P17 now creates a dedicated iterable plus an iterator
that is also the Map.Entry. State key and namespace remain defensively copied, while user key and
value copies are deferred until `getKey` and `getValue`. Multiple iterators share `setValue` and
`remove` state exactly as the generic iterable did. Multi-entry and key-only snapshots retain the
existing path. P16's four-way point-authority table remains in place.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Passing the 10% K/s/core gate
promotes P17 to the effective-five campaign.
