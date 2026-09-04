# P18 snapshot-iterator authority-table lookup

P18 keeps P17's value-bearing snapshot semantics and allocation-minimal SINGLE iterator, while
removing an access-order LRU operation from the dominant iterator hit path. P17 measured about
17.96M authoritative iterator short-circuits on q9. Those hits still called the general snapshot
LRU before using the separate four-way value-authority table, causing a `LinkedHashMap.get` and
access-order relink even though the authority table already held the exact same complete snapshot.

When value authority is enabled, P18 probes its fixed four-way table first for iterator lookups.
An exact hit serves the snapshot directly; a miss falls back to the unchanged general snapshot
cache. Wrapper mutations continue to invalidate both structures together. A dedicated counter and
focused test prove that table-served iterators bypass the general cache-policy probe.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Passing the 10% K/s/core gate
promotes this source candidate to a fresh effective-five campaign.
