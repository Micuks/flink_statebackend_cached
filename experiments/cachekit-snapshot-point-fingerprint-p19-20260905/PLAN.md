# P19 point-authority control-word fingerprint filter

P18 reached +9.50% K/s/core on q9 and exposed a sharply asymmetric table workload: 25.64M point
probes produced only 1.61M snapshot hits, while 22.24M iterator lookups hit the same four-way
authority table. Scanning four full hashes and keys on roughly 24M point misses is treatment-only
overhead, but adding work to the iterator path would tax its dominant hit stream.

P19 adds one 64-bit control word per set, packing four nonzero 16-bit fingerprints in the style of
SwissTable control metadata. Point probes compare all four fingerprints with a single subtraction
and mask operation; an exact lane match falls through to the unchanged full-hash/key verification,
so fingerprint collisions only create false positives and cannot change answers. Stores, evictions,
and invalidations update the affected 16-bit lane. Iterator probes deliberately bypass this filter
and retain P18's direct lookup path.

The q9 screen uses fresh same-artifact A/A+B legs on Kunpeng NUMA0. Passing the 10% K/s/core gate
promotes P19 to the effective-five campaign.
