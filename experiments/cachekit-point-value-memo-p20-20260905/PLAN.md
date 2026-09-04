# P20 SIEVE point-observation memo

P19's valid effective-five campaign isolated q11's coverage problem: 68,318,923 snapshot-authority
point probes produced only 6,398 point-get elisions. The full-snapshot authority has no way to help
until a complete bounded traversal has occurred, even though session-window MapState repeatedly
observes the current point value through get/put.

P20 keeps P19 intact and adds a separate write-through point-result memo. Each four-way directory
slot owns one `(state key, namespace)` and its last exact user-key observation. Packed fingerprints
reject absent owners before equality checks. Eviction uses SIEVE visited bits: hits set one bit but
do not reorder an access-order list; replacement clears visited bits while advancing a per-set hand.
Values and keys are defensively copied, present-without-value observations cannot answer `get()`,
and put/remove/clear maintain exact wrapper semantics. The general MapState write-back cache remains
disabled.

The first external gate is a fresh q11 P19/P20 canary on an idle Kunpeng NUMA cluster. P20 expands
only if both the leg integrity gate and point-memo activation counters pass. Final claims remain
fresh A versus A+P19+P20, 100M events, with effective-query uplift averaged arithmetically.
