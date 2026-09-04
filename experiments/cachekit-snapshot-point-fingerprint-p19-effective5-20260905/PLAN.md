# P19 effective-five promotion campaign

P19's fresh q9 screen measured 520.90 K/s at 15.18 cores (34.33 K/s/core) for A and
581.26 K/s at 15.35 cores (37.87 K/s/core) for A+B, a nominal +10.31%. Activation showed
24,055,359 packed-fingerprint rejects out of 25,664,440 point-authority probes, while preserving
22,278,952 direct iterator-table hits. This passes the single-query direction gate but is not the
final performance claim.

The promotion campaign runs fresh interleaved A/A+B legs for q5, q9, q11, q15, and q18 using the
same P19 ARM artifact, 100M events, 8 TMs / 16 slots, and the fixed Kunpeng NUMA0 physical CPU set
with memnode 0. The host is shared under the user's explicit idle-NUMA coexistence authorization.
Every leg must prove no foreign CPU overlap before launch and continuously at five-second cadence;
full foreign-container signatures are retained as context, but disjoint NUMA1/2/3 job changes are
not treated as target-cluster overlap.

The promotion metric is the arithmetic mean of the five per-query K/s/core percentage uplifts.
The result table must also retain raw K/s, measured cores, valid-leg status, source/artifact hashes,
activation totals, and target-NUMA coexistence evidence. The final gate is mean uplift >=10.00%.
