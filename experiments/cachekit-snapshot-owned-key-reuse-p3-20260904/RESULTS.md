# P3 snapshot-owned key reuse verdict

P3 is rejected as a primary performance optimization. Its mechanism activated on the Kunpeng q9 treatment leg, but the paired K/s/core uplift was only `+0.66%`, below both the `+3%` screen gate and the user's `+10%` effective-query goal.

| Query | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Activation |
| --- | --- | ---: | ---: | ---: | --- | --- |
| q9 | control | 25.65 | 396.47 | 15.46 | yes | control |
| q9 | reuse | 25.82 | 401.13 | 15.54 | yes | yes |

The treatment reported 64 active states, 95,248,798 owned internal-key reuses, 80,611,480 avoided internal-key copies, 95,251,462 deferred exposed-key copies, and 95,249,813 materialized exposed-key copies. Thus the rejection is based on performance, not failure to activate.

The follow-up CPU-time profile explains the small result: `copyUserKey` accounted for only 221 leaf samples (`0.34%`) while RocksDB background flush/compaction accounted for `28.36%` of the complete 65,504-sample capture. P4 therefore moved to RocksDB compression work rather than expanding P3.
