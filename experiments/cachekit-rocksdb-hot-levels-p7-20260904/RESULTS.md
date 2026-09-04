# P7 source-level hot-tier compression result

## Outcome

P7 passes the requested effective-query gate. On the five frozen Kunpeng effective queries, the
arithmetic mean of paired per-query K/s/core uplifts is **+11.04%**. All five legs completed with
valid CPU accounting and exact runtime evidence for the source policy.

This is a Flink RocksDB-backend source change, not only a RocksDB YAML/resource setting and not a
modification to the RocksDB C++ compression engine. The opt-in
`state.backend.rocksdb.compression.uncompressed-hot-levels` policy programs RocksDB's per-level
compression vector: hot2 uses `NO_COMPRESSION` for the first two policy slots and retains Snappy
for the colder five slots. The default is zero and preserves the prior single compression policy.

## Why this direction

The q9 CPU-time profile attributed 28.36% of samples to RocksDB background flush/compaction and
only 0.14% to the CacheKit prefetch worker. P7 therefore converts P6's successful global
no-compression experiment into a narrower source-controlled policy rather than adding more
prefetch scheduling machinery.

The source commit is `fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce`. The x86 and aarch64 P7
artifacts overlay only `RocksDBConfigurableOptions` and `RocksDBResourceContainer` class stems on
their frozen P4 artifacts; the audited non-overlay JAR entries are byte-identical.

## Verification

- Focused module tests: 43 run, 0 failures, 0 errors, 0 skipped; build success.
- Runtime policy logs: exact hot-level count, global compression type, and seven-entry per-level
  vector audited in every treatment leg.
- x86 same-artifact screen: hot2 improves q9 by +14.98% over the hot-level-zero Snappy control.
- The x86 hot2 row retains 99.68% of frozen P6 global-no-compression K/s/core while its captured
  SST bytes are 8.49% lower.

| x86 q9 variant | K/s/core | Raw K/s | Cores | vs same-artifact Snappy | SST bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| highmem-snappy | 32.10 | 488.55 | 15.22 | +0.00% | 15,516,066,739 |
| hot1 | 33.89 | 512.53 | 15.12 | +5.58% | 16,477,100,154 |
| hot2 | 36.91 | 544.42 | 14.75 | +14.98% | 31,078,570,606 |

## Kunpeng effective-5

| Query | Control K/s/core | hot2 K/s/core | hot2 raw K/s | Cores | Uplift | Valid |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| q5 | 78.39 | 81.35 | 962.68 | 11.83 | +3.78% | yes |
| q9 | 25.54 | 34.44 | 523.09 | 15.19 | +34.85% | yes |
| q11 | 46.73 | 46.37 | 721.00 | 15.55 | -0.77% | yes |
| q15 | 53.06 | 52.78 | 678.26 | 12.85 | -0.53% | yes |
| q18 | 95.91 | 113.04 | 1,730.00 | 15.27 | +17.86% | yes |

The headline is the arithmetic mean of those five percentages: **+11.04%**. Relative to the P6
global-no-compression candidate, hot2 is lower by only 0.86% on the arithmetic mean of the five
per-query K/s/core deltas. SST-size changes are query- and platform-dependent, so the 8.49% storage
reduction is claimed only for the x86 q9 screen, not for every query.

## Claim boundary

The Kunpeng controls are frozen same-host P4 controls reused as explicitly permitted for rapid
validation. P7 is an intended source-artifact change whose build boundary is independently
audited; the x86 same-artifact screen supplies the direct policy attribution. During q5 and q9, a
foreign campaign occupied NUMA1 while P7 was bound to NUMA0 CPUs 38-74 and memory node 0. The
foreign campaign ended before q11. The candidate host was container-clean at finalization.

The result establishes the +10% gate for the five **effective** queries only. It is not an all-15Q
mean claim and does not imply that the RocksDB C++ engine itself was modified.
