# CacheKit Kunpeng Nexmark results index (2026-08-05)

This directory freezes four audited result reports in the same reporting format:
raw K/s/core values (two decimals), per-query uplift, arithmetic-mean group uplift,
configuration identity, and validity/provenance notes.

- [Bloom-only: SST, memtable, and combined](BLOOM_ONLY.md)
- [FullOpt plus SST/memtable Bloom](FULLOPT_BLOOM.md)
- [Access-guided MultiGet](MULTIGET.md)
- [Kunpeng TSV110/LSE compiler contrast](TSV110_LSE.md)
- [CDC correctness evidence index](CDC_EVIDENCE.md)
- [Public paste URLs and listed verification](PASTE_URLS.md)

The public MicroBin URLs are recorded after upload in `PASTE_URLS.md`. Public
paste expiration is one week, which is the longest duration exposed by the
service UI; the Git copies are durable.

## Reporting rules

- Front eight: q4, q5, q8, q9, q11, q18, q19, q20.
- Back seven: q3, q7, q12, q13, q15, q16, q17.
- ValueState-only: q4, q5, q7, q8, q9, q11, q12, q15, q16, q17, q18.
- Any-state: ValueState-only plus q3, q19, q20.
- Group uplift is the arithmetic mean of per-query percentage uplifts.
- Raw K/s/core is displayed to exactly two decimals; calculations use the
  underlying unrounded values where available.
