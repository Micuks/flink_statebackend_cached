# CacheKit ready-gated depth-four q9 result

Generated from the completed Kunpeng campaign at
`/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-depth4-q9-100m-kunpeng-20260904`.
Both `LEG.SHA256SUMS` manifests were independently rechecked after `CAMPAIGN_COMPLETE`.

| Query | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Activation |
| --- | --- | ---: | ---: | ---: | --- | --- |
| q9 | control | 25.67 | 400.10 | 15.59 | yes | control |
| q9 | ready-d4 | 25.27 | 392.00 | 15.51 | yes | yes |

Paired per-core uplift: **-1.5582391897%**.

The treatment independently proved depth four active on all 32 active metric series.  It reached a
maximum in-flight depth of four, retained at most 256 records, staged 3,996,928 values and consumed
3,924,120, with zero worker failures and zero staging-admission drops.  Only 21.70% of batches and
21.85% of records were ready before dispatch.  Aggregate worker queue time was 1,036.12 s versus
28.71 s of worker service, while ring-full time remained 253.70 s.

Decision: **REJECT** ready-gate depths two through four.  The path is active but non-positive on
both hosts at depth two and remains negative on the isolated Kunpeng depth-four screen.  Continue
with the profile-derived inline record-key MultiGet path in
`refine-logs/P2_INLINE_RECORD_MULTIGET_PLAN_20260904.md`.

Frozen identity:

- source commit: `5a9d1e656715a403afac72ee1a516876ddbfb7f1`
- AArch64 CacheKit artifact SHA-256:
  `97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc`
- control config SHA-256: `454140d6d33c70d51aacaa9042ab0e9379771bb7989c0f70cbe35011b604df04`
- ready-d4 config SHA-256: `0b83b5bb87b8c0f26500a1927c92b9d69d2aa4a7c11583d26e1d2772a28e60db`
- host isolation: NUMA node 0, CPUs 38,40,...,74; the foreign RocksDB campaign remained on
  disjoint NUMA node 2 CPUs 160,162,...,206.
