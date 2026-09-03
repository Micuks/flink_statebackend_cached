# CacheKit RocksDB no-compression P4 q9 result

Generated: `2026-09-04T04:20:47.734000+08:00`

Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.

| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Applied mode |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- |
| q5 | 1 | control | 95.83 | 1160.00 | 12.09 | yes | SNAPPY_COMPRESSION=50 |
| q5 | 1 | no-compression | 103.50 | 1230.00 | 11.88 | yes | NO_COMPRESSION=41 |
| q11 | 1 | control | 68.14 | 1060.00 | 15.57 | yes | SNAPPY_COMPRESSION=40 |
| q11 | 1 | no-compression | 69.01 | 1060.00 | 15.40 | yes | NO_COMPRESSION=40 |
| q18 | 1 | control | 115.95 | 1840.00 | 15.83 | yes | SNAPPY_COMPRESSION=32 |
| q18 | 1 | no-compression | 131.91 | 2050.00 | 15.51 | yes | NO_COMPRESSION=32 |

| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| no-compression | 3 | +7.68% | +8.00% | +1.28% | +13.76% |

- Activation: PASS
- q9 screen >= +3.00%: PASS
- effective-query goal >= +10.00%: FAIL
