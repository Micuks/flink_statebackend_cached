# CacheKit RocksDB no-compression P4 q9 result

Generated: `2026-09-04T04:20:47.144973+08:00`

Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.

| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Applied mode |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- |
| q5 | 1 | control | 78.39 | 976.80 | 12.46 | yes | SNAPPY_COMPRESSION=32 |
| q5 | 1 | no-compression | 82.02 | 987.52 | 12.04 | yes | NO_COMPRESSION=41 |
| q11 | 1 | control | 46.73 | 728.12 | 15.58 | yes | SNAPPY_COMPRESSION=40 |
| q11 | 1 | no-compression | 47.31 | 742.34 | 15.69 | yes | NO_COMPRESSION=40 |
| q15 | 1 | control | 53.06 | 680.89 | 12.83 | yes | SNAPPY_COMPRESSION=96 |
| q15 | 1 | no-compression | 53.39 | 709.52 | 13.29 | yes | NO_COMPRESSION=96 |
| q18 | 1 | control | 95.91 | 1480.00 | 15.44 | yes | SNAPPY_COMPRESSION=32 |
| q18 | 1 | no-compression | 108.28 | 1660.00 | 15.32 | yes | NO_COMPRESSION=32 |

| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| no-compression | 4 | +4.85% | +2.94% | +0.62% | +12.90% |

- Activation: PASS
- q9 screen >= +3.00%: PASS
- effective-query goal >= +10.00%: FAIL
