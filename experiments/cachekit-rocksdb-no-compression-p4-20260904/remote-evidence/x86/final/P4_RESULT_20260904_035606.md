# CacheKit RocksDB no-compression P4 q9 result

Generated: `2026-09-04T03:56:06.774033+08:00`

Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.

| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Applied mode |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- |
| q9 | 1 | control | 27.23 | 423.23 | 15.54 | yes | SNAPPY_COMPRESSION=56 |
| q9 | 1 | no-compression | 34.61 | 526.21 | 15.21 | yes | NO_COMPRESSION=56 |

| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| no-compression | 1 | +27.10% | +27.10% | +27.10% | +27.10% |

- Activation: PASS
- q9 screen >= +3.00%: PASS
- effective-query goal >= +10.00%: PASS
