# CacheKit RocksDB no-compression P4 q9 result

Generated: `2026-09-04T03:44:42.428510+08:00`

Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.

| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Applied mode |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- |
| q9 | 1 | control | 25.54 | 396.37 | 15.52 | yes | SNAPPY_COMPRESSION=56 |
| q9 | 1 | no-compression | 32.40 | 493.72 | 15.24 | yes | NO_COMPRESSION=56 |

| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| no-compression | 1 | +26.86% | +26.86% | +26.86% | +26.86% |

- Activation: PASS
- q9 screen >= +3.00%: PASS
- effective-query goal >= +10.00%: PASS
