# CacheKit snapshot-owned key reuse P3 q9 result

Generated: `2026-09-04T03:10:00.037472+08:00`

Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.

| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Activation |
| --- | ---: | --- | ---: | ---: | ---: | --- | --- |
| q9 | 1 | control | 25.65 | 396.47 | 15.46 | yes | control |
| q9 | 1 | reuse | 25.82 | 401.13 | 15.54 | yes | yes |

| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| reuse | 1 | +0.66% | +0.66% | +0.66% | +0.66% |

- Activation: PASS
- q9 screen >= +3.00%: FAIL
- effective-query goal >= +10.00%: FAIL
