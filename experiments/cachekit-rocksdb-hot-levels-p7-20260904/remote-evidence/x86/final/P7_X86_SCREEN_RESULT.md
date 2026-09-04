# CacheKit P7 x86 source-policy screen

Generated: `2026-09-04T11:00:11.157713+08:00`

| Variant | Hot levels | K/s/core | Raw K/s | Cores | vs same-artifact Snappy | SST files | SST bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| highmem-snappy | 0 | 32.10 | 488.55 | 15.22 | +0.00% | 96 | 15516066739 |
| hot1 | 1 | 33.89 | 512.53 | 15.12 | +5.58% | 95 | 16477100154 |
| hot2 | 2 | 36.91 | 544.42 | 14.75 | +14.98% | 166 | 31078570606 |

- hot2 throughput retained versus frozen global no-compression: `99.68%`
- hot2 SST-byte reduction versus frozen global no-compression: `8.49%`
- Source-policy screen: `PASS`
