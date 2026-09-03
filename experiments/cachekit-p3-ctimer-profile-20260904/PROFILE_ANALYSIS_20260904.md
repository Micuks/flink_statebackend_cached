# Kunpeng q9 P3 CPU-time profile

This diagnostic run used async-profiler `ctimer` for 90 seconds on the busiest JVM in each of the two TaskManager containers. It is diagnostic-only: its throughput row is not part of a paired performance claim.

## Capture identity

- Platform: Kunpeng, NUMA node 0, even logical CPUs `38..74`, memory node `0`.
- Query and scale: Nexmark q9, 100 million events.
- Runtime source: `18cfb52bdb6e740d9ae716da0a03761481e885e2`.
- Runtime artifact: `0a1981cbd3591b4c99dbd5d7d3a32170ab6c630fa1dcdb4faa16005b9acf5f49`.
- Capture completeness: both `ctimer-check.txt` files contain `OK`; each profile has its own `PROFILE.SHA256SUMS` and target record.
- Combined sample population: 65,504 CPU-time samples across 8,977 collapsed stacks.

The first launch failed before measurement because the cloned runtime manifest retained absolute paths from the source campaign. It was preserved remotely as `results/failed/001-r1-q9-control-runtime-path`; the corrected retry completed and is the only profile analyzed here.

## Thread attribution

| Thread group | Samples | Share |
| --- | ---: | ---: |
| Join operator | 30,219 | 46.13% |
| RocksDB background flush/compaction | 18,575 | 28.36% |
| Source | 10,646 | 16.25% |
| Rank | 2,317 | 3.54% |
| GC/VM | 1,716 | 2.62% |
| Netty | 1,165 | 1.78% |
| Other | 772 | 1.18% |
| CacheKit prefetch worker | 94 | 0.14% |

Thread groups are mutually exclusive and derived from the first collapsed-stack frame. The table sums to all 65,504 samples.

## Join-path inclusive attribution

Inclusive stack presence within the same sample population showed:

- `CachedInternalMapState.get`: 30.47%; RocksDB `get`: 29.62%.
- join `addRecord`: 39.16%; MapState `put`: 20.14%; RocksDB `put`: 19.01%.
- iterator `hasNext`/`loadCache`: 19.33%; RocksIterator `seek`: 14.92%.
- map-snapshot short-circuit path: about 14.8%.
- `CachedInternalMapState.copyUserKey` leaf samples: 0.34% (221 samples); the broader P3 key-copy target is too small to explain a large gain.

`CopyingChainingOutput` appears on 17.48% of stacks, but chain-copy-elision is frozen and identical across state-native A/B legs, so it is not attributed to CacheKit.

## Decision

P3 owned-key reuse is rejected as a primary optimization because its paired q9 result was only `+0.66%` and the CPU-time profile confirms that defensive key copying is a small leaf cost. P4 instead targets RocksDB compression work: background flush/compaction alone consumes 28.36% of CPU time, while foreground `get`/`put` also enters the same native library. The P4 control explicitly keeps `SNAPPY_COMPRESSION`; only the treatment uses `NO_COMPRESSION`, and the runtime logs independently audit the applied mode.
