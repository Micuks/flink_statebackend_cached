# Native request-plane microbenchmark

This executable measures the real `RequestPlane::ProbeBatch` path. It does not
contain expected-performance constants or synthetic result rows. Unsupported
ISA requests are emitted as `skip,unsupported`; `selected_kernel` records the
kernel that actually ran.

The default matrix is fixed:

- key lengths: 16, 32, 64, 128 bytes
- batch sizes: 8, 32, 64, 128
- hit ratios: 0%, 50%, 100%
- pairs: scalar/NEON+CRC, scalar/SVE-256, scalar/auto, NEON+CRC/SVE-256
- five repeated `A-B-B-A` cycles after per-kernel warmup
- 8,192 warmup keys and at least 32,768 measured keys per leg
- eight deterministic hot workload batches per scenario
- one full-capacity churn leg at 16,384 entries and 250,000 timed
  insert/evict operations
- fixed seed: `0x43414348454b4954`

Build and run on the target host:

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DCACHEKIT_NATIVE_BUILD_TESTS=OFF \
  -DCACHEKIT_NATIVE_BUILD_BENCHMARKS=ON
cmake --build build-release -j
./build-release/cachekit_native_request_plane_microbench \
  --output native-request-plane.csv
```

`raw` rows contain every timed leg. `summary` rows contain the median `ns/key`
and its corresponding `Mkeys/s` for each requested kernel in each pair.
The `churn_summary` row reports `ns/op` for a full native plane whose every
timed fill performs one O(1) intrusive-LRU eviction and reuses the bounded key
and value arenas. The churn uses fixed 32-byte keys and varying 16--96-byte
values so free-list fragmentation/reuse remains visible instead of measuring
only a single allocation size.
`metadata` records the fixed seed and detected AArch64/NEON/CRC/SVE features.
A valid complete run ends with a `complete,ok` row; a partial file is not a
successful benchmark.

The direct `neon_crc_vs_sve256` pair is the architecture-specific comparison:
both legs use the AArch64 CRC32C instructions. Scalar comparisons intentionally
remain in the output as implementation diagnostics, but they must not be
reported as SVE uplift because the scalar kernel uses a software CRC32C loop.

The timed metric is named `probe_batch_plus_one_result_sample`: the timer
contains `ProbeBatch` plus one result-dependent checksum update per batch. Each
workload is fully probed and checked against its requested hit ratio before any
timed leg.

For a short plumbing/sanitizer check without changing the matrix:

```bash
./build-release/cachekit_native_request_plane_microbench \
  --output smoke.csv \
  --cycles 1 \
  --warmup-keys 128 \
  --measured-keys 256 \
  --workload-batches 2 \
  --churn-capacity 256 \
  --churn-operations 1024
```
