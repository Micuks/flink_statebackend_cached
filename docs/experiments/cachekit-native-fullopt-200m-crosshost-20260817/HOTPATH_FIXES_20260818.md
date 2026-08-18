# Native FullOpt hot-path corrections and retest

## Motivation

The first audited 200M cross-host sweep showed that the retained native trio regressed the Java
FullOpt+mailbox control by 2.08% on x86 and 2.62% on Kunpeng. Three independent reviews agreed that
the implementation was suitable only as an experimental plug-in until its grouping complexity,
composite-key identity, snapshot invalidation, and architecture dispatch were corrected.

## Implemented corrections

1. `GroupBatch` and `CompactBatch` now use a reusable, preallocated open-addressing table instead
   of comparing every candidate with every prior unique key. Each source key is fingerprinted once;
   state id, generation, length, and fingerprint are checked before the mandatory exact byte compare.
   First-seen group order is unchanged.
2. LocalPreAgg validates that a native plan assigns new groups as contiguous first-seen ids. A sparse
   or permuted JNI plan fails closed to Java grouping.
3. Native MapState point keys and MapSnapshot keys use length-prefixed serialized components. The
   previous one-byte delimiter encoding could alias under variable-length serializers.
4. Native MapSnapshot invalidation publishes a negative fill for the exact serialized
   key+namespace identity. A state-wide generation increment alone is not treated as invalidation.
5. Runtime layout is architecture-specific: x86 uses one 64-byte/eight-slot metadata bucket;
   AArch64/Kunpeng uses one 128-byte/sixteen-slot bucket.
6. Auto dispatch on x86 selects an SSE4.2 CRC32C/tag/equality kernel when CPUID reports support;
   scalar remains the fail-closed fallback. Kunpeng retains NEON CRC32C and SVE-256 hybrid dispatch.

## Verification completed before cluster retest

- C++ request-plane and JNI codec tests under ASan+UBSan: 2/2 passed.
- Large 4096-entry grouping/collision-safe plan added to the native suite.
- CacheKit Java suite: 160 passed, 0 failed, 6 JNI-gated tests skipped in the non-JNI invocation.
- True JNI bridge/coordinator run with the freshly built shared object: 16/16 passed, 0 skipped.
- LocalPreAgg targeted suite: 5/5 passed, including permuted native group ids.

## Retest protocol

Use the same semantic control and treatments as the first 200M campaign, with 5M warmup, 200M
measured events, checkpoints disabled, parallelism 16, eight TaskManagers, and one round on both
x86 and Kunpeng. Preserve exact rendered configs, runtime hashes, all per-query K/s/core rows, and
arithmetic means of per-query uplift. The corrected source commit and runtime artifact hashes must be
recorded in each experiment directory before results are accepted.

## Retest outcome

The one-round 200M retest completed 45/45 valid legs on each host. The retained native trio was
-4.12% on x86 and -2.79% on Kunpeng; adding Native LocalPreAgg was -1.38% on x86 and -5.51% on
Kunpeng. These are arithmetic means of per-query K/s/core uplift against the adjacent Java
FullOpt+mailbox control. See [RETEST_RESULTS_20260818.md](RETEST_RESULTS_20260818.md) for all raw
per-query values, group summaries, hashes, and interpretation.
