# CacheKit P1 ready-gated prefetch q9 canary

## Frozen question

Does a two-slot, completion-gated, strictly ordered prefetch ring improve q9
throughput per measured core by at least 3.00% over the same-source Java FullOpt
control, while proving real completion-before-dispatch activation?

## Identity

- Base source: `7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000`
- P1 runtime source: `5a9d1e656715a403afac72ee1a516876ddbfb7f1`
- Frozen RocksDB/table overlay: `bbd39affde9278d44b9f78d201849bf929249d54`
- Platform: x86_64 host `114.116.229.206`
- Events/query: 100,000,000
- Topology: 8 TaskManagers, 2 slots/TM, 16 slots total, fixed cpusets
- Checkpointing: disabled by absence of `execution.checkpointing.interval`
- Object reuse: disabled
- MapState point cache and native request-plane treatments: disabled

The control and candidate use the same P1 artifact. The only treatment config
difference is
`state.backend.cachekit.bp-prefetch.ready-gated.enabled=false/true`.
Both set max in-flight batches to 2 and timeout to 5000 us.

## Correctness evidence before launch

- Streaming focused suite: 32 tests, 0 failures, 0 errors.
- CacheKit completion/lifecycle suite: 57 tests, 0 failures, 0 errors.
- Streaming Checkstyle and Spotless: pass.
- Review repaired completion-wrapper cancellation during backend close and
  synchronous prefetch-start failure fallback before artifact construction.

## Execution and validity

Run one breadth-first q9 round in order `control`, `ready-d2`. Each leg must
prove real job start/completion, warmup completion, positive throughput and CPU,
eight-TM CPU coverage, `cores <= 16.05`, exact config/artifact hashes, and clean
logs. The x86 host preflight rejects any foreign running container.
The harness has no query, variant, or round environment override; these arrays
are fixed to the identity above.

The candidate activation gate additionally requires positive batches, positive
completion-before-dispatch, maximum in-flight depth at least 2, positive staged
and consumed values, zero worker failure fallback, and zero staging admission
drops. Prometheus and close-time ValueState counters are retained separately.

## Dual-host quick-validation amendment (2026-09-03)

Run the same paired q9 control/`ready-d2` canary independently on cloud x86 and
Kunpeng when each host is eligible under the host-specific isolation gate. Each
host keeps its own same-artifact
control as the primary denominator. Same-host prior Java/RocksDB baselines may
be reused as secondary quick-validation references, but results are never
pooled across hosts or storage media.

The Kunpeng artifact is the P1 Java candidate with only
`META-INF/native/libcachekit_snapshot_jni.so` replaced by the audited AArch64
entry from the same-day Kunpeng base JAR. Entry-by-entry validation requires all
other 1,592 file entries to match the x86-built Java candidate. The frozen
Kunpeng candidate SHA-256 is
`97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc`.
Its inputs and RocksDB runtime are reused from the same-host 2026-09-03
campaign; that source campaign need not complete. The initial launch guard
required a wholly idle host and was narrowed by the NUMA amendment below.

The user subsequently authorized execution on an idle NUMA cluster even while
another host-local job is active. On Kunpeng, P1 therefore binds every service
to NUMA node 0 memory and CPUs 38--74, while the observed foreign campaign is
CPU-bound to NUMA node 2 CPUs 160--206. The harness rechecks CPU-set disjointness
before the campaign and before every leg. Because the host's Compose version
does not accept `cpuset_mems`, the harness uses `up --no-start` to create the
network, volumes, and stopped containers, applies and verifies Docker cgroup
memory-node 0 binding, and only then starts them.
Because the foreign containers permit
memory on nodes 0--3 and `/tmp` is a host-wide tmpfs, this is explicitly
co-located quick-validation evidence, not idle-host final performance evidence.
Cloud x86 has only two NUMA nodes and the active parent spans both, so this
exception does not make that host eligible.

## Advancement boundary

Advance only if both activation passes and q9 K/s/core uplift is at least
`+3.00%`. Only after this gate may depths 3/4 be screened or a 15-query R1 be
materialized. A failed gate is a recorded P1 rejection, not permission to reuse
the earlier P0 result or a non-contemporaneous baseline.

At 2026-09-03 19:46 +08:00 the x86 host was occupied by the existing
`ckx86bbd39abl` campaign (q11, R2). P1 staging/launch must wait for its owner to
finish; no foreign container may be stopped by this campaign.
