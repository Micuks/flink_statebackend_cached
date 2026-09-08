# FullOpt+LC: reproduce the Kunpeng historical +65.48% configuration

## CacheKit option namespace migration

LC now uses `cachekit.binary-string.lazy-copy.enabled` or
`CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED`, default OFF, with JVM property
precedence. The old Flink-prefixed names are no longer accepted by the runtime.
This remains a CacheKit-owned optimization implemented in Flink's string-copy path.
Rebuild and restart: merely renaming the environment on an old JAR will not enable LC.
The preparation tool migrates the archived environment and audit code automatically.
Historical artifacts/results remain unchanged.

The verification below permits exactly the two option-name UTF8 constant changes
in `BinaryStringData.class`; all 42 other target classes and every non-target entry
must still match the archive. `lock.json.class_sha256` retains historical hashes;
`rebuild.py` derives the renamed class expectation from the verified reference.
This revision is not byte-identical to the historical runtime and was not benchmarked.

This is the supported **frozen-runtime reproduction path**, not a promise that
an arbitrary full-tree Maven distribution or YAML alone yields +65%.
`cachekit/dev` now contains the five tested source families, their tests and
three compile-time prerequisites. The unrelated experiment history is not merged.

The reference result is 100M events, no periodic checkpoint, 15 queries, one round,
8 TM JVMs / 16 slots on Kunpeng: mean per-query **K/s/core** uplift of +65.48%
against historical RocksDB and +15.60% against historical FullOpt. It is neither
65% on every query nor an x86 claim (the x86 historical RDB comparison was -11.34%).
The historical controls are non-contemporaneous and have NUMA/runtime differences.
This publication was checked by compilation, tests and bytecode comparison;
**the user explicitly requested no new performance experiment**.

## Identity and required external artifacts

- Historical five-family source pin: `7b95de31e51a6ee6fec4e4b1f625dc6538960d2c`.
- Reference campaign on the existing Kunpeng host:
  `/home/wuql/flink-cluster/experiments/cachekit-fullopt-p29-p30-100m-kunpeng-20260908`.
- Its `inputs/runtime/` holds the three locked JARs in [lock.json](lock.json).
  The lock also records all five source hashes and all 43 compiled class hashes.
- Its `inputs/`, runner, native libraries, measurement collector, benchmark driver,
  side input and content-addressed Docker images are required. Some unchanged
  Compose dependencies point to the September 4 archive on that host; preserve
  those paths. The preparation script does not fetch missing dependencies.
- Build environment verified here: OpenJDK 11.0.23, repository `./mvnw` Maven 3.2.5,
  and the existing project Maven dependency repository. In particular this branch
  uses a custom `frocksdbjni` dependency; do not silently substitute upstream JNI.
  This is not a clean-machine bootstrap or a publication of the binary archives.

Get host access using `skills/nexmark-bench-run/SKILL.md`; never embed credentials.
Copy the reference runtime JARs to a local directory if building on x86. JVM bytecode
is portable; keep the **Kunpeng reference runtime**, not x86 native libraries.

## 1. Build and verify (does not launch experiments)

From the root of `cachekit/dev`, with the three reference JARs already available:

```bash
python3 reproduction/fullopt-lc/rebuild.py --compile \
  --reference-runtime /absolute/path/to/reference-runtime \
  --output /absolute/path/to/new-rebuilt-runtime
```

The output directory must not exist. The tool compiles from this checkout,
requires exact source and bytecode hashes (with the two-name migration above), replaces only target classes in the
locked runtime and compares **every ZIP entry** to the reference. Archive SHA256
may differ from ZIP metadata/recompression; no other class/resource changes are allowed.
`BUILD_AUDIT.json` records the branch commit, dirty status, source hashes and
each output JAR hash. Missing or stale classes fail closed. No second table-common
JAR should be added to the classpath: P30 belongs in the existing table API uber JAR.

The reference includes dormant exploratory code inside the two state classes.
Those sources are intentionally preserved to match the measured classes; the
environment switches keep those paths off. `PrefetchExecutor`,
`MapSnapshotCacheMetrics` and `NativeMapSnapshotOptions` changes are needed for
source compilation, but are **not additional overlay families** in this frozen
runtime recipe. A full distribution built from the branch is a different artifact
and is not covered by byte-for-byte reference equivalence.

## 2. Config and settings

[kunpeng-flink-conf.yaml](kunpeng-flink-conf.yaml) is the exact measured config;
its original provenance comments are retained, not instructions to enable native.
SHA256: `698eeacf91eb9c27b48ed0364d337a57b15a3c583703be313e519cd3a613acc6`.
It omits `execution.checkpointing.interval` and preserves the memory/GC settings.
Both JM and every TM need the environment map in
[fullopt-lc-environment.yaml](../../skills/nexmark-bench-run/assets/fullopt-lc-environment.yaml).
In particular:

```yaml
CACHEKIT_VALUE_EVICTION_WRITE_BATCH_ENABLED: "true"
CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED: "true"
```

These are environment/JVM switches, not Flink YAML keys. Explicit JVM properties
override environment values; remove conflicting flags and restart owned JVMs.
Native stages, Chen and unrelated experimental paths stay off.

The existing reference harness fixes:

| Setting | Value |
|---|---|
| Events / rounds | 100000000 / 1 |
| Queries | q4,q5,q8,q9,q11,q18,q19,q20,q3,q7,q12,q13,q15,q16,q17 |
| Topology | 1 JM; 2 TM containers × 4 JVMs × 2 slots |
| TM1 CPUs | 38,40,42,44,46,48,50,52 |
| TM2 CPUs | 56,58,60,62,64,66,68,70 |
| JM / Prometheus / Pushgateway CPUs | 54 / 72 / 74 |
| Memory NUMA node | 0, enforced by the harness |
| TM process memory | 8g per JVM, not per container |
| CPU accounting | two disjoint container-PID1 receiver roots; eight sender identities for coverage |

Do not infer 16 consumed cores from 16 slots. Keep the patched collector and the
archive's `check_cpu_metric_ownership.py` and capacity checks. The generic
eight-recursive-TM-tree checker alone is inappropriate for this repaired receiver
topology. No automatic 1.75 correction is valid.

## 3. Prepare a fresh campaign, without running it

On the archive host, after copying the rebuilt runtime there:

```bash
python3 reproduction/fullopt-lc/prepare_campaign.py \
  --archive /home/wuql/flink-cluster/experiments/cachekit-fullopt-p29-p30-100m-kunpeng-20260908 \
  --runtime /absolute/path/to/new-rebuilt-runtime \
  --output /home/wuql/flink-cluster/experiments/cachekit-dev-lc-reproduction-NEW \
  --project cklcreproNEW --port-base 13980
```

Replace `NEW` with a unique lowercase alphanumeric suffix (including in the project).
Requires Python3 + PyYAML. This copies inputs and harness, not old query results,
remaps project/paths/ports, mounts all three rebuilt JARs on all JM/TMs, updates
artifact checksums and records the build audit. It **does not launch Docker**.
The current build commit is recorded in the generated identity, runtime manifest
and harness; the historical reference is retained separately.

For a future authorized run, first inspect free ports, foreign jobs, CPU/NUMA
occupancy and all bind mounts, then validate the generated Compose with the
host's `/home/wuql/bin/docker-compose ... config -q`. Wait while the protected
other-user experiment is running even on another NUMA node. Only then invoke
the printed `bash .../run_campaign.sh` command. Do not execute the old campaign
in place, modify its results, or bypass a failing ownership/runtime audit.

This publication does not perform that run. A future result must independently
verify actual driver `EventsNum=100000000`, runtime JAR hashes and P30 activation.
Original P29 batch keys were zero, so the result is not proof of a P29 benefit.

## Verification and reporting

See [VALIDATION.md](VALIDATION.md) for this integration's static/unit-test evidence.
The raw historical evidence and result tables remain on the experiment branch:
https://github.com/Micuks/flink_statebackend_cached/tree/wuql/p29-p30-fullopt-100m-20260908/experiments/cachekit-fullopt-p29-p30-100m-20260908

Use the canonical comparison reporting reference. Average per-query percentages
equally, not ratios of summed throughput; report raw K/s, actual cores and K/s/core.
Reproducing the runtime/config identity does not eliminate host noise or historical
baseline differences and does not establish a new measured +65% result.
