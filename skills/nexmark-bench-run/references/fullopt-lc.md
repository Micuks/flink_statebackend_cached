# FullOpt+LC: configuration and usage

**FullOpt+LC = portable FullOpt + P29 + P30.** Use exactly `FullOpt+LC` in new
reports; `FullOpt+P29+P30` is the historical alias, not a different treatment.
LC means Lazy Copy. This is a source-level enhancement, not RocksDB compression
tuning. Native stages and Chen remain OFF.

## What changes

- P29: synchronous dirty ValueState eviction WriteBatch at LRU overflow; retain
  the single-write fallback and propagate failure without publishing clean state.
- P30: `BinaryStringData.copy()` returns a fresh wrapper around an immutable Java
  String only while no binary section exists. Binary-backed copies retain deep
  byte-copy semantics. This is a Flink row-copy optimization, not proof of a
  CacheKit state-access improvement.
- Both gates default OFF. Runtime artifacts must contain the implementation:
  tested source `7b95de31e51a6ee6fec4e4b1f625dc6538960d2c` in
  `Micuks/flink_statebackend_cached`; campaign and reports at `78f1c5409a`.

## Configuration

Use all portable FullOpt settings from
[optimization definitions](cachekit-optimization-definitions.md#portable-fullopt).
The supplied [YAML fragment](../assets/fullopt-lc.yaml) contains those treatment
settings; merge it into a host-specific configuration without duplicate keys.
It is not a complete cluster config: retain memory, slots, paths, reporters and
network settings from the matched control. For no-checkpoint performance omit
`execution.checkpointing.interval`. MultiGet minimum 64 is a reference value,
not a hard requirement; record any change and keep controls matched.

Apply [the environment map](../assets/fullopt-lc-environment.yaml) to **each JM
and TM service's `environment` mapping**. The two positive gates are:

```yaml
CACHEKIT_VALUE_EVICTION_WRITE_BATCH_ENABLED: "true"
CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED: "true"
```

These are NOT `flink-conf.yaml` keys. Setting shell variables alone does not
propagate them into containers. Equivalent JVM options are:

```text
-Dcachekit.value.eviction-write-batch.enabled=true
-Dcachekit.binary-string.lazy-copy.enabled=true
```

JVM properties take precedence over environment settings; remove conflicting
options. Apply before JVM startup and restart the owned cluster after changes.
The old Flink-prefixed LC names are not runtime aliases. Rebuild the runtime when
migrating: old archived JARs still require their historical option names. The
locked reproduction preparation tool migrates archived configuration for new JARs.
For plain FullOpt controls set both gates explicitly false and match all other
settings. Leave unrelated exploratory gates off as the environment map specifies.

## Usage and artifact checks

The required delivery is [one CacheKit fat JAR](single-fat-jar-delivery.md),
including P30 and its nested classes. The older multi-JAR recipe is historical
archive reconstruction only, not the default packaging or delivery path.

On `cachekit/dev`, use the supported
[single-JAR build and historical reconstruction appendix](../../../../reproduction/fullopt-lc/README.md).
It includes the implementation, exact full config, source/class/JAR lock, rebuild
verifier and an isolated prepare-only campaign tool. It requires the archived
runtime dependencies; a full-tree distribution is not asserted equivalent.
Publication was compile/unit/bytecode verified without a new performance run.

1. Use an isolated checkout containing the tested source (or audited descendants).
   Build/package the CacheKit, RocksDB state-backend, and Flink table-common
   changes into one CacheKit fat JAR. An old CacheKit JAR without BinaryStringData
   cannot enable P30; the corrected packaging includes it.
2. Deploy the single CacheKit fat JAR to all JM/TM JVMs and verify class ownership.
   For historical context only, the earlier verified campaign
   overlaid CacheKit classes in the CacheKit JAR, RocksDB ValueState/interface
   classes in `flink-dist`, and BinaryStringData classes in the table API uber
   JAR. Merely dropping a second table-common JAR into `lib/` is not sufficient:
   check classpath ownership, source revision and hashes of the loaded JARs.
3. Render the composed configuration, archive YAML/environment/JVM flags and JAR
   hashes, then launch only on idle allocated CPU and memory NUMA nodes. If the
   user requires waiting for wutb on Kunpeng, that takes precedence over coexistence.
4. Run the benchmark with the custom merged profile, e.g. from the benchmark
   workspace after runtime deployment:

   ```bash
   python -m nexmark_bench.orchestrator --mode cached --rounds 1 \
     --config-file /absolute/path/to/merged-fullopt-lc.yaml \
     -q q4,q5,q8,q9,q11,q18,q19,q20,q3,q7,q12,q13,q15,q16,q17
   ```

   The command illustrates profile selection, not a complete 100M recipe.
   Configure event count through the checked-out runner's supported interface
   and verify actual driver `EventsNum=100000000`; do not infer it from a label.
   Perform CPU ownership/capacity checks in the parent skill before per-core claims.
5. Verify current-leg activation independently: `[FLINK LAZY STRING COPY]`
   positive observed copies proves P30 execution; `[CACHEKIT EVICTION WRITE BATCH]`
   positive batch keys proves P29 execution. Enabled flags alone prove neither.
   Copy counters are sampled lifecycle lower bounds, including warmup.

For the exact archived 100M/no-checkpoint experiment, use the source repository's
`experiments/cachekit-fullopt-p29-p30-100m-20260908/README.md` and harness. Those
staging scripts require the named historical runtime archives and tested build
artifacts; they are not a standalone fresh-machine bootstrap. Do not rerun
completed campaigns or reuse occupied ports/projects merely to apply this name.

## Evidence and reporting boundary

The 2026-09-08 campaign completed 15/15 on each host, reusing historical FullOpt
and RocksDB. Equal-weight per-query uplift versus FullOpt: x86 +13.26%, Kunpeng
+15.60%. Versus RocksDB: x86 -11.34%, Kunpeng +65.48%. P30 was observed in all
30 new legs; P29 batch keys were zero throughout. These are historical-control
comparisons, not isolated P29/P30 causal attribution or a future speedup guarantee.

For this three-arm report use the requested order:

```text
Query/Group | RocksDB | FullOpt | FullOpt+LC | FullOpt uplift vs RocksDB | FullOpt+LC uplift vs RocksDB | FullOpt+LC uplift vs FullOpt
```

Keep per-query rows, large8/small7/15Q groups, raw K/s, cores, K/s/core, activation
evidence, negative values and baseline provenance. Preserve historical raw names
and hashes; add the alias only at the presentation/metadata layer.
