# CacheKit fullOpt — benchmark handoff runbook

Reproduce the CacheKit fullOpt (D1 + bp-prefetch + local-preagg) nexmark 15q
**CDC correctness** and **throughput** results on a fresh server.

- Code: GitHub `Micuks/flink_statebackend_cached`, branch **`cachekit/dev`**
  (= `merge/cachekit-fullopt-objresident` + D1/bp/preagg; merge-base `a9e86f47bd`).
- Expected: **15/15 CDC** (q12 = proc-time, SKIP) · **+32.43% throughput/core** mean
  (50M×3, quiet host; +34.72% excl. q12).

The fullOpt stack spans **three** Flink modules — this is the #1 thing people get
wrong. You must build and stage all three, not just the cachekit jar.

---

## 0. Prerequisites

- Linux + Docker + Docker Compose, JDK 8/11, ~32 GB free RAM for one 16-slot stack.
- The Nexmark harness repos laid out as siblings (see the `nexmark-bench-run` skill
  for a fresh-machine bootstrap). You need:
  - `nexmark-flink/` (query SQL + runners + `data_sources/`)
  - `nexmark_bench/` (Python orchestrator: `nexmark_bench.orchestrator`, profiles)
  - the base Flink image **`flink-cluster-jobmanager:latest`** (built once from
    `nexmark_bench/deploy/docker-compose.yml`)
  - `lib/flink-dist-1.16.3.jar` and `lib/flink-table-runtime-1.16.3.jar` (the FULL
    ~117 MB / ~3 MB stock jars — keep a pristine copy of each; see staging).
- A `$WORKSPACE` root holding `lib/`, `config/`, `nexmark-flink/`, `nexmark_bench/`.

---

## 1. Get the code

```bash
cd $WORKSPACE
git clone https://github.com/Micuks/flink_statebackend_cached cachekit-src
cd cachekit-src
git checkout cachekit/dev
```

## 2. Build the three fullOpt modules

`-DskipTests`, **not** `-Dmaven.test.skip=true` (the latter makes `flink-clients`'
test-jar assembly fail with "You must set at least one file").

```bash
cd $WORKSPACE/cachekit-src
./mvnw -o -pl flink-state-backends/flink-statebackend-cachekit,\
flink-streaming-java,flink-table/flink-table-runtime \
  -am package -DskipTests
```

Produces:
- `flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar`
- `flink-streaming-java/target/classes/org/apache/flink/streaming/...` (bp-prefetch + local-preagg)
- `flink-table/flink-table-runtime/target/classes/.../GroupAggFunction.class`, `RowTimeDeduplicateFunction.class` (local-preagg fold)

## 3. Stage the jars into `lib/`  ⚠️ the tricky part

fullOpt modifies classes in **flink-streaming-java** and **flink-table-runtime**,
which live inside `flink-dist` / the full `flink-table-runtime` jar. You must PATCH
the modified classes into the FULL jars — do **not** copy the thin `*-SNAPSHOT.jar`
module outputs over the full runtime jars (the thin `flink-table-runtime-1.16-SNAPSHOT.jar`
is ~76 KB, shade-filtered, and does **not** even contain GroupAggFunction; and
`flink-dist` bundles **zero** `org/apache/flink/table/runtime/` classes, so a thin
replace yields ClassNotFound at runtime).

Keep pristine copies of the stock jars once (`*.orig`), then patch working copies:

```bash
WS=$WORKSPACE ; SRC=$WS/cachekit-src ; LIB=$WS/lib
[ -f $LIB/flink-dist-1.16.3.jar.orig ]         || cp $LIB/flink-dist-1.16.3.jar          $LIB/flink-dist-1.16.3.jar.orig
[ -f $LIB/flink-table-runtime-1.16.3.jar.orig ]|| cp $LIB/flink-table-runtime-1.16.3.jar $LIB/flink-table-runtime-1.16.3.jar.orig

# 3a. flink-dist  = stock + patched streaming classes (bp-prefetch + local-preagg)
cp $LIB/flink-dist-1.16.3.jar.orig $LIB/flink-dist-1.16.3.jar
jar uf $LIB/flink-dist-1.16.3.jar -C $SRC/flink-streaming-java/target/classes org/apache/flink/streaming

# 3b. flink-table-runtime = FULL stock jar + the two patched classes (NOT the thin jar)
cp $LIB/flink-table-runtime-1.16.3.jar.orig $LIB/flink-table-runtime-1.16.3.jar
TRC=$SRC/flink-table/flink-table-runtime/target/classes
jar uf $LIB/flink-table-runtime-1.16.3.jar \
  -C $TRC org/apache/flink/table/runtime/operators/aggregate/GroupAggFunction.class \
  -C $TRC org/apache/flink/table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.class

# 3c. cachekit backend = the module jar as-is (full, ~90 KB, has D1 + bp)
cp $SRC/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
   $LIB/flink-statebackend-cachekit-1.16-SNAPSHOT.jar

# sanity
jar tf $LIB/flink-table-runtime-1.16.3.jar | grep GroupAggFunction.class   # must print
```

The compose mounts these three from `$LIB` into `/opt/flink/lib/` over the image's copies.

## 4. Flink conf (fullOpt)

`conf/flink-conf-cachekit-example.yaml` in the repo is the canonical version.
Effective settings (drop the whole `state.backend.cached.*` block — that is the OLD
`CachingStateBackendFactory` prefix and is **silently ignored** by CacheKit):

```yaml
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory
state.backend.incremental: true
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.fixed-per-slot: 1024m

# D1 ValueState object cache (VoidNamespace-gated). value.cache.max-entries=0 disables it.
state.backend.cachekit.cache.void-namespace-only: true
state.backend.cachekit.value.cache.max-entries: 8000
state.backend.cachekit.value.cache.policy: LRU
state.backend.cachekit.value.cache.lru.overflow: 1024
state.backend.cachekit.value.bypass.enabled: true
state.backend.cachekit.value.hit-rate.threshold: 0.03
state.backend.cachekit.value.hit-rate.window: 5000

# MapState caches OFF — the wrappers regress joins (q3/q4/q9/q20) and q13.
state.backend.cachekit.map.cache.max-entries: 0
state.backend.cachekit.map.snapshot.cache.max-entries: 0
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.map.bypass.enabled: false

# bp-prefetch + mailbox-batch + runtime local pre-aggregation
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: 64
state.backend.cachekit.bp-prefetch.backpressure-gated: true
state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: 4096
state.backend.cachekit.mailbox-batch.timeout-us: 0
state.backend.cachekit.local-preagg.enabled: true

table.exec.mini-batch.enabled: false
table.optimizer.distinct-agg.split.enabled: true
```

Baseline = a plain `state.backend: rocksdb` conf (see `nexmark_bench/profiles/flink-conf-rocksdb.yaml`).

Topology knobs differ by test:
- **Perf**: `parallelism.default: 16`, `taskmanager.numberOfTaskSlots: 2`.
- **CDC**: `parallelism.default: 1` + a **single-split source** (see §5).

---

## 5. CDC correctness (15q)

Compare CacheKit(fullOpt) vs RocksDB terminal state after CDC replay.

⚠️ **Windowed queries need a single-split source.** The stock `nexmark-events-1000000`
is 4 part files; at p=1 the FileSource assigns splits in a non-deterministic order, so
watermark progression (and late-event drop under the `-4s` watermark) varies run-to-run
— **rocksdb-vs-rocksdb itself diverges** on q5/q7/q8/q11. Use a single file < 128 MB
(one split → deterministic). The stock `nexmark-events-100000` is a single 46 MB file
and is deterministic; or concatenate the parts in time order into one file.

Per query, run both backends and diff:
```bash
# leg output dirs are nexmark-flink/results/<q>_rocksdb_results_p1 (–backend is only a label)
# run rocksdb leg (rocksdb conf mounted), copy sink; run cachekit leg (fullopt conf), copy sink; then:
python3 -m nexmark_bench.correctness_orchestrator pair <rocksdb_dir> <cachekit_dir> --query <q>
# PASS = "SUCCESS: Final states identical after CDC replay"
```
Queries: `q4 q5 q8 q9 q11 q18 q19 q20 q3 q7 q12 q13 q15 q16 q17`. **q12 is proc-time
(wall-clock windows) → non-deterministic → SKIP.** Expect 14/14 (+q12 skip) = 15/15.
q13 needs `nexmark-side-input/side_input.txt` mounted at `/opt/flink/data`.

---

## 6. Throughput (15q)

Use the repo runner. **Quiet host only** — on a loaded machine throughput is
noise-dominated (contention exaggerates ~70%+); always **interleave** legs (the runner
already runs rocksdb then cachekit-bp back-to-back per query).

```bash
cd $WORKSPACE
BENCH_ROOT=$WORKSPACE PROJECT=ck-bench COMPOSE_PROJECT_NAME=ck-bench \
JM=http://localhost:18130 PROM=http://localhost:19830 PUSHGATEWAY_PORT=19831 \
EVENTS_LIST=50000000 WARMUP_EVENTS=5000000 ROUNDS=3 \
QUERIES=q3,q4,q5,q7,q8,q9,q11,q12,q13,q15,q16,q17,q18,q19,q20 \
NO_MAP_CACHE=1 SKIP_DOCKER_BUILD=1 SKIP_BUILD=1 \
OUT=$WORKSPACE/results-fullopt/$(date +%Y%m%d_%H%M%S) \
bash $WORKSPACE/cachekit-src/scripts/run_cachekit_bp_prefetch_perf.sh
```
- `SKIP_BUILD=1` uses your §3 staged jars (recommended — the runner's own
  `build_cachekit_bp_prefetch_artifacts.sh` does the thin-table-runtime replace §3 warns about).
- `SKIP_DOCKER_BUILD=1` uses the pre-built image + mounted jars (no image build).
- Metric = **throughput/core** (`results[0]`). Note the **q15 per-core artifact**: q15's
  rocksdb leg samples 1–2 cores so per-core is inflated (+318%); the wall-time (740s→121s)
  and total TPS (~12×) are the honest signal. Report per-core **and** wall.
- Comparison table lands in `$OUT/*.md`.

### Isolation (running alongside other stacks)
If the host is not exclusively yours:
- Dedicated `PROJECT`/ports (as above) and a dedicated `BENCH_ROOT` — but note the
  orchestrator writes the effective conf to `<nexmark_bench parent>/config/flink-conf.yaml`
  (path derives from the module's `__file__`, not cwd). To isolate it, point `BENCH_ROOT`
  at a dir whose `nexmark_bench` is a **symlink** and run with `PYTHONPATH=$BENCH_ROOT`
  (abspath keeps the symlink path → `script_dir` = your dir → isolated config).
- Use **real-copy** lib jars in your `BENCH_ROOT/lib` (never symlinks) — the staging in
  §3 patches jars in place and would otherwise corrupt the shared lib / other stacks.
- The gate/smoke pattern (`.ck-perf-root/gate_perf.sh`) waits for `load1<4 & flink-JM<=2`
  sustained, smoke-tests one query (25-min cap) to catch a non-submitting harness before
  committing a whole night, then runs the full suite.

---

## 7. Gotchas cheat-sheet

| Symptom | Cause / fix |
|---|---|
| `flink-clients` assembly "must set at least one file" | used `-Dmaven.test.skip=true`; use `-DskipTests` |
| Runtime ClassNotFound in table/runtime, or local-preagg not firing | thin `flink-table-runtime-1.16-SNAPSHOT.jar` staged; patch the FULL jar (§3b) |
| Joins q3/q4/q9/q20 fail CDC | MapState cache on; set `map.cache.max-entries: 0` |
| Windowed q5/q7/q8/q11 differ (even rocksdb-vs-rocksdb) | multi-split source; use single-split <128 MB source at p=1 |
| q12 "fails" CDC | proc-time windows, inherently non-deterministic → SKIP |
| D1 won't turn off via `value.bypass.enabled:false` | that only disables adaptive bypass; D1's switch is `value.cache.max-entries` (0 = off) or `cache.void-namespace-only` |
| `state.backend.cached.*` seems ignored | it is — wrong prefix (old CachingStateBackend); CacheKit reads `state.backend.cachekit.*` |
| Job never appears in JM (0 running jobs) | host too loaded / harness couldn't submit; run on a quiet host, smoke-test first |
```
