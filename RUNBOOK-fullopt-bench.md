# CacheKit fullOpt — benchmark handoff runbook

Reproduce the CacheKit fullOpt (bp-prefetch + local-preagg) nexmark 15q
**CDC correctness** and **throughput** results on a fresh server.

- Code: GitHub `Micuks/flink_statebackend_cached`, branch **`cachekit/dev`**.
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
  - `lib/flink-dist-1.16.3.jar` and `lib/flink-table-runtime-1.16.3.jar` — the
    **stock** jars, used as-is (CacheKit ships as a single jar; see §2–3).
- A `$WORKSPACE` root holding `lib/`, `config/`, `nexmark-flink/`, `nexmark_bench/`.

---

## 1. Get the code

```bash
cd $WORKSPACE
git clone https://github.com/Micuks/flink_statebackend_cached cachekit-src
cd cachekit-src
git checkout cachekit/dev
```

## 2. Build the single CacheKit jar

`-DskipTests`, **not** `-Dmaven.test.skip=true` (the latter makes `flink-clients`'
test-jar assembly fail with "You must set at least one file").

```bash
cd $WORKSPACE/cachekit-src
./mvnw -o -pl flink-state-backends/flink-statebackend-cachekit -am package -DskipTests
```

Produces one deployable artifact, ~2.7 MB:

`flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar`

It already contains everything CacheKit needs:

- its own backend/state/cache classes;
- the 22 classes it needs from **flink-streaming-java**,
  **flink-statebackend-rocksdb** and **flink-table-runtime** — some new
  (bp-prefetch, local-preagg, batch plumbing), some overriding an upstream class
  with an additive hook (`OneInputStreamTask`, `StreamOneInputProcessor`,
  `GroupAggFunction`, `RowTimeDeduplicateFunction`, four RocksDB classes);
- `caffeine` and `fastutil`, shaded under
  `org.apache.flink.contrib.streaming.state.cachekit.shaded.*`.

Why the overrides take effect: Flink builds its classpath from a **sorted**
`lib/*.jar` and appends `flink-dist` **last**. `flink-statebackend-cachekit-…jar`
therefore precedes both `flink-table-runtime-…jar` and `flink-dist-…jar`, and
class loading is first-wins.

## 3. Stage into `lib/`

```bash
WS=$WORKSPACE ; SRC=$WS/cachekit-src ; LIB=$WS/lib
cp $SRC/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
   $LIB/flink-statebackend-cachekit-1.16-SNAPSHOT.jar
```

That is the whole staging step. `flink-dist-1.16.3.jar` and
`flink-table-runtime-1.16.3.jar` stay **stock** — no `jar uf` patching, no
`*.orig` copies to keep straight.

```bash
# sanity: the override classes are in the one jar
unzip -l $LIB/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
  | grep -E "OneInputStreamTask.class|GroupAggFunction.class"   # must print both
```

> Historical note: fullOpt used to be staged by `jar uf`-patching the modified
> classes into a copy of `flink-dist` (117 MB) plus the full
> `flink-table-runtime` jar, because the thin module jars are shade-filtered
> (`flink-table-runtime-1.16-SNAPSHOT.jar` is ~76 KB and does not even contain
> `GroupAggFunction`, and `flink-dist` bundles zero
> `org/apache/flink/table/runtime/` classes, so a thin replace yielded
> ClassNotFound). Runs recorded before 2026-08-07 were staged that way. The
> single jar carries a verified superset of those patched classes, so the
> deployed bytecode is equivalent.

The compose mounts `$LIB` into `/opt/flink/lib/` over the image's copies.

## 4. Flink conf (fullOpt)

`conf/flink-conf-cachekit-example.yaml` in the repo is the canonical version.
Effective settings (drop the whole `state.backend.cached.*` block — that is the OLD
`CachingStateBackendFactory` prefix and is **silently ignored** by CacheKit):

```yaml
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory
state.backend.incremental: true
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.fixed-per-slot: 1024m

# ValueState cache. Set value.cache.max-entries to 0 to disable it.
state.backend.cachekit.cache.void-namespace-only: true
state.backend.cachekit.value.cache.max-entries: 8000
state.backend.cachekit.value.cache.policy: LRU
state.backend.cachekit.value.cache.lru.overflow: 1024
state.backend.cachekit.value.bypass.enabled: true
state.backend.cachekit.value.hit-rate.threshold: 0.03
state.backend.cachekit.value.hit-rate.window: 5000

# MapState caches are optional. Set an individual capacity to 0 to disable that cache.
state.backend.cachekit.map.cache.max-entries: 8000
state.backend.cachekit.map.snapshot.cache.max-entries: 2000
state.backend.cachekit.map.presence.cache.max-entries: 0
state.backend.cachekit.map.bypass.enabled: true

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
SKIP_DOCKER_BUILD=1 SKIP_BUILD=1 \
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
- Do **not** preserve Flink checkpoint/local-state runtime directories as benchmark artifacts.
  Keep only `results/...` outputs (`summary.csv`, `query_status.csv`, `input-snapshot/`, and
  query logs needed for failures). The per-query `runtime/data/<run>/round-N/qX/.../checkpoint`
  tree can easily reach tens of GB for state-heavy queries such as q9/q20 and must be deleted
  after the CSV/log artifacts have been copied.

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

## 7. q4/q16 cancellation-time native crash (fixed 2026-07-16)

The throughput harness cancels the warmup job after roughly 120 seconds. The previous CacheKit
fullOpt build could crash TaskManager JVMs in `rocksdb::GetColumnFamilyID(ColumnFamilyHandle*)`
during that teardown, invalidating q4/q16 before their measured query could finish.

The failing path is:

```text
StreamTask cancel thread
  -> CacheKitKeyedStateBackend.close()
  -> flushWrappers()
  -> CachedInternalValueState.flushEntryToDelegate()
  -> RocksDB.put() on a disposed ColumnFamilyHandle
```

This is a CacheKit close/dispose lifecycle race, not a host-memory or MapState-cache failure.
`dispose()` could free delegate RocksDB handles while `close()` flushed dirty ValueState entries.
The fix serializes terminal backend operations, quiesces ValueState async prefetch and dirty
write-back, and closes MapState dirty write-back before native delegate disposal. Keep the
final-close flush: it persists dirty cache entries; the required invariant is that it completes
before `delegate.dispose()`.

**Validation (2026-07-16):** JDK 11 CacheKit module tests pass (56/56). The exact fullOpt config
with MapState cache, bp-prefetch, mailbox batch, and local-preagg enabled then passed one 100M
round for both q4 (258.433s, 386.95K/s) and q16 (517.146s, 193.37K/s). Both warmup cancellations
completed normally, and the result tree contained no `hs_err_pid*.log`, `SIGSEGV`, or TaskManager
failure signature.

The old q4/q16 failure result remains invalid. This confirms the two former blockers only; rerun
the full 15-query suite before publishing a replacement aggregate result.

---

## 8. MapState iterator removal cache coherence (fixed 2026-07-18)

Older CacheKit builds could use separate delegate iterators for traversal and
`Iterator.remove()`. On `entries()` this could fail before the removal reached RocksDB; on a
successful removal, Value/Presence cache entries could still report the deleted key as present.
With snapshot cache disabled, the cache-filling iterator could also throw
`UnsupportedOperationException` from `remove()`.

The `cachekit/dev_wutb` patch uses one underlying iterator for `next()` and `remove()`, records a
clean negative Value cache entry and `presence=false` after a physical iterator deletion, and
invalidates the affected MapSnapshot. Snapshot `SINGLE` hits now return an iterator whose
`remove()` follows the ordinary MapState write-back path. This applies even when iteration cache
fill is disabled, without enabling cache fill itself.

Do not disable MapState cache as a workaround for this issue. Build and stage the CacheKit jar
from this branch using §3. **Validation (2026-07-18):** JDK 11 CacheKit module tests pass
61/61, including the five iterator-removal regressions.

---

## 9. Gotchas cheat-sheet

| Symptom | Cause / fix |
|---|---|
| `flink-clients` assembly "must set at least one file" | used `-Dmaven.test.skip=true`; use `-DskipTests` |
| Runtime ClassNotFound in table/runtime, or local-preagg not firing | thin `flink-table-runtime-1.16-SNAPSHOT.jar` staged; patch the FULL jar (§3b) |
| MapState cache needs isolation | Tune its capacities per workload; set an individual capacity to `0` only when isolating that cache |
| `MapState.entries()/iterator()` removal fails or returns deleted data later | old CacheKit iterator wrapper; build and stage `cachekit/dev_wutb` (§8) |
| q4/q16 crash while warmup stops | CacheKit close/dispose race in an old build; use the lifecycle fix in §7 and confirm the three staged jars |
| Windowed q5/q7/q8/q11 differ (even rocksdb-vs-rocksdb) | multi-split source; use single-split <128 MB source at p=1 |
| q12 "fails" CDC | proc-time windows, inherently non-deterministic → SKIP |
| `state.backend.cached.*` seems ignored | it is — wrong prefix (old CachingStateBackend); CacheKit reads `state.backend.cachekit.*` |
| Job never appears in JM (0 running jobs) | host too loaded / harness couldn't submit; run on a quiet host, smoke-test first |
| RocksDB `No space left on device` / huge runtime dir | stale `runtime/data/<run>/.../checkpoint` trees were preserved; checkpoint/local-state runtime data is not a result artifact and should be cleaned before the next baseline |
```
