---
name: nexmark-bench-run
description: Run Nexmark SQL or JMH benchmarks with Micuks/nexmark-bench, including UniVPN/SSH access to Kunpeng, fresh-machine bootstrap, the 1 JM + 2 TM containers × 4 JVMs × 2 slots topology, parallel Compose units, state-backend profiles, canonical CacheKit FullOpt / Native FullOpt / Stage1 / Stage2 / Stage3 identities, branch switching, comparison, and troubleshooting. Use when accessing Kunpeng, setting up or restarting the Flink cluster, running query sweeps or JMH, configuring or auditing named optimization stacks, comparing backends, or diagnosing benchmark failures.
metadata:
  short-description: Run Nexmark / JMH benchmarks on the nexmark-bench Flink cluster
---

# Flink Benchmark Operator Playbook

Everything lives in **one repo**: `github.com/Micuks/nexmark-bench`. No separate deployment repo.

## Kunpeng host access from a new session

The Kunpeng benchmark host is `root@173.154.10.2` and is reachable through
UniVPN. Start every new session with the bounded status checks below:

```bash
VPN=/mnt/data1/wuql/.claude/skills/univpn-connect

"$VPN/scripts/univpn.sh" status
"$VPN/scripts/univpn.sh" check
```

If the output already contains `result=connected`, do not reconnect. Confirm
that the target route uses the VPN interface, then log in:

```bash
ip route get 173.154.10.2
ssh root@173.154.10.2
```

The route must show `dev cnem_vnic`. If VPN is not connected, run:

```bash
"$VPN/scripts/univpn.sh" connect
```

Follow the script prompt and enter a fresh OTP; do not automate the GUI. After
connection, rerun `status`, `check`, and `ip route get 173.154.10.2` before
SSH. Never put the SSH password or OTP in the skill, shell command arguments,
environment variables, files, or logs.

## 0. Fresh-machine bootstrap

```bash
WORKSPACE=$(pwd)   # any empty dir
git clone git@github.com:Micuks/nexmark-bench.git nexmark_bench
./nexmark_bench/scripts/bootstrap.sh
python -m nexmark_bench.orchestrator --mode comparison --rounds 3 -q q5
```

What `bootstrap.sh` does, in order:

1. Clones sibling source repos into `$WORKSPACE`: `OmniStateStore`, `flink-benchmarks`, `flink-statebackend-cached`, `nexmark` (and `nexmark-flink` either symlinked from inside `nexmark/` or cloned separately).
2. `mvn clean package -DskipTests` on each; `cmake --build` for Falcon native lib.
3. Collects `*.jar` and `libfalcon.so` into `$WORKSPACE/lib/`.
4. Downloads `flink.tgz` into `$WORKSPACE/flink.tgz` (Flink 1.16.3 by default).
5. Symlinks `Dockerfile`, `entrypoint.sh`, `prometheus.yml`, all `docker-compose*.yml`, and `config/workers` from `nexmark_bench/deploy/` into `$WORKSPACE` root.
6. Seeds `$WORKSPACE/config/flink-conf.yaml` from `nexmark_bench/profiles/flink-conf-rocksdb.yaml`.
7. `docker compose build` the Flink image.

Override anything via env:

```
WORKSPACE=/custom/path
OMNISTATE_BRANCH=unit4-mapstate-opt
FLINK_BENCH_BRANCH=flink-1.16
CACHEKIT_BRANCH=mapstate-iter-cache-toggle
NEXMARK_BRANCH=master
FLINK_VERSION=1.16.3
SKIP_CLONE=1  SKIP_BUILD=1  SKIP_DOCKER=1  SKIP_LINKS=1  SKIP_FLINK_DL=1
```

## 1. Workspace layout after bootstrap

```
$WORKSPACE/
├── nexmark_bench/              # the single repo (Micuks/nexmark-bench)
│   ├── *.py                    # python package
│   ├── profiles/*.yaml         # flink-conf profiles (source of truth)
│   ├── deploy/                 # Dockerfile, compose, entrypoint, prometheus, workers
│   ├── scripts/                # bootstrap.sh, collect_flink_diagnostics.py
│   ├── tests/
│   ├── pyproject.toml, uv.lock
│   └── .git/
├── Dockerfile → nexmark_bench/deploy/Dockerfile          (symlink)
├── docker-compose.yml → nexmark_bench/deploy/...         (symlink)
├── docker-compose-{baseline,unit1,unit23,unit4}.yml      (symlinks)
├── entrypoint.sh → nexmark_bench/deploy/entrypoint.sh    (symlink)
├── prometheus.yml → nexmark_bench/deploy/prometheus.yml  (symlink)
├── config/
│   ├── workers → ../nexmark_bench/deploy/workers         (symlink)
│   └── flink-conf.yaml                                    (bootstrap-seeded; overwritten per run)
├── lib/                        # built jars + libfalcon.so (bootstrap output)
├── flink.tgz                   # Flink 1.16.3 tarball (bootstrap output)
├── OmniStateStore/             # sibling source repo (bootstrap clone)
├── flink-benchmarks/           # sibling source repo (bootstrap clone)
├── flink-statebackend-cached/  # sibling source repo (bootstrap clone)
├── nexmark/                    # sibling source repo (bootstrap clone)
├── nexmark-flink/              # driver scripts (symlinked from nexmark/)
├── results/                    # run output
├── flamegraphs/                # run output (if --flamegraph)
├── metrics/                    # run output (rocksdb charts)
└── benchmark_progress.json     # resume state
```

## 2. Default Cluster Topology

The Flink image's `entrypoint.sh` fans each TM container out to 4 TM JVMs (`taskmanager.sh start` loop + a final `start-foreground`), **not** one. See `nexmark_bench/deploy/entrypoint.sh:76-88`.

> **CPU metric integrity:** the historical loop-plus-final-foreground entrypoint can
> make the foreground TM (container PID 1) the OS parent of the three daemon TMs.
> `CpuMetricSender` builds a recursive `ProcfsBasedProcessTree` for every TM PID,
> so that topology counts three TMs twice and inflates CPU by about `7/4` under
> balanced load. The 2x4 topology is valid for throughput, but it is **not valid
> for cores or throughput/core until the process trees are disjoint**.

| Layer | Count | Source |
|---|---|---|
| JobManager containers | **1** | `docker-compose.yml` service `jobmanager` |
| TaskManager containers | **2** | `taskmanager1`, `taskmanager2` |
| TaskManager JVMs per container | **4** | `entrypoint.sh` loop |
| Task slots per JVM | **2** | `flink-conf.yaml → taskmanager.numberOfTaskSlots` |
| **TMs Flink sees at `/overview`** | **8** | 2 containers × 4 JVMs |
| **Total task slots** | **16** | 8 JVMs × 2 slots |

Ports:
- JobManager REST: **8081**
- Prometheus: **9099** (container 9090)
- Pushgateway: **9092** (container 9091)
- Flink metrics reporter (scraped): 9249 inside each JM/TM

`BENCH_EXPECTED_TMS=8` is the **correct** default (matches `/overview.taskmanagers`). If you change the JVM fan-out, update it:

```bash
export BENCH_EXPECTED_TMS=8           # default topology (2 × 4)
export BENCH_EXPECTED_TMS=4           # if you edit entrypoint.sh to 2 JVMs/container
```

### Mandatory CPU-integrity preflight

After the TM containers are running and before warmup, run:

```bash
python ~/.codex/skills/nexmark-bench-run/scripts/check_cpu_metric_process_trees.py \
  --compose "${BENCH_COMPOSE_PATH:-$WORKSPACE/docker-compose.yml}" \
  --expected-tms "${BENCH_EXPECTED_TMS:-8}"
```

If the stack uses an explicit Compose project name, also pass
`--project-name "$COMPOSE_PROJECT_NAME"`.

For any result that reports `cores`, `K/s/core`, or per-core uplift, this check
must pass. A failure means recursive TM process trees overlap; do not publish or
compare per-core numbers from that run. Do not repair historical rows by blindly
dividing by `1.75`: the exact bias depends on per-TM load balance.

Repair either the launch topology (all TM JVMs are siblings beneath a non-TM
supervisor/PID 1) or the collector (assign every process to exactly one monitored
TM root). Then rerun the preflight and the CPU-capacity validation described in
[CPU metric integrity](references/cpu-metric-integrity.md).

## 3. Parallel-Unit Compose Variants

Port-offset stacks so multiple configurations run concurrently on the same host:

| Compose file | JM REST | Prometheus | Pushgateway | Typical use |
|---|---|---|---|---|
| `docker-compose.yml` | 8081 | 9099 | 9092 | Default unit |
| `docker-compose-unit1.yml` | 8181 | 9199 | 9192 | Pairs with OmniStateStore `unit1-rocksdb-tuning` |
| `docker-compose-unit23.yml` | 8281 | 9299 | 9292 | Pairs with `unit2-lru-cache-x86` or `unit3-batch-jni` |
| `docker-compose-unit4.yml` | 8381 | 9399 | 9392 | Pairs with `unit4-mapstate-opt` |
| `docker-compose-baseline.yml` | 8481 | 9099 | 9092 | Baseline run — ⚠️ Prom/PG ports collide with default; don't run together |

Point the orchestrator at a specific unit:

```bash
export BENCH_COMPOSE_PATH=$WORKSPACE/docker-compose-unit1.yml
export BENCH_JM_URL=http://localhost:8181
export BENCH_PROM_URL=http://localhost:9199
```

Variants use `image: flink-falcon:latest` (pre-built) instead of `build:` — tag the default-built image as `flink-falcon:latest` before using them:

```bash
docker tag deploy-jobmanager flink-falcon:latest
```

## 4. OmniStateStore branches (the experiment axis)

Each branch is a different optimization study:

| Branch | Experiment |
|---|---|
| `master` | Vanilla baseline |
| `all-units-v2` (default) | Combined build of all optimizations |
| `falcon` | Falcon native-lib integration |
| `unit1-rocksdb-tuning` | RocksDB option tuning study |
| `unit2-lru-cache-x86` | Off-heap LRU cache on x86 |
| `unit3-batch-jni` | Batched JNI crossings |
| `unit4-mapstate-opt` | MapState fast-path rewrite |
| `serialization-opt` | Serializer / codec optimization |

Switch + rebuild:

```bash
cd $WORKSPACE/OmniStateStore
git fetch micuks && git checkout unit1-rocksdb-tuning
./scripts/build.sh            # or: mvn package + cmake --build cpp/build
cp output/flink-alg-falcon.jar ../lib/
cp output/libfalcon.so         ../lib/
docker compose -f nexmark_bench/deploy/docker-compose.yml build --no-cache
```

## 5. State-Backend Profiles (`nexmark_bench/profiles/*.yaml`)

`nexmark_bench.config.prepare_benchmark_config(backend, script_dir)` resolves `<bundled_profiles>/flink-conf-<backend>.yaml` and copies it onto `$WORKSPACE/config/flink-conf.yaml` before each backend's rounds.

| `--mode` value | Profile | Notes |
|---|---|---|
| `rocksdb` | `flink-conf-rocksdb.yaml` | Pure RocksDB + RocksDB metrics enabled |
| `cached` | `flink-conf-cached.yaml` | CacheKit wrapper (`CacheKitStateBackendFactory`), 8k LRU entries |
| Custom | `--config-file <abs-path>` | Pass any bundled or user-provided file directly |

Also bundled for manual use via `--config-file`: `flink-conf-falcon-cache.yaml`, `flink-conf-falcon-nocache.yaml`, `flink-conf-jmh.yaml`, `flink-conf-jmh-cache{10,30,50,100}.yaml`.

## 6. Running Nexmark SQL benchmarks

```bash
cd $WORKSPACE
# Comparison: cached vs rocksdb
python -m nexmark_bench.orchestrator --mode comparison --rounds 3 -q q5,q8,q9

# Single backend + flamegraphs
python -m nexmark_bench.orchestrator --mode rocksdb --rounds 5 \
    --flamegraph --flamegraph-event cpu -q q4,q5,q8,q11

# Falcon profile on unit1 stack
BENCH_COMPOSE_PATH=$WORKSPACE/docker-compose-unit1.yml \
BENCH_JM_URL=http://localhost:8181 \
BENCH_PROM_URL=http://localhost:9199 \
python -m nexmark_bench.orchestrator \
    --config-file $WORKSPACE/nexmark_bench/profiles/flink-conf-falcon-cache.yaml \
    --mode rocksdb --rounds 3 -q q5
```

Default query set when `-q` omitted: `q4,q5,q8,q9,q11,q18,q19,q20,q3,q7,q12,q13,q15,q16,q17`. Per-query timeout: **7200s**.

Artifacts:
- `results/benchmark_results_<ts>.json` — per-round data + all metrics + counters
- `results/benchmark_summary_<ts>.txt` — rendered tables
- `flamegraphs/<backend>/<query>/*.html`
- `metrics/<backend>/*.png,*.csv,index.html`
- `benchmark_progress.json` — resumable state; delete to force-restart

### Canonical CacheKit treatment identities

Treat `fullopt`, `FullOpt+LC`, `native fullopt`, `stage1`, `stage2`, and `stage3` as reserved
experiment labels. Before constructing, launching, auditing, or reporting any of
these legs, read and follow
[CacheKit optimization definitions](references/cachekit-optimization-definitions.md).
Use the rendered effective configuration, not defaults or a variant name, as the
authority. A mismatch requires an explicit variant label and must not be silently
reported as one of the reserved treatments.

For **FullOpt+LC** (the agreed name for FullOpt+P29+P30), read
[FullOpt+LC config and usage](references/fullopt-lc.md). This requires patched
runtime JARs plus two JVM/environment gates, not just a Flink YAML profile.

### Required comparison-result format

Report baseline-versus-optimization results, including requested partial results,
as a rendered table, not prose-only output or fenced Markdown source. Keep queries
as rows, RocksDB as the first numeric column, then each optimization immediately
followed by its uplift versus RocksDB:

```text
Query/Group | RocksDB | Opt A | Opt A uplift | Opt B | Opt B uplift | ...
```

Use the actual baseline and optimization names; do not leave the generic labels
`baseline` or `opt` when the identities are known (for example Java and Native).
Append `大状态 8Q`, `小状态 7Q`, and `15Q` summary rows; each uplift is the
equal-weight arithmetic mean of per-query percentage uplifts, never weighted by
throughput. Preserve this layout in chat, Feishu, and XLSX. Put individual rounds
in detail tables/sheets rather than restoring the old fixed R1/R2/R3 main columns.
Before formatting, aggregating multiple rounds, or reporting partial groups, follow
[comparison result reporting](references/comparison-result-reporting.md).

## 7. Running JMH micro-benchmarks

```bash
python -m nexmark_bench.jmh_orchestrator --list     # show suites
python -m nexmark_bench.jmh_orchestrator --build    # mvn package flink-benchmarks/
python -m nexmark_bench.jmh_orchestrator -b value-state,map-state -f 3 -i 5 -wi 3

# Falcon suite with mounted native lib + JMH config
python -m nexmark_bench.jmh_orchestrator -b omni-state \
    --falcon-lib $WORKSPACE/OmniStateStore/output \
    --flink-conf $WORKSPACE/nexmark_bench/profiles/flink-conf-jmh.yaml
```

Suites: `value-state`, `map-state`, `list-state`, `omni-state`, `kryo-state`. Artifacts: `results/jmh/jmh_results_<ts>.json`, `jmh_summary_<ts>.txt`, per-suite CSVs.

## 8. Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_COMPOSE_PATH` | `$WORKSPACE/docker-compose.yml` | Which compose file to drive |
| `BENCH_CONTAINER_NAME` | `flink-cluster-jobmanager-1` | JM container for `docker exec` (override to `deploy-jobmanager-1` if compose project name is `deploy`) |
| `BENCH_EXPECTED_TMS` | `8` | Number of TM **JVMs** to wait for |
| `BENCH_JM_URL` | `http://localhost:8081` | Flink REST endpoint |
| `BENCH_PROM_URL` | `http://localhost:9099` | Prometheus |

## 9. Troubleshooting

**`Overlapping TaskManager process trees` / implausible cores above cpuset** —
stop using per-core results. Run the mandatory CPU-integrity preflight above and
read [CPU metric integrity](references/cpu-metric-integrity.md). The common 2x4
failure is one foreground TM becoming the parent of three daemon TMs, causing
those three to be counted twice. `BENCH_EXPECTED_TMS=8` checks coverage only; it
does not prove that the eight process-tree metrics are disjoint.

**`TaskManagers did not reach expected count`** — Flink's `/overview.taskmanagers` counts JVMs, not containers. Default is 8. Verify: `curl -s $BENCH_JM_URL/overview | jq .taskmanagers`. If only 2 registered, check `docker compose logs taskmanager1` for port clashes between the 4 JVMs.

**Stale image after jar changes** — `docker compose -f nexmark_bench/deploy/docker-compose.yml build --no-cache && docker compose -f ... up -d`. The orchestrator also runs `docker compose build` at startup.

**Prometheus returns empty for `flink_taskmanager_*_rocksdb_*`** — confirm reporter: `docker exec <tm-container> curl -s localhost:9249/metrics | head`. Verify `flink-conf.yaml` has `metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter`.

**Falcon `UnsatisfiedLinkError: libfalcon.so`** — JMH needs `--falcon-lib $WORKSPACE/OmniStateStore/output` which the orchestrator mounts at `/opt/falcon` and prepends to `LD_LIBRARY_PATH` + `java.library.path`. For the Nexmark runner (in the Flink image), copy `libfalcon.so` into `$WORKSPACE/lib/` so the Dockerfile picks it up.

**Compose port clash** — `docker-compose-baseline.yml` reuses 9099/9092. Never run alongside default. Use unit1/23/4 for parallel runs.

**`flink-conf.yaml` overwritten** — `prepare_benchmark_config()` replaces `config/flink-conf.yaml` every backend. Hand-edited configs go elsewhere and are passed via `--config-file`.

**Stuck job / hung TM** — watchdog will kill and restart. Force clean: `docker compose -f nexmark_bench/deploy/docker-compose.yml down -v --remove-orphans && docker compose -f ... up -d`.

**Resuming after Ctrl+C** — `benchmark_progress.json` at workspace root. Rerunning the same command skips completed rounds. Delete the file to start fresh.

**Bootstrap re-run after updating source branches** — `SKIP_CLONE=0 SKIP_FLINK_DL=1 SKIP_LINKS=1 ./nexmark_bench/scripts/bootstrap.sh` updates sibling repos + rebuilds jars, skipping redundant work.

## 10. Quick reference

```bash
# Nexmark SQL, default topology, comparison mode
python -m nexmark_bench.orchestrator --mode comparison --rounds 3 -q q5

# JMH, with Falcon
python -m nexmark_bench.jmh_orchestrator -b omni-state \
    --falcon-lib ./OmniStateStore/output \
    --flink-conf ./nexmark_bench/profiles/flink-conf-jmh.yaml

# Switch OmniStateStore branch and rebuild
git -C OmniStateStore checkout unit4-mapstate-opt \
    && (cd OmniStateStore && ./scripts/build.sh) \
    && cp OmniStateStore/output/flink-alg-falcon.jar lib/

# Tail the latest TaskManager log
docker compose -f nexmark_bench/deploy/docker-compose.yml logs -f taskmanager1 | tail -50
```
