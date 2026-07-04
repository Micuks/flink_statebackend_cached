#!/usr/bin/env bash
# Interleaved RocksDB baseline vs CacheKit bp-prefetch benchmark runner.
# Set EXPERIMENT=prefetch-only to run a three-leg isolation:
# rocksdb reference, CacheKit cache-only, CacheKit cache+bp-prefetch.
#
# Metric policy:
# - Nexmark prints both Throughput and Throughput/Cores.
# - nexmark_bench.orchestrator parses the final Throughput/Cores column into
#   the per-round JSON "results" field, so the comparison below is throughput
#   per core.
set -euo pipefail

WT=${WT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}
BENCH_ROOT=${BENCH_ROOT:-/mnt/data2/wuql/flink-cluster}
COMPOSE=${COMPOSE:-$BENCH_ROOT/nexmark_bench/deploy/docker-compose.yml}
JM=${JM:-http://localhost:8081}
PROM=${PROM:-http://localhost:9099}
JM_PORT=${JM_PORT:-${JM##*:}}
JM_PORT=${JM_PORT%%/*}
PROM_PORT=${PROM_PORT:-${PROM##*:}}
PROM_PORT=${PROM_PORT%%/*}
PUSHGATEWAY_PORT=${PUSHGATEWAY_PORT:-9092}
PROJECT=${PROJECT:-${COMPOSE_PROJECT_NAME:-flink-cluster}}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-$PROJECT}
CONTAINER=${CONTAINER:-${COMPOSE_PROJECT_NAME}-jobmanager-1}
EXPECTED_TMS=${EXPECTED_TMS:-2}
EXPERIMENT=${EXPERIMENT:-bp-prefetch}
case "$EXPERIMENT" in
    bp-prefetch|prefetch-only) ;;
    *)
        echo "Unsupported EXPERIMENT=$EXPERIMENT; expected bp-prefetch or prefetch-only" >&2
        exit 2
        ;;
esac
DEFAULT_OUT_ROOT="$BENCH_ROOT/results-cachekit-bp-prefetch"
if [ "$EXPERIMENT" = "prefetch-only" ]; then
    DEFAULT_OUT_ROOT="$BENCH_ROOT/results-cachekit-prefetch-only"
fi
OUT=${OUT:-$DEFAULT_OUT_ROOT/$(date +%Y%m%d_%H%M%S)}
SKIP_DOCKER_BUILD=${SKIP_DOCKER_BUILD:-1}
FLINK_IMAGE=${FLINK_IMAGE:-flink-cluster-jobmanager:latest}
SIDE_INPUT_DIR=${SIDE_INPUT_DIR:-$BENCH_ROOT/nexmark-side-input}

EVENTS_LIST=${EVENTS_LIST:-50000000,100000000}
QUERIES=${QUERIES:-q3,q4,q5,q7,q8,q9,q11,q12,q13,q15,q16,q17,q18,q19,q20}
ROUNDS=${ROUNDS:-1}
WARMUP_EVENTS=${WARMUP_EVENTS:-5000000}

ON_DISTANCE=${ON_DISTANCE:-64}
ON_BACKPRESSURE_GATED=${ON_BACKPRESSURE_GATED:-true}
if [ "$EXPERIMENT" = "prefetch-only" ]; then
    ON_LOCAL_PREAGG=${ON_LOCAL_PREAGG:-false}
else
    ON_LOCAL_PREAGG=${ON_LOCAL_PREAGG:-true}
fi
ON_KEY_SORT=${ON_KEY_SORT:-false}
MAILBOX_BATCH_SIZE=${MAILBOX_BATCH_SIZE:-4096}
MAILBOX_TIMEOUT_US=${MAILBOX_TIMEOUT_US:-0}

mkdir -p "$OUT/work"
exec > >(tee -a "$OUT/run.log") 2>&1

ts() { date '+%Y-%m-%dT%H:%M:%S'; }
log() { echo "[$(ts)] $*"; }

write_nexmark_conf() {
    local events=$1
    awk -v events="$events" -v warmup="$WARMUP_EVENTS" '
        /^[[:space:]]*nexmark\.workload\.suite\.100m\.events\.num:/ {
            print "nexmark.workload.suite.100m.events.num: " events; next
        }
        /^[[:space:]]*nexmark\.workload\.suite\.100m\.warmup\.events\.num:/ {
            print "nexmark.workload.suite.100m.warmup.events.num: " warmup; next
        }
        { print }
    ' "$BENCH_ROOT/nexmark-flink/conf/nexmark.yaml" > "$OUT/nexmark-$events.yaml"
    cp "$OUT/nexmark-$events.yaml" "$BENCH_ROOT/nexmark-flink/conf/nexmark.yaml"
}

write_configs() {
    cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-rocksdb.yaml" "$OUT/flink-conf-rocksdb-baseline.yaml"
    cat >> "$OUT/flink-conf-rocksdb-baseline.yaml" <<CFG

# CacheKit bp-prefetch controls explicitly disabled for the RocksDB baseline.
state.backend.cachekit.bp-prefetch.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.local-preagg.enabled: false
CFG

    if [ "$EXPERIMENT" = "prefetch-only" ]; then
        cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml" "$OUT/flink-conf-cachekit-cache-only.yaml"
        cat >> "$OUT/flink-conf-cachekit-cache-only.yaml" <<CFG

# CacheKit cache-only reference for isolated prefetch comparison.
# The CacheKit ValueState/MapState cache settings come from flink-conf-cached.yaml.
# All batching, local pre-aggregation, and prefetch paths are disabled.
state.backend.cachekit.bp-prefetch.enabled: false
state.backend.cachekit.bp-prefetch.async-chunks.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.local-preagg.enabled: false
CFG

        cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml" "$OUT/flink-conf-cachekit-prefetch-only.yaml"
        cat >> "$OUT/flink-conf-cachekit-prefetch-only.yaml" <<CFG

# CacheKit prefetch-only candidate for isolated comparison.
# Same CacheKit ValueState/MapState cache settings as cache-only; only bp-prefetch is enabled.
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: $ON_DISTANCE
state.backend.cachekit.bp-prefetch.backpressure-gated: $ON_BACKPRESSURE_GATED
state.backend.cachekit.bp-prefetch.commutative-key-sort: false
state.backend.cachekit.bp-prefetch.async-chunks.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.mailbox-batch.size: $MAILBOX_BATCH_SIZE
state.backend.cachekit.mailbox-batch.timeout-us: $MAILBOX_TIMEOUT_US
state.backend.cachekit.mailbox-batch.commutative-key-sort: false
state.backend.cachekit.local-preagg.enabled: false
CFG
    else
        cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml" "$OUT/flink-conf-cachekit-bp.yaml"
        cat >> "$OUT/flink-conf-cachekit-bp.yaml" <<CFG

# CacheKit bp-prefetch candidate.
state.backend.cachekit.bp-prefetch.enabled: true
state.backend.cachekit.bp-prefetch.distance: $ON_DISTANCE
state.backend.cachekit.bp-prefetch.backpressure-gated: $ON_BACKPRESSURE_GATED
state.backend.cachekit.bp-prefetch.commutative-key-sort: $ON_KEY_SORT
state.backend.cachekit.mailbox-batch.enabled: true
state.backend.cachekit.mailbox-batch.size: $MAILBOX_BATCH_SIZE
state.backend.cachekit.mailbox-batch.timeout-us: $MAILBOX_TIMEOUT_US
state.backend.cachekit.mailbox-batch.commutative-key-sort: $ON_KEY_SORT
state.backend.cachekit.local-preagg.enabled: $ON_LOCAL_PREAGG
CFG
    fi

    grep -q 'state.backend: rocksdb' "$OUT/flink-conf-rocksdb-baseline.yaml"
    if [ "$EXPERIMENT" = "prefetch-only" ]; then
        grep -q 'state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory' \
            "$OUT/flink-conf-cachekit-cache-only.yaml"
        grep -q 'state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory' \
            "$OUT/flink-conf-cachekit-prefetch-only.yaml"
    else
        grep -q 'state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory' \
            "$OUT/flink-conf-cachekit-bp.yaml"
    fi
}

prepare_compose() {
    if [ "$SKIP_DOCKER_BUILD" != "1" ]; then
        return
    fi
    case ",$QUERIES," in
        *,q13,*)
            if [ ! -f "$SIDE_INPUT_DIR/side_input.txt" ]; then
                log "ERROR: q13 requires side input file: $SIDE_INPUT_DIR/side_input.txt"
                exit 1
            fi
            ;;
    esac
    COMPOSE="$OUT/docker-compose-$EXPERIMENT-runtime.yml"
    cat > "$COMPOSE" <<CFG
services:
  jobmanager:
    image: $FLINK_IMAGE
    hostname: jobmanager
    ports:
      - "$JM_PORT:8081"
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
    volumes:
      - $BENCH_ROOT/config/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-rocksdb.yaml:/opt/flink/conf/flink-conf-rocksdb.yaml:ro
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml:/opt/flink/conf/flink-conf-cached.yaml:ro
      - $BENCH_ROOT/nexmark_bench/deploy/workers:/opt/flink/conf/workers:ro
      - flink-logs:/opt/flink/log
      - $SIDE_INPUT_DIR:/opt/flink/data:ro
      - $BENCH_ROOT/nexmark-flink:/opt/nexmark
      - $BENCH_ROOT/lib/flink-dist-1.16.3.jar:/opt/flink/lib/flink-dist-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-table-runtime-1.16.3.jar:/opt/flink/lib/flink-table-runtime-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:/opt/flink/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:ro

  taskmanager1:
    image: $FLINK_IMAGE
    hostname: taskmanager1
    command: taskmanager
    depends_on:
      - jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
    volumes:
      - $BENCH_ROOT/config/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-rocksdb.yaml:/opt/flink/conf/flink-conf-rocksdb.yaml:ro
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml:/opt/flink/conf/flink-conf-cached.yaml:ro
      - $BENCH_ROOT/nexmark_bench/deploy/workers:/opt/flink/conf/workers:ro
      - flink-logs:/opt/flink/log
      - $SIDE_INPUT_DIR:/opt/flink/data:ro
      - $BENCH_ROOT/nexmark-flink:/opt/nexmark
      - $BENCH_ROOT/lib/flink-dist-1.16.3.jar:/opt/flink/lib/flink-dist-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-table-runtime-1.16.3.jar:/opt/flink/lib/flink-table-runtime-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:/opt/flink/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:ro

  taskmanager2:
    image: $FLINK_IMAGE
    hostname: taskmanager2
    command: taskmanager
    depends_on:
      - jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
    volumes:
      - $BENCH_ROOT/config/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-rocksdb.yaml:/opt/flink/conf/flink-conf-rocksdb.yaml:ro
      - $BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml:/opt/flink/conf/flink-conf-cached.yaml:ro
      - $BENCH_ROOT/nexmark_bench/deploy/workers:/opt/flink/conf/workers:ro
      - flink-logs:/opt/flink/log
      - $SIDE_INPUT_DIR:/opt/flink/data:ro
      - $BENCH_ROOT/nexmark-flink:/opt/nexmark
      - $BENCH_ROOT/lib/flink-dist-1.16.3.jar:/opt/flink/lib/flink-dist-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-table-runtime-1.16.3.jar:/opt/flink/lib/flink-table-runtime-1.16.3.jar:ro
      - $BENCH_ROOT/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:/opt/flink/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar:ro

  prometheus:
    image: prom/prometheus:v2.37.0
    ports:
      - "$PROM_PORT:9090"
    volumes:
      - $BENCH_ROOT/nexmark_bench/deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    depends_on:
      - pushgateway

  pushgateway:
    image: prom/pushgateway:v1.4.3
    ports:
      - "$PUSHGATEWAY_PORT:9091"

volumes:
  flink-logs:
CFG
    log "using runtime compose without docker build: $COMPOSE image=$FLINK_IMAGE"
}

run_one() {
    local events=$1 leg=$2 mode=$3 cfg=$4 query=$5 round=$6
    local state="$OUT/work/${events}.${leg}.${query}.r${round}.json"
    rm -f "$state"
    log "START events=$events leg=$leg query=$query round=$round"
    (
        cd "$BENCH_ROOT"
        BENCH_STATE_FILE="$state" \
        BENCH_COMPOSE_PATH="$COMPOSE" \
        BENCH_JM_URL="$JM" \
        BENCH_PROM_URL="$PROM" \
        BENCH_CONTAINER_NAME="$CONTAINER" \
        BENCH_EXPECTED_TMS="$EXPECTED_TMS" \
        PYTHONPATH="$BENCH_ROOT" \
            python3 -u -m nexmark_bench.orchestrator \
                --mode "$mode" --config-file "$cfg" -q "$query" --rounds 1
    )
    log "DONE events=$events leg=$leg query=$query round=$round"
}

compare_results() {
    OUT_DIR="$OUT" EVENTS_LIST="$EVENTS_LIST" QUERY_LIST="$QUERIES" ROUNDS="$ROUNDS" EXPERIMENT="$EXPERIMENT" python3 - <<'PY'
import json
import os
import statistics
from pathlib import Path

out = Path(os.environ["OUT_DIR"])
events_list = [e.strip() for e in os.environ["EVENTS_LIST"].split(",") if e.strip()]
queries = [q.strip() for q in os.environ["QUERY_LIST"].split(",") if q.strip()]
rounds = int(os.environ["ROUNDS"])
experiment = os.environ["EXPERIMENT"]

def read_result(events, leg, q, r):
    p = out / "work" / f"{events}.{leg}.{q}.r{r}.json"
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    for backend_data in data.values():
        if q in backend_data:
            qd = backend_data[q]
            results = qd.get("results") or []
            cores = qd.get("cores") or []
            times = qd.get("round_times") or []
            return {
                "throughput_per_core_kps": results[0] if results else None,
                "cores": cores[0] if cores else None,
                "wall_seconds": times[0] if times else None,
            }
    return None

def collect(events, leg, q):
    vals = []
    meta = []
    for r in range(1, rounds + 1):
        item = read_result(events, leg, q, r)
        if not item:
            continue
        if item["throughput_per_core_kps"] is not None:
            vals.append(item["throughput_per_core_kps"])
            meta.append(item)
    return vals, meta

def mean(vals):
    return statistics.mean(vals) if vals else None

def speedup(candidate, baseline):
    return ((candidate - baseline) / baseline * 100.0) if baseline and candidate else None

def f(v):
    return "NA" if v is None else f"{v:.2f}"

def join_cores(meta):
    return "/".join(str(x.get("cores")) for x in meta) or "NA"

def join_walls(meta):
    return "/".join(f"{x.get('wall_seconds'):.1f}" for x in meta if x.get("wall_seconds") is not None) or "NA"

lines = []
payload = {"metric": "throughput_per_core_kps", "experiment": experiment, "events": {}}

if experiment == "prefetch-only":
    payload["isolated_baseline"] = "cache-only"
    payload["reference_baseline"] = "rocksdb"
    for events in events_list:
        lines.append(f"## events={events}\n")
        lines.append("| query | rocksdb k/s/core | cache-only k/s/core | prefetch-only k/s/core | prefetch vs cache-only | prefetch vs rocksdb | cache-only vs rocksdb | cores r/c/p | wall r/c/p |\n")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---|---|\n")
        isolated_speedups = []
        prefetch_vs_rocksdb = []
        cache_vs_rocksdb = []
        payload["events"][events] = {}
        for q in queries:
            rocks_vals, rocks_meta = collect(events, "rocksdb", q)
            cache_vals, cache_meta = collect(events, "cache-only", q)
            prefetch_vals, prefetch_meta = collect(events, "prefetch-only", q)
            rocks = mean(rocks_vals)
            cache = mean(cache_vals)
            prefetch = mean(prefetch_vals)
            isolated = speedup(prefetch, cache)
            prefetch_abs = speedup(prefetch, rocks)
            cache_abs = speedup(cache, rocks)
            if isolated is not None:
                isolated_speedups.append(isolated)
            if prefetch_abs is not None:
                prefetch_vs_rocksdb.append(prefetch_abs)
            if cache_abs is not None:
                cache_vs_rocksdb.append(cache_abs)
            payload["events"][events][q] = {
                "rocksdb_throughput_per_core_kps": rocks,
                "cache_only_throughput_per_core_kps": cache,
                "prefetch_only_throughput_per_core_kps": prefetch,
                "prefetch_vs_cache_only_speedup_pct": isolated,
                "prefetch_vs_rocksdb_speedup_pct": prefetch_abs,
                "cache_only_vs_rocksdb_speedup_pct": cache_abs,
                "rocksdb_rounds": rocks_meta,
                "cache_only_rounds": cache_meta,
                "prefetch_only_rounds": prefetch_meta,
            }
            cores = f"{join_cores(rocks_meta)}/{join_cores(cache_meta)}/{join_cores(prefetch_meta)}"
            walls = f"{join_walls(rocks_meta)}/{join_walls(cache_meta)}/{join_walls(prefetch_meta)}"
            lines.append(
                f"| {q} | {f(rocks)} | {f(cache)} | {f(prefetch)} | {f(isolated)}% | "
                f"{f(prefetch_abs)}% | {f(cache_abs)}% | {cores} | {walls} |\n"
            )
        iso_mean = mean(isolated_speedups)
        prefetch_abs_mean = mean(prefetch_vs_rocksdb)
        cache_abs_mean = mean(cache_vs_rocksdb)
        payload["events"][events]["_mean_prefetch_vs_cache_only_speedup_pct"] = iso_mean
        payload["events"][events]["_mean_prefetch_vs_rocksdb_speedup_pct"] = prefetch_abs_mean
        payload["events"][events]["_mean_cache_only_vs_rocksdb_speedup_pct"] = cache_abs_mean
        lines.append(f"\nmean prefetch vs cache-only: {f(iso_mean)}%\n")
        lines.append(f"mean prefetch vs rocksdb: {f(prefetch_abs_mean)}%\n")
        lines.append(f"mean cache-only vs rocksdb: {f(cache_abs_mean)}%\n\n")

    (out / "COMPARE-cachekit-prefetch-only.md").write_text("".join(lines))
    (out / "COMPARE-cachekit-prefetch-only.json").write_text(json.dumps(payload, indent=2))
else:
    payload["baseline"] = "rocksdb"
    for events in events_list:
        lines.append(f"## events={events}\n")
        lines.append("| query | rocksdb k/s/core | cachekit-bp k/s/core | speedup | cores base/cand | wall base/cand |\n")
        lines.append("|---|---:|---:|---:|---|---|\n")
        speedups = []
        payload["events"][events] = {}
        for q in queries:
            base_vals, base_meta = collect(events, "rocksdb", q)
            cand_vals, cand_meta = collect(events, "cachekit-bp", q)
            base = mean(base_vals)
            cand = mean(cand_vals)
            speed = speedup(cand, base)
            if speed is not None:
                speedups.append(speed)
            payload["events"][events][q] = {
                "rocksdb_throughput_per_core_kps": base,
                "cachekit_bp_throughput_per_core_kps": cand,
                "speedup_pct": speed,
                "rocksdb_rounds": base_meta,
                "cachekit_bp_rounds": cand_meta,
            }
            bc = join_cores(base_meta)
            cc = join_cores(cand_meta)
            bw = join_walls(base_meta)
            cw = join_walls(cand_meta)
            lines.append(f"| {q} | {f(base)} | {f(cand)} | {f(speed)}% | {bc}/{cc} | {bw}/{cw} |\n")
        speed_mean = mean(speedups)
        payload["events"][events]["_mean_speedup_pct"] = speed_mean
        lines.append(f"\nmean speedup: {f(speed_mean)}%\n\n")

    (out / "COMPARE-cachekit-bp-prefetch.md").write_text("".join(lines))
    (out / "COMPARE-cachekit-bp-prefetch.json").write_text(json.dumps(payload, indent=2))

print("".join(lines))
PY
}

log "CacheKit benchmark start experiment=$EXPERIMENT"
if [ "$EXPERIMENT" = "prefetch-only" ]; then
    log "legs=rocksdb,cache-only,prefetch-only isolated_baseline=cache-only metric=throughput_per_core_kps"
else
    log "baseline=rocksdb candidate=cachekit-bp metric=throughput_per_core_kps"
fi
log "events=$EVENTS_LIST queries=$QUERIES rounds=$ROUNDS compose=$COMPOSE project=$COMPOSE_PROJECT_NAME"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    "$WT/scripts/build_cachekit_bp_prefetch_artifacts.sh"
fi

cp "$BENCH_ROOT/nexmark-flink/conf/nexmark.yaml" "$OUT/nexmark.yaml.before"
restore_nexmark() {
    cp "$OUT/nexmark.yaml.before" "$BENCH_ROOT/nexmark-flink/conf/nexmark.yaml" || true
}
trap restore_nexmark EXIT

write_configs
prepare_compose

IFS=',' read -r -a events_array <<< "$EVENTS_LIST"
IFS=',' read -r -a query_array <<< "$QUERIES"

for events in "${events_array[@]}"; do
    events=$(echo "$events" | xargs)
    [ -n "$events" ] || continue
    write_nexmark_conf "$events"
    grep -q "nexmark.workload.suite.100m.events.num: $events" "$BENCH_ROOT/nexmark-flink/conf/nexmark.yaml"
    for round in $(seq 1 "$ROUNDS"); do
        for query in "${query_array[@]}"; do
            query=$(echo "$query" | xargs)
            [ -n "$query" ] || continue
            if [ "$EXPERIMENT" = "prefetch-only" ]; then
                run_one "$events" rocksdb rocksdb "$OUT/flink-conf-rocksdb-baseline.yaml" "$query" "$round"
                run_one "$events" cache-only cached "$OUT/flink-conf-cachekit-cache-only.yaml" "$query" "$round"
                run_one "$events" prefetch-only cached "$OUT/flink-conf-cachekit-prefetch-only.yaml" "$query" "$round"
            else
                run_one "$events" rocksdb rocksdb "$OUT/flink-conf-rocksdb-baseline.yaml" "$query" "$round"
                run_one "$events" cachekit-bp cached "$OUT/flink-conf-cachekit-bp.yaml" "$query" "$round"
            fi
        done
    done
done

compare_results
log "CacheKit benchmark done: $OUT"
