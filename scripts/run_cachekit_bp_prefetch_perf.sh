#!/usr/bin/env bash
# Interleaved RocksDB baseline vs CacheKit bp-prefetch benchmark runner.
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
OUT=${OUT:-$BENCH_ROOT/results-cachekit-bp-prefetch/$(date +%Y%m%d_%H%M%S)}
SKIP_DOCKER_BUILD=${SKIP_DOCKER_BUILD:-1}
FLINK_IMAGE=${FLINK_IMAGE:-flink-cluster-jobmanager:latest}

EVENTS_LIST=${EVENTS_LIST:-50000000,100000000}
QUERIES=${QUERIES:-q3,q4,q5,q7,q8,q9,q11,q12,q13,q15,q16,q17,q18,q19,q20}
ROUNDS=${ROUNDS:-1}
WARMUP_EVENTS=${WARMUP_EVENTS:-5000000}

ON_DISTANCE=${ON_DISTANCE:-64}
ON_BACKPRESSURE_GATED=${ON_BACKPRESSURE_GATED:-true}
ON_LOCAL_PREAGG=${ON_LOCAL_PREAGG:-true}
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

    grep -q 'state.backend: rocksdb' "$OUT/flink-conf-rocksdb-baseline.yaml"
    grep -q 'state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory' \
        "$OUT/flink-conf-cachekit-bp.yaml"
}

prepare_compose() {
    if [ "$SKIP_DOCKER_BUILD" != "1" ]; then
        return
    fi
    COMPOSE="$OUT/docker-compose-cachekit-bp-runtime.yml"
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
    OUT_DIR="$OUT" EVENTS_LIST="$EVENTS_LIST" QUERY_LIST="$QUERIES" ROUNDS="$ROUNDS" python3 - <<'PY'
import json
import os
import statistics
from pathlib import Path

out = Path(os.environ["OUT_DIR"])
events_list = [e.strip() for e in os.environ["EVENTS_LIST"].split(",") if e.strip()]
queries = [q.strip() for q in os.environ["QUERY_LIST"].split(",") if q.strip()]
rounds = int(os.environ["ROUNDS"])

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

lines = []
payload = {"metric": "throughput_per_core_kps", "events": {}, "baseline": "rocksdb"}
for events in events_list:
    lines.append(f"## events={events}\n")
    lines.append("| query | rocksdb k/s/core | cachekit-bp k/s/core | speedup | cores base/cand | wall base/cand |\n")
    lines.append("|---|---:|---:|---:|---|---|\n")
    speedups = []
    payload["events"][events] = {}
    for q in queries:
        base_vals = []
        cand_vals = []
        base_meta = []
        cand_meta = []
        for r in range(1, rounds + 1):
            b = read_result(events, "rocksdb", q, r)
            c = read_result(events, "cachekit-bp", q, r)
            if not b or not c:
                continue
            if b["throughput_per_core_kps"] is not None:
                base_vals.append(b["throughput_per_core_kps"])
                base_meta.append(b)
            if c["throughput_per_core_kps"] is not None:
                cand_vals.append(c["throughput_per_core_kps"])
                cand_meta.append(c)
        base = statistics.mean(base_vals) if base_vals else None
        cand = statistics.mean(cand_vals) if cand_vals else None
        speed = ((cand - base) / base * 100.0) if base and cand else None
        if speed is not None:
            speedups.append(speed)
        payload["events"][events][q] = {
            "rocksdb_throughput_per_core_kps": base,
            "cachekit_bp_throughput_per_core_kps": cand,
            "speedup_pct": speed,
            "rocksdb_rounds": base_meta,
            "cachekit_bp_rounds": cand_meta,
        }
        def f(v):
            return "NA" if v is None else f"{v:.2f}"
        bc = "/".join(str(x.get("cores")) for x in base_meta) or "NA"
        cc = "/".join(str(x.get("cores")) for x in cand_meta) or "NA"
        bw = "/".join(f"{x.get('wall_seconds'):.1f}" for x in base_meta if x.get("wall_seconds") is not None) or "NA"
        cw = "/".join(f"{x.get('wall_seconds'):.1f}" for x in cand_meta if x.get("wall_seconds") is not None) or "NA"
        lines.append(f"| {q} | {f(base)} | {f(cand)} | {f(speed)}% | {bc}/{cc} | {bw}/{cw} |\n")
    mean = statistics.mean(speedups) if speedups else None
    payload["events"][events]["_mean_speedup_pct"] = mean
    lines.append(f"\nmean speedup: {f(mean)}%\n\n")

(out / "COMPARE-cachekit-bp-prefetch.md").write_text("".join(lines))
(out / "COMPARE-cachekit-bp-prefetch.json").write_text(json.dumps(payload, indent=2))
print("".join(lines))
PY
}

log "CacheKit bp-prefetch benchmark start"
log "baseline=rocksdb candidate=cachekit-bp metric=throughput_per_core_kps"
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
            run_one "$events" rocksdb rocksdb "$OUT/flink-conf-rocksdb-baseline.yaml" "$query" "$round"
            run_one "$events" cachekit-bp cached "$OUT/flink-conf-cachekit-bp.yaml" "$query" "$round"
        done
    done
done

compare_results
log "CacheKit bp-prefetch benchmark done: $OUT"
