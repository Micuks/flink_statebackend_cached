#!/usr/bin/env bash
# CDC final-state correctness check: RocksDB baseline vs CacheKit bp-prefetch.
set -euo pipefail

WT=${WT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)}
BENCH_ROOT=${BENCH_ROOT:-/mnt/data2/wuql/flink-cluster}
COMPOSE=${COMPOSE:-$BENCH_ROOT/nexmark_bench/deploy/docker-compose.yml}
JM=${JM:-http://localhost:8081}
JM_PORT=${JM_PORT:-${JM##*:}}
JM_PORT=${JM_PORT%%/*}
PROM_PORT=${PROM_PORT:-19099}
PUSHGATEWAY_PORT=${PUSHGATEWAY_PORT:-19092}
PROJECT=${PROJECT:-${COMPOSE_PROJECT_NAME:-flink-cluster}}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-$PROJECT}
CONTAINER=${CONTAINER:-${COMPOSE_PROJECT_NAME}-jobmanager-1}
TM_CONTAINERS=${TM_CONTAINERS:-${COMPOSE_PROJECT_NAME}-taskmanager1-1,${COMPOSE_PROJECT_NAME}-taskmanager2-1}
EXPECTED_TMS=${EXPECTED_TMS:-2}
OUT=${OUT:-$BENCH_ROOT/results-cachekit-bp-prefetch-cdc/$(date +%Y%m%d_%H%M%S)}
SKIP_DOCKER_BUILD=${SKIP_DOCKER_BUILD:-1}
FLINK_IMAGE=${FLINK_IMAGE:-flink-cluster-jobmanager:latest}

QUERY=${QUERY:-q3}
NUM_EVENTS=${NUM_EVENTS:-1000000}
TPS=${TPS:-5000}
DATA_GEN_TPS=${DATA_GEN_TPS:-10000000}
ON_DISTANCE=${ON_DISTANCE:-64}
ON_BACKPRESSURE_GATED=${ON_BACKPRESSURE_GATED:-true}
ON_LOCAL_PREAGG=${ON_LOCAL_PREAGG:-true}
ON_KEY_SORT=${ON_KEY_SORT:-false}
MAILBOX_BATCH_SIZE=${MAILBOX_BATCH_SIZE:-4096}
MAILBOX_TIMEOUT_US=${MAILBOX_TIMEOUT_US:-0}

mkdir -p "$OUT"
exec > >(tee -a "$OUT/run.log") 2>&1

ts() { date '+%Y-%m-%dT%H:%M:%S'; }
log() { echo "[$(ts)] $*"; }

wait_tms() {
    local deadline=$(( $(date +%s) + 180 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local n
        n=$(curl -s -m 5 "$JM/overview" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("taskmanagers",0))' 2>/dev/null || echo 0)
        [ "$n" -ge "$EXPECTED_TMS" ] && return 0
        sleep 5
    done
    return 1
}

write_configs() {
    cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-correctness-base.yaml" "$OUT/flink-conf-rocksdb-baseline.yaml"
    cat >> "$OUT/flink-conf-rocksdb-baseline.yaml" <<CFG

state.backend.cachekit.bp-prefetch.enabled: false
state.backend.cachekit.mailbox-batch.enabled: false
state.backend.cachekit.local-preagg.enabled: false
CFG

    cp "$BENCH_ROOT/nexmark_bench/profiles/flink-conf-cached.yaml" "$OUT/flink-conf-cachekit-bp.yaml"
    cat >> "$OUT/flink-conf-cachekit-bp.yaml" <<CFG

# Correctness profile overrides.
parallelism.default: 1
taskmanager.numberOfTaskSlots: 2
execution.checkpointing.checkpoints-after-tasks-finish.enabled: true

# CacheKit bp-prefetch candidate.
state.backend.cachekit.bp-prefetch.enabled: ${ON_BP_PREFETCH:-true}
state.backend.cachekit.bp-prefetch.distance: $ON_DISTANCE
state.backend.cachekit.bp-prefetch.backpressure-gated: $ON_BACKPRESSURE_GATED
state.backend.cachekit.bp-prefetch.commutative-key-sort: $ON_KEY_SORT
state.backend.cachekit.mailbox-batch.enabled: ${ON_MAILBOX:-true}
state.backend.cachekit.mailbox-batch.size: $MAILBOX_BATCH_SIZE
state.backend.cachekit.mailbox-batch.timeout-us: $MAILBOX_TIMEOUT_US
state.backend.cachekit.mailbox-batch.commutative-key-sort: $ON_KEY_SORT
state.backend.cachekit.local-preagg.enabled: $ON_LOCAL_PREAGG
CFG
    if [ "${NO_MAP_CACHE:-0}" = "1" ]; then
        cat >> "$OUT/flink-conf-cachekit-bp.yaml" <<CFG

# MapState caching fully disabled (matches the final perf configuration).
state.backend.cachekit.map.cache.max-entries: 0
state.backend.cachekit.map.snapshot.cache.max-entries: 0
state.backend.cachekit.map.presence.cache.max-entries: 0
CFG
    fi
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

compose_up() {
    if [ "$SKIP_DOCKER_BUILD" = "1" ]; then
        (cd "$BENCH_ROOT" && docker compose -f "$COMPOSE" up -d --no-build --remove-orphans --force-recreate)
    else
        (cd "$BENCH_ROOT" && docker compose -f "$COMPOSE" up -d --remove-orphans --force-recreate)
    fi
    wait_tms
}

clean_state() {
    docker exec "$CONTAINER" bash -lc "rm -rf /tmp/checkpoint/* /opt/flink/flink-* /opt/nexmark/results/${QUERY}_rocksdb_results_p1 /opt/nexmark/results/${QUERY}_cachekit_bp_results_p1 2>/dev/null || true"
    IFS=',' read -r -a tms <<< "$TM_CONTAINERS"
    for tm in "${tms[@]}"; do
        docker exec "$tm" bash -lc "rm -rf /tmp/checkpoint/* /opt/flink/flink-* 2>/dev/null || true" || true
    done
}

ensure_source_data() {
    local data_dir="$BENCH_ROOT/nexmark-flink/data_sources/nexmark-events-$NUM_EVENTS"
    if [ -d "$data_dir" ] && ! find "$data_dir" -maxdepth 1 -name '*.inprogress*' | grep -q .; then
        log "source data exists: $data_dir"
        return
    fi

    log "source data missing or incomplete; generating events=$NUM_EVENTS data_gen_tps=$DATA_GEN_TPS"
    cp "$OUT/flink-conf-rocksdb-baseline.yaml" "$BENCH_ROOT/config/flink-conf.yaml"
    docker restart "$CONTAINER" >/dev/null
    IFS=',' read -r -a tms <<< "$TM_CONTAINERS"
    for tm in "${tms[@]}"; do
        docker restart "$tm" >/dev/null
    done
    wait_tms
    docker exec "$CONTAINER" bash -lc "rm -rf /opt/nexmark/data_sources/nexmark-events-$NUM_EVENTS"
    docker exec -i -w /opt/nexmark "$CONTAINER" python3 - "$NUM_EVENTS" "$DATA_GEN_TPS" "$QUERY" <<'PY'
import os
import subprocess
import sys

events = int(sys.argv[1])
tps = int(sys.argv[2])
query = sys.argv[3]

root = "/opt/nexmark"
flink_home = "/opt/flink"
data_path = os.path.join(root, "data_sources", f"nexmark-events-{events}")
templates = [
    os.path.join(root, "queries", "ddl_gen_file.sql"),
    os.path.join(root, "queries", "ddl_gen.sql"),
]
for candidate in templates:
    if os.path.exists(candidate):
        template_path = candidate
        break
else:
    raise SystemExit("No ddl_gen template file found")

with open(template_path, "r", encoding="utf-8") as handle:
    sql = handle.read()

replacements = {
    "OUTPUT_PATH": data_path,
    "EVENTS_NUM": str(events),
    "TPS": str(tps),
    "PERSON_PROPORTION": "1",
    "AUCTION_PROPORTION": "1",
    "BID_PROPORTION": "1",
}
for key, value in replacements.items():
    sql = sql.replace("${" + key + "}", value)
    sql = sql.replace("{" + key + "}", value)

sql = (
    "SET 'sql-client.execution.result-mode' = 'tableau';\n"
    "SET 'table.dml-sync' = 'true';\n"
    + sql
)

print(f"--- Generating source data: events={events}, tps={tps}, query={query}, path={data_path} ---", flush=True)
proc = subprocess.run(
    [os.path.join(flink_home, "bin", "sql-client.sh"), "embedded"],
    cwd=root,
    input=sql,
    text=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
)
print(proc.stdout)
if proc.returncode != 0:
    raise SystemExit(proc.returncode)

if not os.path.isdir(data_path):
    raise SystemExit(f"Data directory was not created: {data_path}")

names = os.listdir(data_path)
inprogress = [name for name in names if ".inprogress" in name]
parts = [name for name in names if name.startswith("part-")]
if inprogress:
    raise SystemExit(f"Data generation left in-progress files: {inprogress[:5]}")
if not parts:
    raise SystemExit(f"Data generation produced no part files in {data_path}")

total_bytes = sum(os.path.getsize(os.path.join(data_path, name)) for name in parts)
print(f"--- Source data generated successfully: parts={len(parts)} bytes={total_bytes} ---", flush=True)
PY
}

run_leg() {
    local leg=$1 cfg=$2 backend_tag=$3
    log "START CDC leg=$leg query=$QUERY events=$NUM_EVENTS"
    cp "$cfg" "$BENCH_ROOT/config/flink-conf.yaml"
    clean_state
    docker restart "$CONTAINER" >/dev/null
    IFS=',' read -r -a tms <<< "$TM_CONTAINERS"
    for tm in "${tms[@]}"; do
        docker restart "$tm" >/dev/null
    done
    wait_tms
    docker exec -w /opt/nexmark "$CONTAINER" \
        python3 /opt/nexmark/run_nexmark_with_savepoint.py \
            --backend "$backend_tag" --query "$QUERY" --num-events "$NUM_EVENTS" --tps "$TPS"
    local src="$BENCH_ROOT/nexmark-flink/results/${QUERY}_${backend_tag}_results_p1"
    local dst="$OUT/results/$leg"
    rm -rf "$dst"
    mkdir -p "$(dirname "$dst")"
    cp -a "$src" "$dst"
    log "DONE CDC leg=$leg copied=$dst"
}

compare() {
    PYTHONPATH="$BENCH_ROOT" python3 -m nexmark_bench.correctness_orchestrator pair \
        "$OUT/results/rocksdb" "$OUT/results/cachekit-bp" --query "$QUERY" \
        > "$OUT/diff.txt" 2>&1
    cat "$OUT/diff.txt"
    if grep -q '^SUCCESS:' "$OUT/diff.txt"; then
        echo PASS > "$OUT/STATUS"
    else
        echo FAIL > "$OUT/STATUS"
        return 1
    fi
}

log "CacheKit bp-prefetch CDC correctness start"
log "baseline=rocksdb candidate=cachekit-bp query=$QUERY events=$NUM_EVENTS compose=$COMPOSE project=$COMPOSE_PROJECT_NAME"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    "$WT/scripts/build_cachekit_bp_prefetch_artifacts.sh"
fi
write_configs
prepare_compose
if [ "$SKIP_DOCKER_BUILD" != "1" ]; then
    (cd "$BENCH_ROOT" && docker compose -f "$COMPOSE" build)
else
    log "SKIP_DOCKER_BUILD=1; using mounted staged jars"
fi
compose_up
ensure_source_data
run_leg rocksdb "$OUT/flink-conf-rocksdb-baseline.yaml" rocksdb
run_leg cachekit-bp "$OUT/flink-conf-cachekit-bp.yaml" cachekit_bp
compare
log "CacheKit bp-prefetch CDC correctness done: STATUS=$(cat "$OUT/STATUS") OUT=$OUT"
