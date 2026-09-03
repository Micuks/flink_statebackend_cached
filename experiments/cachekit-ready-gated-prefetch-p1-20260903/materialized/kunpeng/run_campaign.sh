#!/usr/bin/env bash
set -euo pipefail

expdir=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-kunpeng-20260903
project=ckkp5a9p1
compose=/home/wuql/bin/docker-compose
rest=http://127.0.0.1:10789
prom=http://127.0.0.1:11823
scratch=/tmp/ckkp5a9p1
runtime_manifest=$expdir/inputs/runtime/RUNTIME_BUNDLE.json
source_commit=5a9d1e656715a403afac72ee1a516876ddbfb7f1
artifact_sha=97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc
allow_disjoint_foreign=true
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74
target_cpuset_mems=0
events=100000000
queries=(q9)
variants=(control ready-d2)
rounds=(1)

export GOLDEN_HOST=kunpeng
export GOLDEN_CONTAINER_FLINK_HOME=/opt/flink-1.16.3
export COMPOSE_PROJECT_NAME=$project
export GOLDEN_COMPOSE_COMMAND_JSON="[\"$compose\",\"--project-name\",\"$project\"]"
export GOLDEN_CONTAINER_CPUSET_MEMS_JSON='{"ckkp5a9p1_jobmanager_1":"0","ckkp5a9p1_taskmanager1_1":"0","ckkp5a9p1_taskmanager2_1":"0","ckkp5a9p1_prometheus_1":"0","ckkp5a9p1_pushgateway_1":"0"}'

mkdir -p "$expdir"/{logs,results/raw,results/failed,final} "$scratch"
owner=$scratch/.cachekit-ready-gated-owner
if [[ -e $owner ]]; then
  [[ $(<"$owner") == "$expdir" ]] || { echo "unowned scratch: $scratch" >&2; exit 70; }
else
  [[ -z $(find "$scratch" -mindepth 1 -maxdepth 1 -print -quit) ]] || {
    echo "nonempty unowned scratch: $scratch" >&2
    exit 70
  }
  printf '%s\n' "$expdir" >"$owner"
fi
chmod 1777 "$scratch"

log() { echo "[$(date -Is)] $*" | tee -a "$expdir/logs/run.log"; }

cleanup() {
  local cf=$expdir/variants/control/docker-compose.yml
  "$compose" --project-name "$project" -f "$cf" down -v --remove-orphans --timeout 30 \
    >>"$expdir/logs/compose-cleanup.log" 2>&1 || true
  find "$scratch" -depth -mindepth 1 ! -path "$owner" -delete 2>/dev/null || true
}
trap cleanup EXIT

fail_on_foreign_containers() {
  local id owner_project owner_cpus foreign=0
  while read -r id; do
    [[ -n $id ]] || continue
    owner_project=$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$id" 2>/dev/null || true)
    if [[ $owner_project != "$project" ]]; then
      owner_cpus=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$id" 2>/dev/null || true)
      if [[ $allow_disjoint_foreign == true && -n $owner_cpus ]] &&
          ! python3 - "$owner_cpus" "$target_cpuset" <<'PY'
import sys


def expand(spec):
    result = set()
    for part in spec.split(','):
        bounds = [int(value) for value in part.split('-', 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


raise SystemExit(0 if expand(sys.argv[1]) & expand(sys.argv[2]) else 1)
PY
      then
        docker inspect -f 'allowed_disjoint_container={{.Name}} cpus={{.HostConfig.CpusetCpus}}' "$id" >&2
      else
        docker inspect -f 'foreign_container={{.Name}} image={{.Config.Image}} project={{index .Config.Labels "com.docker.compose.project"}} cpus={{.HostConfig.CpusetCpus}}' "$id" >&2
        foreign=1
      fi
    fi
  done < <(docker ps -q)
  (( foreign == 0 )) || return 1
}

wait_cluster() {
  local deadline=$((SECONDS+300))
  while (( SECONDS < deadline )); do
    if python3 - "$rest" <<'PY' >/dev/null 2>&1
import json,sys,urllib.request
base=sys.argv[1]
with urllib.request.urlopen(base+'/overview',timeout=3) as r: overview=json.load(r)
with urllib.request.urlopen(base+'/taskmanagers',timeout=3) as r: tms=json.load(r).get('taskmanagers',[])
assert len(tms)==8,(len(tms),overview)
assert int(overview.get('slots-total',-1))==16,overview
PY
    then return 0; fi
    sleep 3
  done
  return 1
}

start_cluster() {
  local cf=$1 id actual_mems
  local -a ids
  if [[ -z $target_cpuset_mems ]]; then
    "$compose" --project-name "$project" -f "$cf" up -d
    return
  fi
  "$compose" --project-name "$project" -f "$cf" up --no-start
  mapfile -t ids < <(
    docker ps -aq --filter "label=com.docker.compose.project=$project"
  )
  (( ${#ids[@]} == 5 )) || {
    echo "expected five stopped campaign containers, found ${#ids[@]}" >&2
    return 1
  }
  docker update --cpuset-mems "$target_cpuset_mems" "${ids[@]}"
  for id in "${ids[@]}"; do
    actual_mems=$(docker inspect -f '{{.HostConfig.CpusetMems}}' "$id")
    [[ $actual_mems == "$target_cpuset_mems" ]] || {
      echo "container memory-node bind mismatch: id=$id mems=$actual_mems" >&2
      return 1
    }
  done
  "$compose" --project-name "$project" -f "$cf" start
}

capture_logs() {
  local d=$1 cf=$2
  mkdir -p "$d/container-logs"
  "$compose" --project-name "$project" -f "$cf" logs --no-color >"$d/compose.log" 2>&1 || true
  local id name
  while read -r id; do
    [[ -n $id ]] || continue
    name=$(docker inspect -f '{{.Name}}' "$id" | sed 's#^/##')
    docker logs "$id" >"$d/container-logs/$name.log" 2>&1 || true
  done < <(docker ps -aq --filter "label=com.docker.compose.project=$project")
}

capture_metric_raw() {
  local d=$1 volume=${project}_nexmark-logs mountpoint
  mountpoint=$(docker volume inspect -f '{{.Mountpoint}}' "$volume")
  mkdir -p "$d/process-logs"
  for name in metric-client.log nexmark-flink.log; do
    [[ -f $mountpoint/$name ]] && cp "$mountpoint/$name" "$d/process-logs/$name"
  done
  curl -fsS "$prom/api/v1/label/__name__/values" >"$d/prometheus-metric-names.json" || true
}

capture_ready_gate_metrics() {
  local d=$1
  curl -fsSG "$prom/api/v1/query" \
    --data-urlencode 'query=last_over_time({__name__=~".*readyGate.*"}[2h])' \
    >"$d/ready-gate-prometheus.json" || true
}

validate_result() {
  local d=$1 query=$2 round=$3 idx=$4 variant=$5 config=$6
  python3 - "$d" "$query" "$round" "$idx" "$variant" "$events" "$config" \
    "$source_commit" "$artifact_sha" "$expdir/identity.json" <<'PY'
import hashlib,json,pathlib,sys
d=pathlib.Path(sys.argv[1]); query=sys.argv[2]; rnd=int(sys.argv[3]); idx=int(sys.argv[4])
variant=sys.argv[5]; events=int(sys.argv[6]); config=pathlib.Path(sys.argv[7])
source=sys.argv[8]; artifact=sys.argv[9]; identity=json.load(open(sys.argv[10]))
m=json.loads((d/'measurement-result.json').read_text())
assert m['schema']=='cachekit-golden-runner-result-v1'
assert m['query']==query and m['plan_index']==idx and m['events']==events
status=m['job_status']
assert status['measurement_integrity_valid'] is True
assert status['real_job_started'] is True and status['real_job_completed'] is True
assert status['real_job_failed'] is False and status['warmup_completed'] is True
samples=m['cpu_metric_samples']
assert samples and all(x['tms']==8 for x in samples)
assert status['expected_cpu_metric_tms']==8
assert status['observed_cpu_metric_tms']==[8]*len(samples)
assert m['raw_throughput_kps']>0 and m['cores']>0 and m['throughput_kps_core']>0
assert m['cores']<=16.05 and all(0<x['cores']<=16.05 for x in samples)
assert abs(m['throughput_kps_core']-m['raw_throughput_kps']/m['cores'])/m['throughput_kps_core']<=0.005
config_sha=hashlib.sha256(config.read_bytes()).hexdigest()
assert config_sha==identity['variant_config_sha256'][variant]
assert identity['source_commit']==source and identity['artifact_sha256']==artifact
values={}
for line in config.read_text().splitlines():
    if ':' in line and not line.lstrip().startswith('#'):
        k,v=line.split(':',1); values[k.strip()]=v.strip()
assert 'execution.checkpointing.interval' not in values
assert values['pipeline.object-reuse']=='false'
assert values['state.backend.cachekit.bp-prefetch.distance']=='64'
assert values['state.backend.cachekit.bp-prefetch.multiget.chunk-size']=='64'
assert values['state.backend.cachekit.bp-prefetch.multiget.min-batch-size']=='8'
assert values['state.backend.cachekit.bp-prefetch.async-chunks.enabled']=='false'
assert values['state.backend.cachekit.bp-prefetch.ready-gated.enabled']==str(variant!='control').lower()
assert values['state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches']=='2'
assert values['state.backend.cachekit.bp-prefetch.ready-gated.timeout-us']=='5000'
assert values['state.backend.cachekit.map.cache.max-entries']=='0'
assert values['state.backend.cachekit.native.request-plane.enabled']=='false'
assert values['state.backend.cachekit.native.value-cache.enabled']=='false'
assert values['state.backend.cachekit.native.map-cache.enabled']=='false'
assert values['state.backend.cachekit.native.map-snapshot.enabled']=='false'
assert values['state.backend.cachekit.native.local-preagg.enabled']=='false'
assert values['state.backend.cachekit.native.mailbox-batch.enabled']=='false'
assert values['state.backend.cachekit.native.prefetch.enabled']=='false'
logs='\n'.join(p.read_text(errors='replace') for p in (d/'container-logs').glob('*.log'))
for forbidden in ('UnsatisfiedLinkError','OutOfMemoryError','Fatal error','Native request-plane JNI failed to load'):
    assert forbidden not in logs,forbidden
assert '[CACHEKIT VALUE PREFETCH]' in logs
result={
 'schema':'cachekit-ready-gated-prefetch-p1-leg-v1','valid':True,
 'source_commit':source,'artifact_sha256':artifact,'platform':'kunpeng','query':query,
 'round':rnd,'variant':variant,'allocated_tm_logical_cpus':16,
 'config_sha256':config_sha,'measurement':m,
 'execution_environment':identity.get('execution_environment','idle-host'),
 'numa_binding':identity.get('numa_binding'),
 'claim_boundary':identity.get('claim_boundary'),
}
(d/'result.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
PY
}

run_leg() {
  local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc
  leg=$(printf '%03d-r%d-%s-%s' "$idx" "$round" "$query" "$variant")
  d=$expdir/results/raw/$leg
  config=$expdir/variants/$variant/flink-conf.yaml
  cf=$expdir/variants/$variant/docker-compose.yml
  if [[ -f $d/LEG_COMPLETE ]]; then log "skip $leg (done)"; return 0; fi
  [[ ! -e $d ]] || mv "$d" "$expdir/results/failed/$leg-$(date +%Y%m%dT%H%M%S)"
  mkdir -p "$d"
  cp "$config" "$d/flink-conf.yaml"
  cp "$cf" "$d/docker-compose.yml"
  log "START $leg"
  cleanup
  fail_on_foreign_containers || { log "FATAL foreign containers before $leg"; exit 71; }
  if ! start_cluster "$cf" >"$d/cluster-start.log" 2>&1; then
    capture_logs "$d" "$cf"
    log "FATAL cluster start $leg"
    return 72
  fi
  if ! wait_cluster; then capture_logs "$d" "$cf"; log "FATAL cluster readiness $leg"; exit 72; fi
  python3 "$expdir/inputs/check_cpu_metric_ownership.py" \
    --compose "$cf" --project-name "$project" --expdir "$expdir" \
    --output "$d/CPU_METRIC_OWNERSHIP_PREFLIGHT.json" \
    >"$d/CPU_METRIC_OWNERSHIP_PREFLIGHT.stdout"
  set +e
  BENCH_DISABLE_WATCHDOG=1 python3 "$expdir/inputs/runner-scripts/measurement_integrity_leg.py" \
    --expdir "$expdir" --workspace "$expdir/inputs/measurement-integrity" \
    --measurement-manifest "$expdir/inputs/measurement-integrity.SHA256SUMS" \
    --compose "$cf" --jobmanager-container "${project}_jobmanager_1" \
    --compose-project "$project" --rest-url "$rest" --prometheus-url "$prom" \
    --query "$query" --variant candidate --plan-index "$idx" --events "$events" \
    --config "$config" --runtime-bundle-manifest "$runtime_manifest" \
    --expected-source-commit "$source_commit" --state-file "$d/measurement-state.json" \
    --output "$d/measurement-result.json" --query-timeout 3600 --max-retries 1 --execute \
    >"$d/runner.log" 2>&1
  rc=$?
  set -e
  capture_ready_gate_metrics "$d"
  capture_metric_raw "$d"
  capture_logs "$d" "$cf"
  if [[ $rc -ne 0 ]] || ! validate_result "$d" "$query" "$round" "$idx" "$variant" "$config" 2>"$d/validate.err"; then
    log "FATAL invalid leg $leg rc=$rc"
    return 73
  fi
  python3 "$expdir/audit_value_prefetch.py" "$d" --output "$d/VALUE_PREFETCH_AUDIT.json" \
    >"$d/value-prefetch-audit.stdout"
  python3 "$expdir/audit_ready_gate.py" "$d" --output "$d/READY_GATE_AUDIT.json" \
    >"$d/ready-gate-audit.stdout"
  sha256sum "$d"/measurement-result.json "$d"/result.json "$d"/flink-conf.yaml \
    "$d"/ready-gate-prometheus.json "$d"/VALUE_PREFETCH_AUDIT.json \
    "$d"/READY_GATE_AUDIT.json \
    >"$d/LEG.SHA256SUMS"
  touch "$d/LEG_COMPLETE"
  local kps_core
  kps_core=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["throughput_kps_core"])' "$d/measurement-result.json")
  log "OK $leg kps_core=$kps_core"
  cleanup
}

(cd "$expdir" && sha256sum -c inputs/ARTIFACTS.SHA256SUMS)
fail_on_foreign_containers || { log "FATAL foreign containers at campaign preflight"; exit 71; }
idx=0
for round in "${rounds[@]}"; do
  for query in "${queries[@]}"; do
    for variant in "${variants[@]}"; do
      idx=$((idx+1))
      run_leg "$query" "$round" "$idx" "$variant"
    done
  done
done
log "CAMPAIGN_COMPLETE legs=$idx"
touch "$expdir/CAMPAIGN_COMPLETE"
