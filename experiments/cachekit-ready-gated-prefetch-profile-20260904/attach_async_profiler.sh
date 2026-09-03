#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-profile-q9-100m-kunpeng-20260904
project=ckkp5a9pr
profiler=/home/wutb/opt/async-profiler-3.0-linux-arm64

ssh -S "$control_path" -o BatchMode=yes "$host" bash -s -- \
  "$exp" "$project" "$profiler" <<'REMOTE'
set -euo pipefail
exp=$1
project=$2
profiler=$3

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$exp/logs/profile-attach.log"
}

wait_for_measurement() {
  local leg=$1 d=$exp/results/raw/$leg deadline=$((SECONDS + 900))
  while ((SECONDS < deadline)); do
    if [[ -f $d/LEG_COMPLETE ]]; then
      log "ERROR $leg completed before profiler attachment"
      return 1
    fi
    if [[ -f $d/runner.log ]] &&
       grep -q 'Start to monitor metrics until job is finished' "$d/runner.log" &&
       docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager1_1" &&
       docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager2_1"; then
      return 0
    fi
    sleep 2
  done
  log "ERROR timed out waiting for $leg measurement window"
  return 1
}

profile_one() {
  local leg=$1 service=$2
  local container=${project}_${service}_1
  local out=$exp/profiles/$leg/$service
  local container_id cpuset pid
  mkdir -p "$out"
  container_id=$(docker inspect -f '{{.Id}}' "$container")
  cpuset=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$container")
  [[ $(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$container") == "$project" ]]

  docker exec "$container" rm -rf /tmp/cachekit-async-profiler
  docker cp "$profiler/." "$container:/tmp/cachekit-async-profiler"
  pid=$(docker exec "$container" sh -c \
    "ps -eo pid=,pcpu=,args= | awk '/org.apache.flink.runtime.taskexecutor.TaskManagerRunner/ {print \$1,\$2}' | sort -k2,2nr | awk 'NR==1 {print \$1}'")
  [[ $pid =~ ^[0-9]+$ ]]
  {
    printf 'leg=%s\nservice=%s\ncontainer=%s\ncontainer_id=%s\ncpuset=%s\ntarget_pid=%s\n' \
      "$leg" "$service" "$container" "$container_id" "$cpuset" "$pid"
    docker exec "$container" ps -eo pid=,ppid=,pcpu=,etimes=,args=
  } >"$out/target.txt"

  log "START cpu leg=$leg service=$service pid=$pid cpuset=$cpuset"
  docker exec "$container" /tmp/cachekit-async-profiler/bin/asprof \
    -d 60 -e cpu -i 5000000 -t -o collapsed \
    -f "/tmp/${leg}-${service}-cpu.collapsed" "$pid"
  docker cp "$container:/tmp/${leg}-${service}-cpu.collapsed" "$out/cpu.collapsed"
  test -s "$out/cpu.collapsed"
  log "OK cpu leg=$leg service=$service"

  log "START alloc leg=$leg service=$service pid=$pid"
  docker exec "$container" /tmp/cachekit-async-profiler/bin/asprof \
    -d 45 -e alloc -t -o collapsed \
    -f "/tmp/${leg}-${service}-alloc.collapsed" "$pid"
  docker cp "$container:/tmp/${leg}-${service}-alloc.collapsed" "$out/alloc.collapsed"
  test -s "$out/alloc.collapsed"
  sha256sum "$out/target.txt" "$out/cpu.collapsed" "$out/alloc.collapsed" \
    >"$out/PROFILE.SHA256SUMS"
  log "OK alloc leg=$leg service=$service"
}

profile_leg() {
  local leg=$1 p1 p2 rc=0
  wait_for_measurement "$leg"
  profile_one "$leg" taskmanager1 & p1=$!
  profile_one "$leg" taskmanager2 & p2=$!
  wait "$p1" || rc=1
  wait "$p2" || rc=1
  ((rc == 0)) || return "$rc"
}

mkdir -p "$exp/profiles" "$exp/logs"
profile_leg 001-r1-q9-control
profile_leg 002-r1-q9-ready-d2
log 'PROFILE_CAPTURE_COMPLETE'
touch "$exp/profiles/PROFILE_CAPTURE_COMPLETE"
REMOTE
