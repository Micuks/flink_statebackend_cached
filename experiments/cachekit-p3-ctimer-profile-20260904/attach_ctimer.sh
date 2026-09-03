#!/usr/bin/env bash
set -euo pipefail

exp=/home/wuql/flink-cluster/experiments/cachekit-p3-ctimer-profile-q9-100m-kunpeng-20260904
project=ckkp5a9c4
profiler=/home/wutb/opt/async-profiler-3.0-linux-arm64
leg=001-r1-q9-control

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "$exp/logs/ctimer-attach.log"
}

wait_for_measurement() {
  local deadline=$((SECONDS + 900)) mountpoint samples
  while ((SECONDS < deadline)); do
    [[ ! -f $exp/results/raw/$leg/LEG_COMPLETE ]] || {
      log "ERROR leg completed before ctimer attachment"
      return 1
    }
    mountpoint=$(docker volume inspect -f '{{.Mountpoint}}' \
      "${project}_nexmark-logs" 2>/dev/null || true)
    if [[ -n $mountpoint && -f $mountpoint/nexmark-flink.log ]]; then
      samples=$(grep -c 'Current Cores=' "$mountpoint/nexmark-flink.log" || true)
      if ((samples >= 3)) &&
        docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager1_1" &&
        docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager2_1"; then
        log "MEASUREMENT_READY samples=$samples"
        return 0
      fi
    fi
    sleep 2
  done
  log "ERROR timed out waiting for measurement"
  return 1
}

profile_one() {
  local service=$1
  local container=${project}_${service}_1
  local out=$exp/profiles/$leg/$service init_pid cpuset pid
  mkdir -p "$out"
  init_pid=$(docker inspect -f '{{.State.Pid}}' "$container")
  cpuset=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$container")
  [[ $(docker inspect -f '{{.HostConfig.CpusetMems}}' "$container") == 0 ]]
  docker cp "$profiler/." "$container:/tmp/cachekit-async-profiler-$leg"
  pid=$(nsenter -t "$init_pid" -m -p -- sh -c '
    for proc in /proc/[0-9]*; do
      pid=${proc##*/}
      [ "$(cat "$proc/comm" 2>/dev/null)" = java ] || continue
      cmd=$(tr "\000" " " <"$proc/cmdline" 2>/dev/null) || continue
      case "$cmd" in
        *org.apache.flink.runtime.taskexecutor.TaskManagerRunner*)
          stat=$(cat "$proc/stat" 2>/dev/null) || continue
          set -- $stat
          printf "%s %s\n" "$((${14} + ${15}))" "$pid"
          ;;
      esac
    done | sort -k1,1nr | sed -n "1s/^[^ ]* //p"
  ')
  [[ $pid =~ ^[0-9]+$ ]]
  {
    printf 'service=%s\ncontainer=%s\ncpuset=%s\ntarget_pid=%s\n' \
      "$service" "$container" "$cpuset" "$pid"
    nsenter -t "$init_pid" -m -p -- sh -c \
      'printf "cmdline="; tr "\000" " " <"/proc/$1/cmdline"; printf "\n"' _ "$pid"
  } >"$out/target.txt"
  timeout 30 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    check -e ctimer "$pid" >"$out/ctimer-check.txt" 2>&1
  log "START ctimer service=$service pid=$pid cpuset=$cpuset"
  timeout 120 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    -d 90 -e ctimer -i 5000000 -t -o collapsed \
    -f "/tmp/${leg}-${service}-ctimer.collapsed" "$pid"
  docker cp "$container:/tmp/${leg}-${service}-ctimer.collapsed" \
    "$out/ctimer.collapsed"
  test -s "$out/ctimer.collapsed"
  sha256sum "$out/target.txt" "$out/ctimer-check.txt" "$out/ctimer.collapsed" \
    >"$out/PROFILE.SHA256SUMS"
  log "OK ctimer service=$service"
}

mkdir -p "$exp/profiles/$leg" "$exp/logs"
wait_for_measurement
profile_one taskmanager1 & p1=$!
profile_one taskmanager2 & p2=$!
rc=0
wait "$p1" || rc=1
wait "$p2" || rc=1
((rc == 0))
touch "$exp/profiles/PROFILE_CAPTURE_COMPLETE"
log PROFILE_CAPTURE_COMPLETE
