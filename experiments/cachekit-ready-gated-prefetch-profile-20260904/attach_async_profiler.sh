#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
profile_attempt=${PROFILE_ATTEMPT:-2}
[[ $profile_attempt =~ ^[2-9]$ ]] || {
  echo "PROFILE_ATTEMPT must be an integer from 2 through 9" >&2
  exit 64
}
exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-profile-q9-100m-kunpeng-20260904-a${profile_attempt}
project=ckkp5a9p${profile_attempt}
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
  local variant volume=${project}_nexmark-logs mountpoint samples service
  case $leg in
    001-r1-q9-control) variant=control ;;
    002-r1-q9-ready-d2) variant=ready-d2 ;;
    *) log "ERROR unknown leg $leg"; return 1 ;;
  esac
  while ((SECONDS < deadline)); do
    if [[ -f $d/LEG_COMPLETE ]]; then
      log "ERROR $leg completed before profiler attachment"
      return 1
    fi
    mountpoint=$(docker volume inspect -f '{{.Mountpoint}}' "$volume" 2>/dev/null || true)
    if [[ -n $mountpoint && -f $mountpoint/nexmark-flink.log ]]; then
      samples=$(grep -c 'Current Cores=' "$mountpoint/nexmark-flink.log" || true)
      if ((samples >= 3)) &&
         docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager1_1" &&
         docker ps --format '{{.Names}}' | grep -qx "${project}_taskmanager2_1"; then
        for service in taskmanager1 taskmanager2; do
          if ! docker inspect -f '{{range .Mounts}}{{println .Source}}{{end}}' \
            "${project}_${service}_1" | \
            grep -Fxq "$exp/variants/$variant/flink-conf.yaml"; then
            continue 2
          fi
        done
        log "MEASUREMENT_READY leg=$leg variant=$variant metric_samples=$samples"
        return 0
      fi
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
  local container_id init_pid cpuset pid cpu_event=cpu
  mkdir -p "$out"
  container_id=$(docker inspect -f '{{.Id}}' "$container")
  init_pid=$(docker inspect -f '{{.State.Pid}}' "$container")
  cpuset=$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$container")
  [[ $(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$container") == "$project" ]]

  docker cp "$profiler/." "$container:/tmp/cachekit-async-profiler-$leg"
  # procps can stall while walking the many JVM threads in these containers. Read only the
  # process-level procfs records and rank TaskManager JVMs by accumulated user+system ticks.
  pid=$(nsenter -t "$init_pid" -m -p -- sh -c '
    for proc in /proc/[0-9]*; do
      pid=${proc##*/}
      comm=$(cat "$proc/comm" 2>/dev/null) || continue
      [ "$comm" = java ] || continue
      cmd=$(tr "\000" " " <"$proc/cmdline" 2>/dev/null) || continue
      case "$cmd" in
        *org.apache.flink.runtime.taskexecutor.TaskManagerRunner*)
          stat=$(cat "$proc/stat" 2>/dev/null) || continue
          set -- $stat
          ticks=$((${14} + ${15}))
          printf "%s %s\n" "$ticks" "$pid"
          ;;
      esac
    done | sort -k1,1nr | sed -n "1s/^[^ ]* //p"
  ')
  [[ $pid =~ ^[0-9]+$ ]]
  {
    printf 'leg=%s\nservice=%s\ncontainer=%s\ncontainer_id=%s\ncpuset=%s\ntarget_pid=%s\n' \
      "$leg" "$service" "$container" "$container_id" "$cpuset" "$pid"
    printf 'container_init_host_pid=%s\n' "$init_pid"
    nsenter -t "$init_pid" -m -p -- sh -c '
      printf "comm="; cat "/proc/$1/comm"
      printf "cmdline="; tr "\000" " " <"/proc/$1/cmdline"; printf "\n"
      printf "stat="; cat "/proc/$1/stat"
      printf "status:\n"; cat "/proc/$1/status"
    ' _ "$pid"
  } >"$out/target.txt"

  if ! timeout 30 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    check -e cpu "$pid" >"$out/cpu-check.txt" 2>&1; then
    cpu_event=wall
    timeout 30 nsenter -t "$init_pid" -m -p -- \
      "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
      check -e wall "$pid" >"$out/wall-check.txt" 2>&1
  fi
  printf 'event=%s\n' "$cpu_event" >"$out/cpu-event.txt"
  log "START $cpu_event leg=$leg service=$service pid=$pid cpuset=$cpuset"
  timeout 90 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    -d 60 -e "$cpu_event" -i 5000000 -t -o collapsed \
    -f "/tmp/${leg}-${service}-cpu.collapsed" "$pid"
  docker cp "$container:/tmp/${leg}-${service}-cpu.collapsed" "$out/cpu.collapsed"
  test -s "$out/cpu.collapsed"
  log "OK cpu leg=$leg service=$service"

  log "START alloc leg=$leg service=$service pid=$pid"
  timeout 30 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    check -e alloc "$pid" >"$out/alloc-check.txt" 2>&1
  timeout 75 nsenter -t "$init_pid" -m -p -- \
    "/tmp/cachekit-async-profiler-$leg/bin/asprof" \
    -d 45 -e alloc -t -o collapsed \
    -f "/tmp/${leg}-${service}-alloc.collapsed" "$pid"
  docker cp "$container:/tmp/${leg}-${service}-alloc.collapsed" "$out/alloc.collapsed"
  test -s "$out/alloc.collapsed"
  local checksum_inputs=(
    "$out/target.txt"
    "$out/cpu-event.txt"
    "$out/cpu-check.txt"
    "$out/cpu.collapsed"
    "$out/alloc-check.txt"
    "$out/alloc.collapsed"
  )
  [[ ! -f $out/wall-check.txt ]] || checksum_inputs+=("$out/wall-check.txt")
  sha256sum "${checksum_inputs[@]}" >"$out/PROFILE.SHA256SUMS"
  log "OK alloc leg=$leg service=$service"
}

profile_complete() {
  local leg=$1 service out
  for service in taskmanager1 taskmanager2; do
    out=$exp/profiles/$leg/$service
    [[ -s $out/cpu.collapsed && -s $out/alloc.collapsed && \
       -f $out/PROFILE.SHA256SUMS ]] || return 1
    sha256sum -c "$out/PROFILE.SHA256SUMS" >/dev/null || return 1
  done
}

profile_leg() {
  local leg=$1 p1 p2 rc=0
  if profile_complete "$leg"; then
    log "SKIP completed profiles leg=$leg"
    return 0
  fi
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
