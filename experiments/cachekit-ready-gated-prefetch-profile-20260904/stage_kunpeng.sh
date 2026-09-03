#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-kunpeng-20260903
profile_attempt=${PROFILE_ATTEMPT:-2}
[[ $profile_attempt =~ ^[2-9]$ ]] || {
  echo "PROFILE_ATTEMPT must be an integer from 2 through 9" >&2
  exit 64
}
target_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-profile-q9-100m-kunpeng-20260904-a${profile_attempt}
source_project=ckkp5a9p1
target_project=ckkp5a9p${profile_attempt}
source_scratch=/tmp/ckkp5a9p1
target_scratch=/tmp/ckkp5a9p${profile_attempt}
target_rest_port=$((10790 + profile_attempt))
target_prom_port=$((11831 + (profile_attempt - 1) * 2))
target_push_port=$((target_prom_port + 1))
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74

ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")

"${ssh_cmd[@]}" bash -s -- \
  "$source_exp" "$target_exp" "$source_project" "$target_project" \
  "$source_scratch" "$target_scratch" "$target_cpuset" \
  "$target_rest_port" "$target_prom_port" "$target_push_port" \
  "$profile_attempt" <<'REMOTE'
set -euo pipefail
source_exp=$1
target_exp=$2
source_project=$3
target_project=$4
source_scratch=$5
target_scratch=$6
target_cpuset=$7
target_rest_port=$8
target_prom_port=$9
target_push_port=${10}
profile_attempt=${11}

[[ -f $source_exp/CAMPAIGN_COMPLETE ]]
[[ -f $source_exp/final/HOST_RESULT_COMPLETE ]]
[[ ! -e $target_exp ]] || {
  echo "target already exists: $target_exp" >&2
  exit 73
}

python3 - "$target_project" "$target_cpuset" <<'PY'
import json
import subprocess
import sys


def expand(spec):
    result = set()
    for part in spec.split(','):
        bounds = [int(value) for value in part.split('-', 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


target_project, target_spec = sys.argv[1:]
target = expand(target_spec)
for container_id in subprocess.check_output(["docker", "ps", "-q"], text=True).split():
    data = json.loads(
        subprocess.check_output(["docker", "inspect", container_id], text=True)
    )[0]
    labels = data["Config"].get("Labels") or {}
    if labels.get("com.docker.compose.project") == target_project:
        raise SystemExit("target project already has running containers")
    cpus = data["HostConfig"].get("CpusetCpus") or ""
    if not cpus or expand(cpus) & target:
        raise SystemExit(
            f"foreign container is not CPU-disjoint: {data['Name']} cpus={cpus!r}"
        )
PY

mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in control ready-d2; do
  mkdir -p "$target_exp/variants/$variant"
  cp "$source_exp/variants/$variant/flink-conf.yaml" \
    "$source_exp/variants/$variant/docker-compose.yml" \
    "$target_exp/variants/$variant/"
done
cp "$source_exp/identity.json" "$source_exp/CONFIG_DIFF_AUDIT.json" \
  "$source_exp/audit_ready_gate.py" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/summarize.py" "$source_exp/run_campaign.sh" "$target_exp/"

python3 - "$target_exp" "$source_exp" "$target_exp" \
  "$source_project" "$target_project" "$source_scratch" "$target_scratch" \
  "$target_rest_port" "$target_prom_port" "$target_push_port" "$profile_attempt" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
replacements = [(value.encode(), replacement.encode()) for value, replacement in (
    (sys.argv[2], sys.argv[3]),
    (sys.argv[4], sys.argv[5]),
    (sys.argv[6], sys.argv[7]),
    ("10789", sys.argv[8]),
    ("11823", sys.argv[9]),
    ("11824", sys.argv[10]),
)]
rewritten = []
for path in root.rglob("*"):
    if not path.is_file():
        continue
    data = path.read_bytes()
    changed = data
    for old, new in replacements:
        changed = changed.replace(old, new)
    if changed != data:
        if b"\0" in data:
            raise SystemExit(f"refusing binary rewrite: {path}")
        path.write_bytes(changed)
        rewritten.append(str(path.relative_to(root)))

identity = json.loads((root / "identity.json").read_text())
assert identity["platform"] == "kunpeng"
assert identity["queries"] == ["q9"]
assert identity["variants"] == ["control", "ready-d2"]
for variant in identity["variants"]:
    config = root / "variants" / variant / "flink-conf.yaml"
    actual = hashlib.sha256(config.read_bytes()).hexdigest()
    assert actual == identity["variant_config_sha256"][variant]
for line in (root / "inputs" / "ARTIFACTS.SHA256SUMS").read_text().splitlines():
    digest, relative = line.split("  ", 1)
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest

profile = {
    "schema": "cachekit-ready-gated-prefetch-profile-staging-v1",
    "diagnostic_only": True,
    "source_experiment": sys.argv[2],
    "target_experiment": sys.argv[3],
    "compose_project": sys.argv[5],
    "artifact_sha256": identity["artifact_sha256"],
    "source_commit": identity["source_commit"],
    "queries": identity["queries"],
    "variants": identity["variants"],
    "profile_attempt": int(sys.argv[11]),
    "ports": {
        "rest": int(sys.argv[8]),
        "prometheus": int(sys.argv[9]),
        "pushgateway": int(sys.argv[10]),
    },
    "rewritten_text_files": sorted(rewritten),
}
(root / "PROFILE_STAGING.json").write_text(
    json.dumps(profile, indent=2, sort_keys=True) + "\n"
)
print(json.dumps(profile, sort_keys=True))
PY

chmod +x "$target_exp/run_campaign.sh"
bash -n "$target_exp/run_campaign.sh"
REMOTE

if [[ ${LAUNCH:-0} == 1 ]]; then
  "${ssh_cmd[@]}" bash -s -- "$target_exp" <<'REMOTE'
set -euo pipefail
target_exp=$1
[[ ! -e $target_exp/R1.pid ]]
nohup "$target_exp/run_campaign.sh" >"$target_exp/logs/nohup-r1.log" 2>&1 &
echo $! >"$target_exp/R1.pid"
echo "launched pid=$(cat "$target_exp/R1.pid")"
REMOTE
else
  echo "staged=$target_exp"
  echo "set LAUNCH=1 to launch after a fresh NUMA-disjoint host check"
fi
