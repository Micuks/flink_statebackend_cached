#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-kunpeng-20260903
target_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-depth4-q9-100m-kunpeng-20260904
source_project=ckkp5a9p1
target_project=ckkp5a9d4
source_scratch=/tmp/ckkp5a9p1
target_scratch=/tmp/ckkp5a9d4
target_rest_port=10794
target_prom_port=11837
target_push_port=11838
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74

ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")

"${ssh_cmd[@]}" bash -s -- \
  "$source_exp" "$target_exp" "$source_project" "$target_project" \
  "$source_scratch" "$target_scratch" "$target_rest_port" \
  "$target_prom_port" "$target_push_port" "$target_cpuset" <<'REMOTE'
set -euo pipefail
source_exp=$1
target_exp=$2
source_project=$3
target_project=$4
source_scratch=$5
target_scratch=$6
target_rest_port=$7
target_prom_port=$8
target_push_port=$9
target_cpuset=${10}

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

mkdir -p "$target_exp/logs" "$target_exp/variants/control" \
  "$target_exp/variants/ready-d4"
cp -a "$source_exp/inputs" "$target_exp/inputs"
cp "$source_exp/variants/control/flink-conf.yaml" \
  "$source_exp/variants/control/docker-compose.yml" \
  "$target_exp/variants/control/"
cp "$source_exp/variants/ready-d2/flink-conf.yaml" \
  "$target_exp/variants/ready-d4/flink-conf.yaml"
cp "$source_exp/variants/ready-d2/docker-compose.yml" \
  "$target_exp/variants/ready-d4/docker-compose.yml"
cp "$source_exp/identity.json" "$source_exp/CONFIG_DIFF_AUDIT.json" \
  "$source_exp/audit_ready_gate.py" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/summarize.py" "$source_exp/run_campaign.sh" "$target_exp/"

python3 - "$target_exp" "$source_exp" "$target_exp" \
  "$source_project" "$target_project" "$source_scratch" "$target_scratch" \
  "$target_rest_port" "$target_prom_port" "$target_push_port" <<'PY'
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
    ("ready-d2", "ready-d4"),
)]
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

candidate = root / "variants" / "ready-d4" / "flink-conf.yaml"
text = candidate.read_text()
old = "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches: 2"
new = "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches: 4"
if text.count(old) != 1:
    raise SystemExit("candidate depth setting not unique")
candidate.write_text(text.replace(old, new))

runner = root / "run_campaign.sh"
text = runner.read_text()
old = "assert values['state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches']=='2'"
new = "assert values['state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches']==('2' if variant=='control' else '4')"
if text.count(old) != 1:
    raise SystemExit("runner depth assertion not unique")
runner.write_text(text.replace(old, new))

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-ready-gated-prefetch-depth4-screen-v1",
    "phase": "q9-ready-depth-four-paired-screen",
    "primary_control": "control",
    "variants": ["control", "ready-d4"],
})
identity["variant_config_sha256"] = {
    variant: hashlib.sha256(
        (root / "variants" / variant / "flink-conf.yaml").read_bytes()
    ).hexdigest()
    for variant in identity["variants"]
}
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

def config(path):
    values = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values

control = config(root / "variants" / "control" / "flink-conf.yaml")
treatment = config(candidate)
keys = sorted(set(control) | set(treatment))
differences = {
    key: {"control": control.get(key), "ready-d4": treatment.get(key)}
    for key in keys
    if control.get(key) != treatment.get(key)
}
allowed = [
    "state.backend.cachekit.bp-prefetch.ready-gated.enabled",
    "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches",
]
unexpected = sorted(set(differences) - set(allowed))
audit = {
    "schema": "cachekit-ready-gated-prefetch-depth4-config-diff-v1",
    "actual_differences": differences,
    "allowed_difference_keys": allowed,
    "unexpected_difference_keys": unexpected,
    "valid": not unexpected and sorted(differences) == sorted(allowed),
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audit, indent=2, sort_keys=True) + "\n"
)

for variant in identity["variants"]:
    actual = hashlib.sha256(
        (root / "variants" / variant / "flink-conf.yaml").read_bytes()
    ).hexdigest()
    assert actual == identity["variant_config_sha256"][variant]
for line in (root / "inputs" / "ARTIFACTS.SHA256SUMS").read_text().splitlines():
    digest, relative = line.split("  ", 1)
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest
print(json.dumps({
    "target": str(root),
    "artifact_sha256": identity["artifact_sha256"],
    "config_sha256": identity["variant_config_sha256"],
    "differences": differences,
}, sort_keys=True))
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
