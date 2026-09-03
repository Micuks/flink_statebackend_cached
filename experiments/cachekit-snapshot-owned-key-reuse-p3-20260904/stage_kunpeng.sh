#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-inline-record-multiget-p2-q9-100m-kunpeng-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-snapshot-owned-key-reuse-p3-q9-100m-kunpeng-20260904
source_project=ckkp5a9i2
target_project=ckkp5a9k3
source_scratch=/tmp/ckkp5a9i2
target_scratch=/tmp/ckkp5a9k3
target_rest_port=10796
target_prom_port=11841
target_push_port=11842
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74
source_commit=18cfb52bdb6e740d9ae716da0a03761481e885e2
artifact_sha=0a1981cbd3591b4c99dbd5d7d3a32170ab6c630fa1dcdb4faa16005b9acf5f49

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p3-aarch64.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")
stage=/tmp/cachekit-snapshot-owned-key-reuse-p3-stage-20260904
"${ssh_cmd[@]}" mkdir -p "$stage"
scp -o ControlPath="$control_path" -o BatchMode=yes \
  "$candidate" "$script_dir/audit_snapshot_key_reuse.py" "$script_dir/summarize.py" \
  "$host:$stage/"

"${ssh_cmd[@]}" bash -s -- \
  "$source_exp" "$target_exp" "$source_project" "$target_project" \
  "$source_scratch" "$target_scratch" "$target_rest_port" \
  "$target_prom_port" "$target_push_port" "$target_cpuset" \
  "$source_commit" "$artifact_sha" "$stage" <<'REMOTE'
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
source_commit=${11}
artifact_sha=${12}
stage=${13}

[[ -f $source_exp/CAMPAIGN_COMPLETE ]]
[[ -f $source_exp/results/raw/001-r1-q9-control/LEG_COMPLETE ]]
[[ -f $source_exp/results/raw/002-r1-q9-immediate/LEG_COMPLETE ]]
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
  "$target_exp/variants/reuse"
cp -a "$source_exp/inputs" "$target_exp/inputs"
cp "$source_exp/variants/control/flink-conf.yaml" \
  "$source_exp/variants/control/docker-compose.yml" \
  "$target_exp/variants/control/"
cp "$source_exp/variants/control/flink-conf.yaml" \
  "$target_exp/variants/reuse/flink-conf.yaml"
cp "$source_exp/variants/control/docker-compose.yml" \
  "$target_exp/variants/reuse/docker-compose.yml"
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_snapshot_key_reuse.py" "$stage/summarize.py" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p3-aarch64.jar" \
  "$target_exp/inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

python3 - "$target_exp" "$source_exp" "$target_exp" \
  "$source_project" "$target_project" "$source_scratch" "$target_scratch" \
  "$target_rest_port" "$target_prom_port" "$target_push_port" \
  "$source_commit" "$artifact_sha" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
source_exp = sys.argv[2]
target_exp = sys.argv[3]
source_project = sys.argv[4]
target_project = sys.argv[5]
source_scratch = sys.argv[6]
target_scratch = sys.argv[7]
rest_port, prom_port, push_port = sys.argv[8:11]
source_commit, artifact_sha = sys.argv[11:13]

replacements = [(value.encode(), replacement.encode()) for value, replacement in (
    (source_exp, target_exp),
    (source_project, target_project),
    (source_scratch, target_scratch),
    ("10795", rest_port),
    ("11839", prom_port),
    ("11840", push_port),
)]
rewrite_paths = (
    root / "run_campaign.sh",
    root / "variants" / "control" / "docker-compose.yml",
    root / "variants" / "reuse" / "docker-compose.yml",
)
for path in rewrite_paths:
    data = path.read_bytes()
    changed = data
    for old, new in replacements:
        changed = changed.replace(old, new)
    if b"\0" in data:
        raise SystemExit(f"refusing binary rewrite: {path}")
    path.write_bytes(changed)

reuse_compose = root / "variants" / "reuse" / "docker-compose.yml"
text = reuse_compose.read_text()
old_mount = f"{target_exp}/variants/control/flink-conf.yaml"
new_mount = f"{target_exp}/variants/reuse/flink-conf.yaml"
if text.count(old_mount) != 4:
    raise SystemExit(
        f"reuse compose config mount count {text.count(old_mount)}; expected 4"
    )
reuse_compose.write_text(text.replace(old_mount, new_mount))

for variant in ("control", "reuse"):
    path = root / "variants" / variant / "flink-conf.yaml"
    text = path.read_text()
    old_header = (
        "# Generated P2 inline record-key MultiGet variant: control\n"
        "# Source: a7aa558791becb34164b27c787ab6cffdebf0dc2\n"
    )
    if text.count(old_header) != 1:
        raise SystemExit(f"P2 header not unique in {path}")
    text = text.replace(
        old_header,
        f"# Generated P3 snapshot-owned key reuse variant: {variant}\n"
        f"# Source: {source_commit}\n",
    )
    immediate = "state.backend.cachekit.bp-prefetch.immediate-record.enabled: false"
    if text.count(immediate) != 1:
        raise SystemExit(f"P2 control setting not unique in {path}")
    if "state.backend.cachekit.map.snapshot.owned-key-reuse.enabled" in text:
        raise SystemExit(f"snapshot-owned key reuse already present in {path}")
    text += (
        "\n# P3 snapshot-owned internal-key reuse and lazy exposed-key copy.\n"
        "state.backend.cachekit.map.snapshot.owned-key-reuse.enabled: "
        + ("true" if variant == "reuse" else "false")
        + "\n"
    )
    path.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()
old_capture_name = "capture_immediate_record_metrics"
if text.count(old_capture_name) != 2:
    raise SystemExit(
        f"runner replacement count {text.count(old_capture_name)} for {old_capture_name!r}"
    )
text = text.replace(old_capture_name, "capture_snapshot_key_reuse_metrics")
special_replacements = (
    ("audit_immediate_record.py", "audit_snapshot_key_reuse.py", 1),
    ("IMMEDIATE_RECORD_AUDIT.json", "SNAPSHOT_KEY_REUSE_AUDIT.json", 2),
    ("immediate-record-audit.stdout", "snapshot-key-reuse-audit.stdout", 1),
    ('"$d"/immediate-record-prometheus.json', '"$d"/snapshot-key-reuse-prometheus.json', 1),
)
for old, new, expected_count in special_replacements:
    if text.count(old) != expected_count:
        raise SystemExit(
            f"runner replacement count {text.count(old)} for {old!r}; "
            f"expected {expected_count}"
        )
    text = text.replace(old, new)
required = {
    "source_commit=a7aa558791becb34164b27c787ab6cffdebf0dc2": f"source_commit={source_commit}",
    "artifact_sha=164411a963b78ccd508524c9ca4e1a0d50b497830394a59c016ed3ef644fd741": f"artifact_sha={artifact_sha}",
    "variants=(control immediate)": "variants=(control reuse)",
    'query=last_over_time({__name__=~".*immediateRecord.*"}[2h])': 'query=last_over_time({__name__=~".*map_snapshot_.*"}[2h])',
    '"$d/immediate-record-prometheus.json"': '"$d/snapshot-key-reuse-prometheus.json"',
    "assert values['state.backend.cachekit.bp-prefetch.immediate-record.enabled']==str(variant=='immediate').lower()": "assert values['state.backend.cachekit.bp-prefetch.immediate-record.enabled']=='false'",
    "'schema':'cachekit-inline-record-multiget-p2-leg-v1'": "'schema':'cachekit-snapshot-owned-key-reuse-p3-leg-v1'",
}
for old, new in required.items():
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
needle = "assert values['state.backend.cachekit.bp-prefetch.immediate-record.enabled']=='false'\n"
addition = (
    needle
    + "assert values['state.backend.cachekit.map.snapshot.owned-key-reuse.enabled']==str(variant=='reuse').lower()\n"
)
if text.count(needle) != 1:
    raise SystemExit("runner snapshot-owned key assertion insertion point not unique")
runner.write_text(text.replace(needle, addition))

runtime_path = root / "inputs" / "runtime" / "RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (
            root / "inputs" / "runtime" / artifact["container_filename"]
        ).stat().st_size
runtime_path.write_text(json.dumps(runtime, indent=2, sort_keys=True) + "\n")

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-snapshot-owned-key-reuse-p3-screen-v1",
    "phase": "q9-snapshot-owned-key-reuse-paired-screen",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (root / "inputs" / "runtime" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").stat().st_size,
    "primary_control": "control",
    "variants": ["control", "reuse"],
    "execution_environment": "idle-host-numa-isolated-quick-validation",
    "claim_boundary": "same-host paired quick validation on idle Kunpeng NUMA node 0 with fixed CPU and memory binding",
})
identity["artifact_build"] = {
    "p2_runtime_sha256": "164411a963b78ccd508524c9ca4e1a0d50b497830394a59c016ed3ef644fd741",
    "embedded_arm_native_sha256": "6765775306c7ed00b1606de5340f0ba18ae5adc8fe242b49b336f79f96749fe2",
    "non_cachekit_entries_identical_to_p2": True,
}
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
treatment = config(root / "variants" / "reuse" / "flink-conf.yaml")
keys = sorted(set(control) | set(treatment))
differences = {
    key: {"control": control.get(key), "reuse": treatment.get(key)}
    for key in keys
    if control.get(key) != treatment.get(key)
}
allowed = ["state.backend.cachekit.map.snapshot.owned-key-reuse.enabled"]
audit = {
    "schema": "cachekit-snapshot-owned-key-reuse-p3-config-diff-v1",
    "actual_differences": differences,
    "allowed_difference_keys": allowed,
    "unexpected_difference_keys": sorted(set(differences) - set(allowed)),
    "valid": sorted(differences) == allowed,
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
assert hashlib.sha256(
    (root / "inputs" / "runtime" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()
).hexdigest() == artifact_sha
print(json.dumps({
    "target": str(root),
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "config_sha256": identity["variant_config_sha256"],
    "differences": differences,
}, sort_keys=True))
PY

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/runtime/libcachekit_native_request_plane_jni.so \
    >inputs/ARTIFACTS.SHA256SUMS
  sha256sum -c inputs/ARTIFACTS.SHA256SUMS
)
chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_snapshot_key_reuse.py" \
  "$target_exp/summarize.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_snapshot_key_reuse.py" "$target_exp/summarize.py"
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
