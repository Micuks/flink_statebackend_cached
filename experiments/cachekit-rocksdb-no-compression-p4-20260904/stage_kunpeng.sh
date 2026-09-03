#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-snapshot-owned-key-reuse-p3-q9-100m-kunpeng-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-q9-100m-kunpeng-20260904
source_project=ckkp5a9k3
target_project=ckkp5a9n4
source_scratch=/tmp/ckkp5a9k3
target_scratch=/tmp/ckkp5a9n4
target_rest_port=10798
target_prom_port=11845
target_push_port=11846
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74
source_commit=a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4
artifact_sha=3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p4-aarch64.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")
stage=/tmp/cachekit-rocksdb-no-compression-p4-stage-20260904
"${ssh_cmd[@]}" mkdir -p "$stage"
scp -o ControlPath="$control_path" -o BatchMode=yes \
  "$candidate" "$script_dir/audit_compression.py" "$script_dir/summarize.py" \
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
[[ -f $source_exp/results/raw/002-r1-q9-reuse/LEG_COMPLETE ]]
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }

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
    data = json.loads(subprocess.check_output(["docker", "inspect", container_id], text=True))[0]
    labels = data["Config"].get("Labels") or {}
    if labels.get("com.docker.compose.project") == target_project:
        raise SystemExit("target project already has running containers")
    cpus = data["HostConfig"].get("CpusetCpus") or ""
    if not cpus or expand(cpus) & target:
        raise SystemExit(f"foreign container is not CPU-disjoint: {data['Name']} cpus={cpus!r}")
PY

mkdir -p "$target_exp/logs" "$target_exp/variants/control" \
  "$target_exp/variants/no-compression"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in control no-compression; do
  cp "$source_exp/variants/control/flink-conf.yaml" \
    "$target_exp/variants/$variant/flink-conf.yaml"
  cp "$source_exp/variants/control/docker-compose.yml" \
    "$target_exp/variants/$variant/docker-compose.yml"
done
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_compression.py" "$stage/summarize.py" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p4-aarch64.jar" \
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
source_exp, target_exp = sys.argv[2:4]
source_project, target_project = sys.argv[4:6]
source_scratch, target_scratch = sys.argv[6:8]
rest_port, prom_port, push_port = sys.argv[8:11]
source_commit, artifact_sha = sys.argv[11:13]

replacements = [(value.encode(), replacement.encode()) for value, replacement in (
    (source_exp, target_exp),
    (source_project, target_project),
    (source_scratch, target_scratch),
    ("10796", rest_port),
    ("11841", prom_port),
    ("11842", push_port),
)]
rewrite_paths = [root / "run_campaign.sh"]
rewrite_paths.extend(root / "variants" / variant / "docker-compose.yml" for variant in ("control", "no-compression"))
for path in rewrite_paths:
    data = path.read_bytes()
    changed = data
    for old, new in replacements:
        changed = changed.replace(old, new)
    if b"\0" in data:
        raise SystemExit(f"refusing binary rewrite: {path}")
    path.write_bytes(changed)

for variant in ("control", "no-compression"):
    compose = root / "variants" / variant / "docker-compose.yml"
    text = compose.read_text()
    old_mount = f"{target_exp}/variants/control/flink-conf.yaml"
    new_mount = f"{target_exp}/variants/{variant}/flink-conf.yaml"
    if text.count(old_mount) != 4:
        raise SystemExit(f"compose config mount count {text.count(old_mount)} in {compose}; expected 4")
    compose.write_text(text.replace(old_mount, new_mount))

for variant in ("control", "no-compression"):
    path = root / "variants" / variant / "flink-conf.yaml"
    text = path.read_text()
    old_header = (
        "# Generated P3 snapshot-owned key reuse variant: control\n"
        "# Source: 18cfb52bdb6e740d9ae716da0a03761481e885e2\n"
    )
    if text.count(old_header) != 1:
        raise SystemExit(f"P3 header not unique in {path}")
    text = text.replace(
        old_header,
        f"# Generated P4 RocksDB compression variant: {variant}\n# Source: {source_commit}\n",
    )
    if "state.backend.rocksdb.compression.type" in text:
        raise SystemExit(f"compression type already present in {path}")
    mode = "NO_COMPRESSION" if variant == "no-compression" else "SNAPPY_COMPRESSION"
    text += f"\n# P4 RocksDB SST compression mode.\nstate.backend.rocksdb.compression.type: {mode}\n"
    path.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()
required = {
    "source_commit=18cfb52bdb6e740d9ae716da0a03761481e885e2": f"source_commit={source_commit}",
    "artifact_sha=0a1981cbd3591b4c99dbd5d7d3a32170ab6c630fa1dcdb4faa16005b9acf5f49": f"artifact_sha={artifact_sha}",
    "variants=(control reuse)": "variants=(control no-compression)",
    "assert values['state.backend.cachekit.map.snapshot.owned-key-reuse.enabled']==str(variant=='reuse').lower()": "assert values['state.backend.cachekit.map.snapshot.owned-key-reuse.enabled']=='false'\nassert values['state.backend.rocksdb.compression.type']==('NO_COMPRESSION' if variant=='no-compression' else 'SNAPPY_COMPRESSION')",
    "'schema':'cachekit-snapshot-owned-key-reuse-p3-leg-v1'": "'schema':'cachekit-rocksdb-no-compression-p4-leg-v1'",
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc": "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode",
    'python3 "$expdir/audit_snapshot_key_reuse.py" "$d" --output "$d/SNAPSHOT_KEY_REUSE_AUDIT.json" \\\n    >"$d/snapshot-key-reuse-audit.stdout"': 'if [[ $variant == no-compression ]]; then compression_mode=NO_COMPRESSION; else compression_mode=SNAPPY_COMPRESSION; fi\n  python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --output "$d/COMPRESSION_AUDIT.json" \\\n    >"$d/compression-audit.stdout"',
    '"$d"/SNAPSHOT_KEY_REUSE_AUDIT.json \\': '"$d"/COMPRESSION_AUDIT.json \\',
}
for old, new in required.items():
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
runner.write_text(text)

runtime_path = root / "inputs" / "runtime" / "RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (root / "inputs" / "runtime" / artifact["container_filename"]).stat().st_size
runtime_path.write_text(json.dumps(runtime, indent=2, sort_keys=True) + "\n")

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-rocksdb-no-compression-p4-screen-v1",
    "phase": "q9-rocksdb-no-compression-paired-screen",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (root / "inputs" / "runtime" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").stat().st_size,
    "primary_control": "control",
    "variants": ["control", "no-compression"],
    "execution_environment": "idle-host-numa-isolated-quick-validation",
    "claim_boundary": "same-host paired quick validation on idle Kunpeng NUMA node 0 with fixed CPU and memory binding",
})
identity["artifact_build"] = {
    "p3_runtime_sha256": "0a1981cbd3591b4c99dbd5d7d3a32170ab6c630fa1dcdb4faa16005b9acf5f49",
    "rocksdb_overlay_source_commit": source_commit,
    "non_overlay_entries_identical_to_p3": True,
}
identity["variant_config_sha256"] = {
    variant: hashlib.sha256((root / "variants" / variant / "flink-conf.yaml").read_bytes()).hexdigest()
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
treatment = config(root / "variants" / "no-compression" / "flink-conf.yaml")
keys = sorted(set(control) | set(treatment))
differences = {key: {"control": control.get(key), "no-compression": treatment.get(key)} for key in keys if control.get(key) != treatment.get(key)}
allowed = ["state.backend.rocksdb.compression.type"]
audit = {
    "schema": "cachekit-rocksdb-no-compression-p4-config-diff-v1",
    "actual_differences": differences,
    "allowed_difference_keys": allowed,
    "unexpected_difference_keys": sorted(set(differences) - set(allowed)),
    "valid": sorted(differences) == allowed,
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")

assert hashlib.sha256((root / "inputs" / "runtime" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()).hexdigest() == artifact_sha
print(json.dumps({"target": str(root), "source_commit": source_commit, "artifact_sha256": artifact_sha, "config_sha256": identity["variant_config_sha256"], "differences": differences}, sort_keys=True))
PY

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/runtime/libcachekit_native_request_plane_jni.so \
    >inputs/ARTIFACTS.SHA256SUMS
  sha256sum -c inputs/ARTIFACTS.SHA256SUMS
)
chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_compression.py" \
  "$target_exp/summarize.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_compression.py" "$target_exp/summarize.py"
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
