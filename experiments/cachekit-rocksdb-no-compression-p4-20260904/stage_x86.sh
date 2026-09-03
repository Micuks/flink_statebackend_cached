#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-x86-20260903
target_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-q9-100m-x86-20260904
source_project=ckx865a9p1
target_project=ckx865a9n4
source_scratch=/tmp/ckx865a9p1
target_scratch=/tmp/ckx865a9n4
target_rest_port=10790
target_prom_port=11825
target_push_port=11826
source_commit=a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4
artifact_sha=66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p4-x86.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

stage=/tmp/cachekit-rocksdb-no-compression-p4-x86-stage-20260904
ssh -o BatchMode=yes "$host" mkdir -p "$stage"
scp -q "$candidate" "$script_dir/audit_compression.py" "$script_dir/summarize.py" \
  "$host:$stage/"

ssh -o BatchMode=yes "$host" bash -s -- \
  "$source_exp" "$target_exp" "$source_project" "$target_project" \
  "$source_scratch" "$target_scratch" "$target_rest_port" \
  "$target_prom_port" "$target_push_port" "$source_commit" "$artifact_sha" \
  "$stage" <<'REMOTE'
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
source_commit=${10}
artifact_sha=${11}
stage=${12}

[[ -f $source_exp/CAMPAIGN_COMPLETE ]]
[[ -f $source_exp/results/raw/001-r1-q9-control/LEG_COMPLETE ]]
[[ -f $source_exp/results/raw/002-r1-q9-ready-d2/LEG_COMPLETE ]]
[[ -z $(docker ps -q) ]] || { echo "x86 host has running containers" >&2; exit 72; }
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }

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
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p4-x86.jar" \
  "$target_exp/inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

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
    ("10788", rest_port),
    ("11821", prom_port),
    ("11822", push_port),
)]
paths = [root / "run_campaign.sh"]
paths.extend(root / "variants" / variant / "docker-compose.yml" for variant in ("control", "no-compression"))
for path in paths:
    data = path.read_bytes()
    for old, new in replacements:
        data = data.replace(old, new)
    path.write_bytes(data)

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
        "# Generated P1 ready-gated prefetch variant: control\n"
        "# Source: 5a9d1e656715a403afac72ee1a516876ddbfb7f1\n"
    )
    if text.count(old_header) != 1:
        raise SystemExit(f"P1 header not unique in {path}")
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
    "source_commit=5a9d1e656715a403afac72ee1a516876ddbfb7f1": f"source_commit={source_commit}",
    "artifact_sha=58676b125fe20a5e7f9994e4531f06f8f076e1b3fc150ee1f222fabbe4e20602": f"artifact_sha={artifact_sha}",
    "variants=(control ready-d2)": "variants=(control no-compression)",
    "assert values['state.backend.cachekit.bp-prefetch.ready-gated.enabled']==str(variant!='control').lower()": "assert values['state.backend.cachekit.bp-prefetch.ready-gated.enabled']=='false'\nassert values['state.backend.rocksdb.compression.type']==('NO_COMPRESSION' if variant=='no-compression' else 'SNAPPY_COMPRESSION')",
    "'schema':'cachekit-ready-gated-prefetch-p1-leg-v1'": "'schema':'cachekit-rocksdb-no-compression-p4-leg-v1'",
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc": "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode",
    'python3 "$expdir/audit_ready_gate.py" "$d" --output "$d/READY_GATE_AUDIT.json" \\\n    >"$d/ready-gate-audit.stdout"': 'if [[ $variant == no-compression ]]; then compression_mode=NO_COMPRESSION; else compression_mode=SNAPPY_COMPRESSION; fi\n  python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --output "$d/COMPRESSION_AUDIT.json" \\\n    >"$d/compression-audit.stdout"',
    '"$d"/READY_GATE_AUDIT.json \\': '"$d"/COMPRESSION_AUDIT.json \\',
}
for old, new in required.items():
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
runner.write_text(text)

runtime_path = root / "inputs" / "artifacts" / "opt" / "RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (root / "inputs" / "artifacts" / "opt" / artifact["container_filename"]).stat().st_size
runtime_path.write_text(json.dumps(runtime, indent=2, sort_keys=True) + "\n")

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-rocksdb-no-compression-p4-screen-v1",
    "phase": "q9-rocksdb-no-compression-paired-screen",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (root / "inputs" / "artifacts" / "opt" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").stat().st_size,
    "primary_control": "control",
    "variants": ["control", "no-compression"],
    "execution_environment": "idle-host-quick-validation",
    "claim_boundary": "same-host paired quick validation on idle cloud x86; storage differs from Kunpeng and is not used for cross-host uplift",
})
identity["artifact_build"] = {
    "p1_x86_runtime_sha256": "58676b125fe20a5e7f9994e4531f06f8f076e1b3fc150ee1f222fabbe4e20602",
    "rocksdb_overlay_source_commit": source_commit,
    "non_overlay_entries_identical_to_p1": True,
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
assert hashlib.sha256((root / "inputs" / "artifacts" / "opt" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()).hexdigest() == artifact_sha
print(json.dumps({"target": str(root), "source_commit": source_commit, "artifact_sha256": artifact_sha, "config_sha256": identity["variant_config_sha256"], "differences": differences}, sort_keys=True))
PY

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/artifacts/opt/libcachekit_native_request_plane_jni.so \
    >inputs/ARTIFACTS.SHA256SUMS
  sha256sum -c inputs/ARTIFACTS.SHA256SUMS
)
chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_compression.py" \
  "$target_exp/summarize.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_compression.py" "$target_exp/summarize.py"
REMOTE

echo "staged=$target_exp"
