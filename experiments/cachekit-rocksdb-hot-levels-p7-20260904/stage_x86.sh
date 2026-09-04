#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-p6-lsm-screen-q9-100m-x86-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p7-hot-level-compression-q9-100m-x86-20260904
source_project=ckx865a9s6
target_project=ckx865a9p7
source_scratch=/tmp/ckx865a9s6
target_scratch=/tmp/ckx865a9p7
source_rest_port=10794
target_rest_port=10810
source_prom_port=11829
target_prom_port=11869
source_push_port=11830
target_push_port=11870
source_commit=fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce
artifact_sha=27466030bae4e792777a6002ca4ee4e831b2b92411340e642c00484f549516bc

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p7-x86.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

stage=/tmp/cachekit-p7-hot-levels-x86-stage-20260904
ssh -o BatchMode=yes "$host" mkdir -p "$stage"
scp -q "$candidate" "$script_dir/audit_hot_levels.py" "$host:$stage/"

ssh -o BatchMode=yes "$host" bash -s -- \
  "$source_exp" "$target_exp" "$source_project" "$target_project" \
  "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$source_commit" \
  "$artifact_sha" "$stage" <<'REMOTE'
set -euo pipefail
source_exp=$1
target_exp=$2
source_project=$3
target_project=$4
source_scratch=$5
target_scratch=$6
source_rest_port=$7
target_rest_port=$8
source_prom_port=$9
target_prom_port=${10}
source_push_port=${11}
target_push_port=${12}
source_commit=${13}
artifact_sha=${14}
stage=${15}

[[ -f $source_exp/CAMPAIGN_COMPLETE ]]
[[ -f $source_exp/final/HOST_RESULT_COMPLETE ]]
[[ -z $(docker ps -q) ]] || { echo "x86 host has running containers" >&2; exit 72; }
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

variants=(highmem-snappy hot1 hot2)
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in "${variants[@]}"; do
  mkdir -p "$target_exp/variants/$variant"
  cp "$source_exp/variants/highmem/flink-conf.yaml" \
    "$source_exp/variants/highmem/docker-compose.yml" \
    "$target_exp/variants/$variant/"
done
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_hot_levels.py" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p7-x86.jar" \
  "$target_exp/inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

python3 - "$target_exp" "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$source_commit" "$artifact_sha" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
source_exp, target_exp = sys.argv[2:4]
source_project, target_project = sys.argv[4:6]
source_scratch, target_scratch = sys.argv[6:8]
source_rest_port, target_rest_port = sys.argv[8:10]
source_prom_port, target_prom_port = sys.argv[10:12]
source_push_port, target_push_port = sys.argv[12:14]
source_commit, artifact_sha = sys.argv[14:16]
variants = {"highmem-snappy": 0, "hot1": 1, "hot2": 2}

replacements = [(value.encode(), replacement.encode()) for value, replacement in (
    (source_exp, target_exp),
    (source_project, target_project),
    (source_scratch, target_scratch),
    (source_rest_port, target_rest_port),
    (source_prom_port, target_prom_port),
    (source_push_port, target_push_port),
)]
paths = [root / "run_campaign.sh"]
paths.extend((root / "variants").glob("*/docker-compose.yml"))
for path in paths:
    data = path.read_bytes()
    for old, new in replacements:
        data = data.replace(old, new)
    path.write_bytes(data)

for variant, hot_levels in variants.items():
    compose = root / "variants" / variant / "docker-compose.yml"
    text = compose.read_text()
    old_mount = f"{target_exp}/variants/highmem/flink-conf.yaml"
    new_mount = f"{target_exp}/variants/{variant}/flink-conf.yaml"
    if text.count(old_mount) != 4:
        raise SystemExit(f"compose mount count {text.count(old_mount)} in {compose}; expected 4")
    compose.write_text(text.replace(old_mount, new_mount))

    config = root / "variants" / variant / "flink-conf.yaml"
    text = config.read_text()
    old = "state.backend.rocksdb.compression.type: NO_COMPRESSION"
    if text.count(old) != 1:
        raise SystemExit(f"compression setting count {text.count(old)} in {config}")
    text = text.replace(old, "state.backend.rocksdb.compression.type: SNAPPY_COMPRESSION")
    key = "state.backend.rocksdb.compression.uncompressed-hot-levels"
    if key in text:
        raise SystemExit(f"hot-level policy already present in {config}")
    text += f"\n# P7 source-level hot SST policy.\n{key}: {hot_levels}\n"
    config.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()

def replace_once(old, new):
    global text
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)

replace_once(
    "source_commit=a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4",
    f"source_commit={source_commit}",
)
replace_once(
    "artifact_sha=66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b",
    f"artifact_sha={artifact_sha}",
)
replace_once("variants=(highmem large-lsm)", "variants=(highmem-snappy hot1 hot2)")
replace_once(
    "local cf=$expdir/variants/control/docker-compose.yml",
    "local cf=$expdir/variants/highmem-snappy/docker-compose.yml",
)
replace_once(
    "assert values['state.backend.rocksdb.compression.type']=='NO_COMPRESSION'",
    "assert values['state.backend.rocksdb.compression.type']=='SNAPPY_COMPRESSION'\n"
    "expected_hot={'highmem-snappy':'0','hot1':'1','hot2':'2'}[variant]\n"
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']==expected_hot",
)
replace_once(
    "'schema':'cachekit-rocksdb-no-compression-p4-leg-v1'",
    "'schema':'cachekit-rocksdb-hot-level-compression-p7-leg-v1'",
)
replace_once(
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode sst_monitor_pid",
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc hot_levels sst_monitor_pid",
)
replace_once(
    "  compression_mode=NO_COMPRESSION\n"
    "  python3 \"$expdir/audit_compression.py\" \"$d\" --expected \"$compression_mode\" --require-sst --output \"$d/COMPRESSION_AUDIT.json\" \\\n"
    "    >\"$d/compression-audit.stdout\"",
    "  case $variant in highmem-snappy) hot_levels=0 ;; hot1) hot_levels=1 ;; hot2) hot_levels=2 ;; *) return 79 ;; esac\n"
    "  python3 \"$expdir/audit_hot_levels.py\" \"$d\" --expected-hot-levels \"$hot_levels\" --require-sst --output \"$d/HOT_LEVEL_COMPRESSION_AUDIT.json\" \\\n"
    "    >\"$d/hot-level-compression-audit.stdout\"",
)
replace_once(
    '"$d"/COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv "$d"/rocksdb-sst-live-inventory.tsv \\',
    '"$d"/HOT_LEVEL_COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv "$d"/rocksdb-sst-live-inventory.tsv \\',
)
runner.write_text(text)

runtime_path = root / "inputs/artifacts/opt/RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (
            root / "inputs/artifacts/opt" / artifact["container_filename"]
        ).stat().st_size
runtime_payload = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
runtime_path.write_text(runtime_payload)
(runtime_path.parent / "RUNTIME_BUNDLE.sha256").write_text(
    hashlib.sha256(runtime_payload.encode()).hexdigest() + "  RUNTIME_BUNDLE.json\n"
)

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-rocksdb-hot-level-compression-p7-x86-screen-v1",
    "phase": "x86-q9-source-hot-level-screen",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "queries": ["q9"],
    "variants": list(variants),
    "primary_control": "highmem-snappy",
    "secondary_reference": (
        f"{source_exp}/results/raw/001-r1-q9-highmem"
    ),
    "execution_environment": "idle-host-same-artifact-paired-source-policy-screen",
    "claim_boundary": "same x86 host and P7 artifact for causal hot-level comparison",
})
identity["artifact_build"] = {
    "p4_x86_base_sha256": "66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b",
    "rocksdb_overlay_source_commit": source_commit,
    "non_overlay_entries_identical_to_p4": True,
}
identity["variant_config_sha256"] = {
    variant: hashlib.sha256(
        (root / "variants" / variant / "flink-conf.yaml").read_bytes()
    ).hexdigest()
    for variant in variants
}
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

def config(path):
    result = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result

control = config(root / "variants/highmem-snappy/flink-conf.yaml")
audits = {}
for variant in variants:
    candidate = config(root / "variants" / variant / "flink-conf.yaml")
    differences = {
        key: {"control": control.get(key), "candidate": candidate.get(key)}
        for key in sorted(set(control) | set(candidate))
        if control.get(key) != candidate.get(key)
    }
    expected = set() if variant == "highmem-snappy" else {
        "state.backend.rocksdb.compression.uncompressed-hot-levels"
    }
    actual = set(differences)
    audits[variant] = {
        "differences": differences,
        "expected_difference_keys": sorted(expected),
        "unexpected_difference_keys": sorted(actual - expected),
        "missing_difference_keys": sorted(expected - actual),
        "valid": actual == expected,
    }
    if not audits[variant]["valid"]:
        raise SystemExit(f"invalid config diff for {variant}: {audits[variant]}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audits, indent=2, sort_keys=True) + "\n"
)
assert hashlib.sha256(
    (root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()
).hexdigest() == artifact_sha
PY

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/artifacts/opt/libcachekit_native_request_plane_jni.so \
    >inputs/ARTIFACTS.SHA256SUMS
  sha256sum -c inputs/ARTIFACTS.SHA256SUMS
)
chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_hot_levels.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_hot_levels.py"
python3 - "$target_exp" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
identity = json.loads((root / "identity.json").read_text())
assert identity["source_commit"] == "fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce"
assert identity["artifact_sha256"] == "27466030bae4e792777a6002ca4ee4e831b2b92411340e642c00484f549516bc"
assert all(item["valid"] for item in json.loads((root / "CONFIG_DIFF_AUDIT.json").read_text()).values())
print("p7-x86-stage-audit=PASS")
PY
REMOTE

echo "staged=$target_exp"
