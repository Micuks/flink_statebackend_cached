#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=${CACHEKIT_KP_CONTROL_PATH:-/tmp/cachekit-kp-ssh-20260904.sock}
source_exp=/home/wuql/flink-cluster/experiments/cachekit-p6-highmem-effective5-100m-kunpeng-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p7-hot-level-compression-effective5-100m-kunpeng-20260904
source_project=ckkp5a9h6
target_project=ckkp5a9p7
source_scratch=/tmp/ckkp5a9h6
target_scratch=/tmp/ckkp5a9p7
source_rest_port=10806
target_rest_port=10814
source_prom_port=11853
target_prom_port=11877
source_push_port=11854
target_push_port=11878
source_commit=fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce
artifact_sha=6f6411d5a73126983eaba65f5c1c3335f4e269054ed0151392afad63bc6e7d57

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p7-aarch64.jar
[[ -S $control_path ]] || { echo "Kunpeng SSH control socket missing: $control_path" >&2; exit 64; }
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")
stage=/tmp/cachekit-p7-hot-levels-kunpeng-stage-20260904
"${ssh_cmd[@]}" mkdir -p "$stage"
scp -q -o ControlPath="$control_path" "$candidate" "$script_dir/audit_hot_levels.py" \
  "$host:$stage/"

"${ssh_cmd[@]}" bash -s -- "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$source_commit" "$artifact_sha" \
  "$stage" <<'REMOTE'
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
target_cpuset=38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74

[[ -f $source_exp/CAMPAIGN_COMPLETE ]]
[[ -f $source_exp/final/HOST_RESULT_COMPLETE ]]
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

mkdir -p "$target_exp/logs" "$target_exp/variants/hot2"
cp -a "$source_exp/inputs" "$target_exp/inputs"
cp "$source_exp/variants/highmem/flink-conf.yaml" \
  "$source_exp/variants/highmem/docker-compose.yml" \
  "$target_exp/variants/hot2/"
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_hot_levels.py" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p7-aarch64.jar" \
  "$target_exp/inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

python3 - "$target_exp/FOREIGN_CONTAINER_PREFLIGHT.json" "$target_cpuset" \
  "$target_project" <<'PY'
import datetime as dt
import json
import subprocess
import sys

output, target_spec, target_project = sys.argv[1:4]

def expand(spec):
    result = set()
    for part in spec.split(","):
        bounds = [int(value) for value in part.split("-", 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result

target = expand(target_spec)
ids = subprocess.check_output(["docker", "ps", "-q"], text=True).split()
containers = []
for container_id in ids:
    item = json.loads(
        subprocess.check_output(["docker", "inspect", container_id], text=True)
    )[0]
    labels = item["Config"].get("Labels") or {}
    project = labels.get("com.docker.compose.project", "")
    cpus = item["HostConfig"].get("CpusetCpus", "")
    overlap = sorted(target & expand(cpus)) if cpus else sorted(target)
    containers.append(
        {
            "id": container_id,
            "name": item["Name"].lstrip("/"),
            "project": project,
            "cpuset_cpus": cpus,
            "cpuset_mems": item["HostConfig"].get("CpusetMems", ""),
            "target_overlap": overlap,
            "allowed_disjoint": project != target_project and bool(cpus) and not overlap,
        }
    )
invalid = [item for item in containers if not item["allowed_disjoint"]]
result = {
    "schema": "cachekit-disjoint-foreign-container-preflight-v1",
    "captured_at": dt.datetime.now().astimezone().isoformat(),
    "target_project": target_project,
    "target_cpuset": target_spec,
    "target_cpuset_mems": "0",
    "foreign_containers": containers,
    "valid": not invalid,
}
open(output, "w").write(json.dumps(result, indent=2, sort_keys=True) + "\n")
if invalid:
    raise SystemExit(f"overlapping or unbound foreign containers: {invalid}")
print("kunpeng-disjoint-foreign-preflight=PASS")
PY

python3 - "$target_exp" "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$source_commit" "$artifact_sha" <<'PY_STAGE'
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

replacements = [(value.encode(), replacement.encode()) for value, replacement in (
    (source_exp, target_exp),
    (source_project, target_project),
    (source_scratch, target_scratch),
    (source_rest_port, target_rest_port),
    (source_prom_port, target_prom_port),
    (source_push_port, target_push_port),
)]
paths = [root / "run_campaign.sh", root / "variants/hot2/docker-compose.yml"]
for path in paths:
    data = path.read_bytes()
    for old, new in replacements:
        data = data.replace(old, new)
    path.write_bytes(data)

compose = root / "variants/hot2/docker-compose.yml"
text = compose.read_text()
old_mount = f"{target_exp}/variants/highmem/flink-conf.yaml"
new_mount = f"{target_exp}/variants/hot2/flink-conf.yaml"
if text.count(old_mount) != 4:
    raise SystemExit(f"compose mount count {text.count(old_mount)}; expected 4")
compose.write_text(text.replace(old_mount, new_mount))

config = root / "variants/hot2/flink-conf.yaml"
text = config.read_text()
old = "state.backend.rocksdb.compression.type: NO_COMPRESSION"
if text.count(old) != 1:
    raise SystemExit(f"compression setting count {text.count(old)}")
text = text.replace(old, "state.backend.rocksdb.compression.type: SNAPPY_COMPRESSION")
key = "state.backend.rocksdb.compression.uncompressed-hot-levels"
if key in text:
    raise SystemExit("hot-level policy already present")
text += f"\n# P7 source-level hot SST policy.\n{key}: 2\n"
config.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()

def replace_once(old, new):
    global text
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)

replace_once(
    '''fail_on_foreign_containers() {
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
}''',
    '''fail_on_foreign_containers() {
  local id inspect owner_name owner_project owner_cpus remainder foreign=0
  while read -r id; do
    [[ -n $id ]] || continue
    inspect=$(docker inspect -f '{{.Name}}|{{index .Config.Labels "com.docker.compose.project"}}|{{.HostConfig.CpusetCpus}}' "$id" 2>/dev/null) || continue
    owner_name=${inspect%%|*}
    remainder=${inspect#*|}
    owner_project=${remainder%%|*}
    owner_cpus=${remainder#*|}
    if [[ $owner_project != "$project" ]]; then
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
        echo "allowed_disjoint_container=$owner_name cpus=$owner_cpus" >&2
      else
        echo "foreign_container=$owner_name project=$owner_project cpus=$owner_cpus" >&2
        foreign=1
      fi
    fi
  done < <(docker ps -q)
  (( foreign == 0 )) || return 1
}''',
)

replace_once(
    "source_commit=a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4",
    f"source_commit={source_commit}",
)
replace_once(
    "artifact_sha=3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723",
    f"artifact_sha={artifact_sha}",
)
replace_once("variants=(highmem)", "variants=(hot2)")
replace_once(
    "local cf=$expdir/variants/control/docker-compose.yml",
    "local cf=$expdir/variants/hot2/docker-compose.yml",
)
replace_once(
    "assert values['state.backend.rocksdb.compression.type']=='NO_COMPRESSION'",
    "assert values['state.backend.rocksdb.compression.type']=='SNAPPY_COMPRESSION'\n"
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']=='2'",
)
replace_once(
    "'schema':'cachekit-rocksdb-no-compression-p4-leg-v1'",
    "'schema':'cachekit-rocksdb-hot-level-compression-p7-leg-v1'",
)
replace_once(
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode sst_monitor_pid",
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc sst_monitor_pid",
)
replace_once("  compression_mode=NO_COMPRESSION\n", "")
replace_once(
    "  local -a compression_sst_args=()\n"
    "  if [[ $query != q15 ]]; then compression_sst_args+=(--require-sst); fi\n"
    "  python3 \"$expdir/audit_compression.py\" \"$d\" --expected \"$compression_mode\" \"${compression_sst_args[@]}\" --output \"$d/COMPRESSION_AUDIT.json\" \\\n"
    "    >\"$d/compression-audit.stdout\"",
    "  local -a hot_level_sst_args=()\n"
    "  if [[ $query != q15 ]]; then hot_level_sst_args+=(--require-sst); fi\n"
    "  python3 \"$expdir/audit_hot_levels.py\" \"$d\" --expected-hot-levels 2 \"${hot_level_sst_args[@]}\" --output \"$d/HOT_LEVEL_COMPRESSION_AUDIT.json\" \\\n"
    "    >\"$d/hot-level-compression-audit.stdout\"",
)
replace_once(
    '"$d"/COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv "$d"/rocksdb-sst-live-inventory.tsv \\',
    '"$d"/HOT_LEVEL_COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv "$d"/rocksdb-sst-live-inventory.tsv \\',
)
runner.write_text(text)

runtime_path = root / "inputs/runtime/RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (
            root / "inputs/runtime" / artifact["container_filename"]
        ).stat().st_size
runtime_payload = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
runtime_path.write_text(runtime_payload)
(runtime_path.parent / "RUNTIME_BUNDLE.sha256").write_text(
    hashlib.sha256(runtime_payload.encode()).hexdigest() + "  RUNTIME_BUNDLE.json\n"
)

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-rocksdb-hot-level-compression-p7-kunpeng-effective5-v1",
    "phase": "kunpeng-effective5-source-hot2-confirmation",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "queries": ["q5", "q9", "q11", "q15", "q18"],
    "variants": ["hot2"],
    "primary_control": "reused per-query P4 controls",
    "execution_environment": "numa0-isolated-candidate-with-disjoint-foreign-numa1-campaign",
    "claim_boundary": "same Kunpeng host; P7 differs from P4 artifact only in audited RocksDB policy classes",
})
identity["artifact_build"] = {
    "p4_aarch64_base_sha256": "3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723",
    "rocksdb_overlay_source_commit": source_commit,
    "non_overlay_entries_identical_to_p4": True,
}
identity["foreign_container_preflight"] = "FOREIGN_CONTAINER_PREFLIGHT.json"
identity["variant_config_sha256"] = {
    "hot2": hashlib.sha256(config.read_bytes()).hexdigest()
}
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

def load_config(path):
    result = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result

source = load_config(pathlib.Path(source_exp) / "variants/highmem/flink-conf.yaml")
candidate = load_config(config)
differences = {
    key: {"source_highmem": source.get(key), "hot2": candidate.get(key)}
    for key in sorted(set(source) | set(candidate))
    if source.get(key) != candidate.get(key)
}
allowed = sorted([
    "state.backend.rocksdb.compression.type",
    "state.backend.rocksdb.compression.uncompressed-hot-levels",
])
audit = {
    "schema": "cachekit-p7-hot2-vs-p6-highmem-config-diff-v1",
    "actual_differences": differences,
    "allowed_difference_keys": allowed,
    "unexpected_difference_keys": sorted(set(differences) - set(allowed)),
    "missing_difference_keys": sorted(set(allowed) - set(differences)),
    "valid": sorted(differences) == allowed,
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audit, indent=2, sort_keys=True) + "\n"
)
assert hashlib.sha256(
    (root / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()
).hexdigest() == artifact_sha
PY_STAGE

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/runtime/libcachekit_native_request_plane_jni.so \
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
assert identity["artifact_sha256"] == "6f6411d5a73126983eaba65f5c1c3335f4e269054ed0151392afad63bc6e7d57"
assert json.loads((root / "CONFIG_DIFF_AUDIT.json").read_text())["valid"]
assert json.loads((root / "FOREIGN_CONTAINER_PREFLIGHT.json").read_text())["valid"]
print("p7-kunpeng-stage-audit=PASS")
PY
REMOTE

echo "staged=$target_exp"
