#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-q9-100m-kunpeng-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-effective4-100m-kunpeng-20260904
source_project=ckkp5a9n4
target_project=ckkp5a9e4
source_scratch=/tmp/ckkp5a9n4
target_scratch=/tmp/ckkp5a9e4
source_rest_port=10798
target_rest_port=10800
source_prom_port=11845
target_prom_port=11847
source_push_port=11846
target_push_port=11848
artifact_sha=3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")
stage=/tmp/cachekit-rocksdb-no-compression-p4-effective-stage-20260904
"${ssh_cmd[@]}" mkdir -p "$stage"
scp -q -o ControlPath="$control_path" "$script_dir/audit_compression.py" \
  "$script_dir/summarize.py" "$host:$stage/"

"${ssh_cmd[@]}" bash -s -- "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$artifact_sha" "$stage" <<'REMOTE'
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
artifact_sha=${13}
stage=${14}

[[ -f $source_exp/final/HOST_RESULT_COMPLETE ]]
[[ -z $(docker ps -q) ]] || { echo "Kunpeng host has running containers" >&2; exit 72; }
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }

mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$source_exp/variants" "$target_exp/"
cp "$source_exp/identity.json" "$source_exp/CONFIG_DIFF_AUDIT.json" \
  "$source_exp/audit_value_prefetch.py" "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_compression.py" "$stage/summarize.py" "$target_exp/"

python3 - "$target_exp" "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$artifact_sha" <<'PY'
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
artifact_sha = sys.argv[14]

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

runner = root / "run_campaign.sh"
text = runner.read_text()
required = {
    "queries=(q9)": "queries=(q5 q11 q15 q18)",
    "capture_metric_raw() {": "capture_sst_inventory() {\n  local d=$1\n  find \"$scratch\" -type f -name '*.sst' -printf '%s\\t%P\\n' | LC_ALL=C sort -k2 >\"$d/rocksdb-sst-inventory.tsv\"\n}\n\ncapture_metric_raw() {",
    'capture_metric_raw "$d"\n  capture_logs': 'capture_metric_raw "$d"\n  capture_sst_inventory "$d"\n  capture_logs',
    'python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --output "$d/COMPRESSION_AUDIT.json" \\': 'python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --require-sst --output "$d/COMPRESSION_AUDIT.json" \\',
    '"$d"/COMPRESSION_AUDIT.json \\': '"$d"/COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv \\',
}
for old, new in required.items():
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
runner.write_text(text)

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-rocksdb-no-compression-p4-effective4-v1",
    "phase": "rocksdb-no-compression-effective-query-expansion",
    "queries": ["q5", "q11", "q15", "q18"],
    "variants": ["control", "no-compression"],
    "artifact_sha256": artifact_sha,
    "execution_environment": "idle-host-numa-isolated-effective-query-expansion",
    "claim_boundary": "same-host paired Kunpeng expansion on NUMA node 0; only legs with actual SST files are effective",
})
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

for variant in identity["variants"]:
    actual = hashlib.sha256((root / "variants" / variant / "flink-conf.yaml").read_bytes()).hexdigest()
    assert actual == identity["variant_config_sha256"][variant]
assert hashlib.sha256((root / "inputs" / "runtime" / "flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()).hexdigest() == artifact_sha
PY

chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_compression.py" \
  "$target_exp/summarize.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_compression.py" "$target_exp/summarize.py"
(cd "$target_exp" && sha256sum -c inputs/ARTIFACTS.SHA256SUMS)
REMOTE

echo "staged=$target_exp"
