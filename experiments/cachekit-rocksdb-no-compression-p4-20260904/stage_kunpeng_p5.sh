#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-q9-100m-kunpeng-20260904
baseline_exp=/home/wuql/flink-cluster/experiments/cachekit-rocksdb-no-compression-p4-effective4-r2-100m-kunpeng-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p5-global-candidate-effective5-100m-kunpeng-20260904
source_project=ckkp5a9n4
target_project=ckkp5a9g5
source_scratch=/tmp/ckkp5a9n4
target_scratch=/tmp/ckkp5a9g5
source_rest_port=10798
target_rest_port=10804
source_prom_port=11845
target_prom_port=11851
source_push_port=11846
target_push_port=11852
artifact_sha=3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
snapshot_auditor=$repo/experiments/cachekit-snapshot-owned-key-reuse-p3-20260904/audit_snapshot_key_reuse.py
ssh_cmd=(ssh -S "$control_path" -o BatchMode=yes "$host")
stage=/tmp/cachekit-p5-global-stage-20260904
"${ssh_cmd[@]}" mkdir -p "$stage"
scp -q -o ControlPath="$control_path" "$script_dir/audit_compression.py" \
  "$script_dir/summarize_p5.py" "$snapshot_auditor" "$host:$stage/"

"${ssh_cmd[@]}" bash -s -- "$source_exp" "$baseline_exp" "$target_exp" \
  "$source_project" "$target_project" "$source_scratch" "$target_scratch" \
  "$source_rest_port" "$target_rest_port" "$source_prom_port" \
  "$target_prom_port" "$source_push_port" "$target_push_port" \
  "$artifact_sha" "$stage" <<'REMOTE'
set -euo pipefail
source_exp=$1
baseline_exp=$2
target_exp=$3
source_project=$4
target_project=$5
source_scratch=$6
target_scratch=$7
source_rest_port=$8
target_rest_port=$9
source_prom_port=${10}
target_prom_port=${11}
source_push_port=${12}
target_push_port=${13}
artifact_sha=${14}
stage=${15}

[[ -f $source_exp/final/HOST_RESULT_COMPLETE ]]
[[ -f $baseline_exp/final/HOST_RESULT_COMPLETE ]]
[[ -z $(docker ps -q) ]] || { echo "Kunpeng host has running containers" >&2; exit 72; }
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }

mkdir -p "$target_exp/logs" "$target_exp/variants/combined"
cp -a "$source_exp/inputs" "$target_exp/inputs"
cp "$source_exp/variants/no-compression/flink-conf.yaml" \
  "$source_exp/variants/no-compression/docker-compose.yml" \
  "$target_exp/variants/combined/"
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/audit_compression.py" "$stage/audit_snapshot_key_reuse.py" \
  "$stage/summarize_p5.py" "$target_exp/"

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
paths = [root / "run_campaign.sh", root / "variants/combined/docker-compose.yml"]
for path in paths:
    data = path.read_bytes()
    for old, new in replacements:
        data = data.replace(old, new)
    path.write_bytes(data)

compose = root / "variants/combined/docker-compose.yml"
text = compose.read_text()
old_mount = f"{target_exp}/variants/no-compression/flink-conf.yaml"
new_mount = f"{target_exp}/variants/combined/flink-conf.yaml"
if text.count(old_mount) != 4:
    raise SystemExit(f"combined compose mount count {text.count(old_mount)}; expected 4")
compose.write_text(text.replace(old_mount, new_mount))

config_path = root / "variants/combined/flink-conf.yaml"
text = config_path.read_text()
changes = {
    "# Generated P4 RocksDB compression variant: no-compression": "# Generated P5 global candidate variant: combined",
    "state.backend.cachekit.map.snapshot.cache.max-entries: 2000": "state.backend.cachekit.map.snapshot.cache.max-entries: 8000\nstate.backend.cachekit.map.snapshot.small.max-entries: 2",
    "state.backend.cachekit.map.snapshot.owned-key-reuse.enabled: false": "state.backend.cachekit.map.snapshot.owned-key-reuse.enabled: true",
}
for old, new in changes.items():
    if text.count(old) != 1:
        raise SystemExit(f"config replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
config_path.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()
required = {
    "queries=(q9)": "queries=(q5 q9 q11 q15 q18)",
    "variants=(control no-compression)": "variants=(combined)",
    "local cf=$expdir/variants/control/docker-compose.yml": "local cf=$expdir/variants/combined/docker-compose.yml",
    "assert values['state.backend.cachekit.map.snapshot.owned-key-reuse.enabled']=='false'": "assert values['state.backend.cachekit.map.snapshot.owned-key-reuse.enabled']=='true'\nassert values['state.backend.cachekit.map.snapshot.cache.max-entries']=='8000'\nassert values['state.backend.cachekit.map.snapshot.small.max-entries']=='2'",
    "assert values['state.backend.rocksdb.compression.type']==('NO_COMPRESSION' if variant=='no-compression' else 'SNAPPY_COMPRESSION')": "assert values['state.backend.rocksdb.compression.type']=='NO_COMPRESSION'",
    "if [[ $variant == no-compression ]]; then compression_mode=NO_COMPRESSION; else compression_mode=SNAPPY_COMPRESSION; fi": "compression_mode=NO_COMPRESSION",
    "capture_metric_raw() {": "capture_sst_inventory() {\n  local d=$1\n  find \"$scratch\" -type f -name '*.sst' -printf '%s\\t%P\\n' | LC_ALL=C sort -k2 >\"$d/rocksdb-sst-inventory.tsv\"\n}\n\nmonitor_sst_inventory() {\n  local d=$1 snapshot\n  snapshot=$(mktemp \"$d/.rocksdb-sst-live.XXXXXX\")\n  while [[ ! -f $d/.sst-monitor-stop ]]; do\n    find \"$scratch\" -type f -name '*.sst' -printf '%s\\t%P\\n' | LC_ALL=C sort -k2 >\"$snapshot\"\n    if [[ -s $snapshot ]]; then cp \"$snapshot\" \"$d/rocksdb-sst-live-inventory.tsv\"; fi\n    sleep 10\n  done\n  rm -f -- \"$snapshot\"\n}\n\ncapture_metric_raw() {",
    "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode": "local query=$1 round=$2 idx=$3 variant=$4 leg d config cf rc compression_mode sst_monitor_pid",
    "set +e\n  BENCH_DISABLE_WATCHDOG=1": ": >\"$d/rocksdb-sst-live-inventory.tsv\"\n  rm -f -- \"$d/.sst-monitor-stop\"\n  monitor_sst_inventory \"$d\" &\n  sst_monitor_pid=$!\n  set +e\n  BENCH_DISABLE_WATCHDOG=1",
    "rc=$?\n  set -e\n  capture_snapshot": "rc=$?\n  set -e\n  touch \"$d/.sst-monitor-stop\"\n  wait \"$sst_monitor_pid\"\n  rm -f -- \"$d/.sst-monitor-stop\"\n  capture_snapshot",
    'capture_metric_raw "$d"\n  capture_logs': 'capture_metric_raw "$d"\n  capture_sst_inventory "$d"\n  capture_logs',
    'python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --output "$d/COMPRESSION_AUDIT.json" \\': 'python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --require-sst --output "$d/COMPRESSION_AUDIT.json" \\',
    '"$d"/COMPRESSION_AUDIT.json \\': '"$d"/COMPRESSION_AUDIT.json "$d"/rocksdb-sst-inventory.tsv "$d"/rocksdb-sst-live-inventory.tsv "$d"/SNAPSHOT_KEY_REUSE_AUDIT.json \\',
}
for old, new in required.items():
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)
audit_needle = 'python3 "$expdir/audit_compression.py" "$d" --expected "$compression_mode" --require-sst --output "$d/COMPRESSION_AUDIT.json" \\\n    >"$d/compression-audit.stdout"\n'
audit_addition = audit_needle + '  python3 "$expdir/audit_snapshot_key_reuse.py" "$d" --output "$d/SNAPSHOT_KEY_REUSE_AUDIT.json" \\\n    >"$d/snapshot-key-reuse-audit.stdout"\n'
if text.count(audit_needle) != 1:
    raise SystemExit("compression audit insertion point not unique")
runner.write_text(text.replace(audit_needle, audit_addition))

runtime_path = root / "inputs/runtime/RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
runtime_payload = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
runtime_path.write_text(runtime_payload)
(runtime_path.parent / "RUNTIME_BUNDLE.sha256").write_text(
    hashlib.sha256(runtime_payload.encode()).hexdigest() + "  RUNTIME_BUNDLE.json\n"
)

identity_path = root / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update({
    "schema": "cachekit-p5-global-candidate-effective5-v1",
    "phase": "global-combined-candidate-effective5",
    "queries": ["q5", "q9", "q11", "q15", "q18"],
    "variants": ["combined"],
    "primary_control": "reused per-query P4 controls",
    "artifact_sha256": artifact_sha,
    "execution_environment": "idle-host-numa-isolated-candidate-only-with-frozen-controls",
    "claim_boundary": "same Kunpeng host and artifact; q9 control reused from P4 q9 and other controls reused from P4 effective4",
})
identity["variant_config_sha256"] = {
    "combined": hashlib.sha256(config_path.read_bytes()).hexdigest()
}
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

def config(path):
    values = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values

control = config(pathlib.Path(source_exp) / "variants/control/flink-conf.yaml")
candidate = config(config_path)
keys = sorted(set(control) | set(candidate))
differences = {key: {"control": control.get(key), "combined": candidate.get(key)} for key in keys if control.get(key) != candidate.get(key)}
allowed = sorted([
    "state.backend.rocksdb.compression.type",
    "state.backend.cachekit.map.snapshot.cache.max-entries",
    "state.backend.cachekit.map.snapshot.small.max-entries",
    "state.backend.cachekit.map.snapshot.owned-key-reuse.enabled",
])
audit = {
    "schema": "cachekit-p5-global-candidate-config-diff-v1",
    "actual_differences": differences,
    "allowed_difference_keys": allowed,
    "unexpected_difference_keys": sorted(set(differences) - set(allowed)),
    "valid": sorted(differences) == allowed,
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
assert hashlib.sha256((root / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar").read_bytes()).hexdigest() == artifact_sha
PY

chmod +x "$target_exp/run_campaign.sh" "$target_exp/audit_compression.py" \
  "$target_exp/audit_snapshot_key_reuse.py" "$target_exp/summarize_p5.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m py_compile "$target_exp/audit_compression.py" \
  "$target_exp/audit_snapshot_key_reuse.py" "$target_exp/summarize_p5.py"
(cd "$target_exp" && sha256sum -c inputs/ARTIFACTS.SHA256SUMS)
REMOTE

echo "staged=$target_exp"
