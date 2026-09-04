#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-p7-hot-level-compression-q9-100m-x86-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p8-indexed-delta-q9-100m-x86-20260904
source_project=ckx865a9p7
target_project=ckx865a9p8
source_scratch=/tmp/ckx865a9p7
target_scratch=/tmp/ckx865a9p8
source_rest_port=10810
target_rest_port=10818
source_prom_port=11869
target_prom_port=11885
source_push_port=11870
target_push_port=11886
source_commit=bff4e00f1c18783e58a29f270dac58b05e9f1f4b
artifact_sha=1100138998352b11a4d469b999e31de49f4dafba598393cf693e3db796b2bfa6

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p8-x86.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

stage=/tmp/cachekit-p8-indexed-delta-x86-stage-20260904
ssh -o BatchMode=yes "$host" mkdir -p "$stage"
scp -q "$candidate" "$host:$stage/"

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
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

variants=(hot2-a hot2-indexed)
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in "${variants[@]}"; do
  mkdir -p "$target_exp/variants/$variant"
  cp "$source_exp/variants/hot2/flink-conf.yaml" \
    "$source_exp/variants/hot2/docker-compose.yml" \
    "$target_exp/variants/$variant/"
done
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/audit_hot_levels.py" "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p8-x86.jar" \
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
variants = {"hot2-a": "false", "hot2-indexed": "true"}

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

for variant, enabled in variants.items():
    compose = root / "variants" / variant / "docker-compose.yml"
    text = compose.read_text()
    old_mount = f"{target_exp}/variants/hot2/flink-conf.yaml"
    new_mount = f"{target_exp}/variants/{variant}/flink-conf.yaml"
    if text.count(old_mount) != 4:
        raise SystemExit(f"compose mount count {text.count(old_mount)} in {compose}; expected 4")
    compose.write_text(text.replace(old_mount, new_mount))

    config = root / "variants" / variant / "flink-conf.yaml"
    text = config.read_text()
    key = "state.backend.rocksdb.write-batch-with-index.enabled"
    if key in text:
        raise SystemExit(f"indexed delta policy already present in {config}")
    text += f"\n# P8 MapState indexed pending-write delta.\n{key}: {enabled}\n"
    config.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()

def replace_once(old, new):
    global text
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)

replace_once(
    "source_commit=fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce",
    f"source_commit={source_commit}",
)
replace_once(
    "artifact_sha=27466030bae4e792777a6002ca4ee4e831b2b92411340e642c00484f549516bc",
    f"artifact_sha={artifact_sha}",
)
replace_once("variants=(highmem-snappy hot1 hot2)", "variants=(hot2-a hot2-indexed)")
replace_once(
    "local cf=$expdir/variants/highmem-snappy/docker-compose.yml",
    "local cf=$expdir/variants/hot2-a/docker-compose.yml",
)
replace_once(
    "expected_hot={'highmem-snappy':'0','hot1':'1','hot2':'2'}[variant]\n"
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']==expected_hot",
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']=='2'\n"
    "expected_indexed={'hot2-a':'false','hot2-indexed':'true'}[variant]\n"
    "assert values['state.backend.rocksdb.write-batch-with-index.enabled']==expected_indexed",
)
replace_once(
    "for forbidden in ('UnsatisfiedLinkError','OutOfMemoryError','Fatal error','Native request-plane JNI failed to load'):\n"
    "    assert forbidden not in logs,forbidden\n"
    "assert '[CACHEKIT VALUE PREFETCH]' in logs",
    "for forbidden in ('UnsatisfiedLinkError','OutOfMemoryError','Fatal error','Native request-plane JNI failed to load'):\n"
    "    assert forbidden not in logs,forbidden\n"
    "assert '[CACHEKIT VALUE PREFETCH]' in logs\n"
    "marker='[CACHEKIT ROCKSDB INDEXED WRITE BATCH]'\n"
    "activation={'enabled':variant=='hot2-indexed','marker_count':logs.count(marker)}\n"
    "if variant=='hot2-indexed':\n"
    "    import re\n"
    "    pattern=(r'\\[CACHEKIT ROCKSDB INDEXED WRITE BATCH\\] enabled=true puts=(\\d+) deletes=(\\d+) '\n"
    "             r'mapPuts=(\\d+) mapDeletes=(\\d+) pointReads=(\\d+) '\n"
    "             r'pointReadsWithPendingWrites=(\\d+) directPointReads=(\\d+) '\n"
    "             r'iteratorRequests=(\\d+) mergedIteratorsWithPendingWrites=(\\d+) '\n"
    "             r'baseIterators=(\\d+) flushes=(\\d+) flushedEntries=(\\d+)')\n"
    "    rows=[tuple(map(int,row)) for row in re.findall(pattern,logs)]\n"
    "    assert rows and activation['marker_count']>=len(rows)\n"
    "    totals=[sum(row[i] for row in rows) for i in range(len(rows[0]))]\n"
    "    activation.update({'rows':len(rows),'totals':totals,\n"
    "        'entries_per_flush':totals[11]/totals[10] if totals[10] else 0.0})\n"
    "    assert totals[2]>0 and totals[10]>0 and totals[11]>totals[10]\n"
    "    assert totals[5]+totals[8]>0\n"
    "else:\n"
    "    assert marker not in logs",
)
replace_once(
    "'schema':'cachekit-rocksdb-hot-level-compression-p7-leg-v1','valid':True,",
    "'schema':'cachekit-indexed-mapstate-delta-p8-leg-v1','valid':True,",
)
replace_once(
    " 'claim_boundary':identity.get('claim_boundary'),",
    " 'claim_boundary':identity.get('claim_boundary'),\n"
    " 'indexed_write_batch_activation':activation,",
)
replace_once(
    "case $variant in highmem-snappy) hot_levels=0 ;; hot1) hot_levels=1 ;; hot2) hot_levels=2 ;; *) return 79 ;; esac",
    "hot_levels=2",
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
    "schema": "cachekit-indexed-mapstate-delta-p8-x86-screen-v1",
    "phase": "x86-q9-same-artifact-a-vs-a-plus-b",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "queries": ["q9"],
    "variants": list(variants),
    "primary_control": "hot2-a",
    "treatment": "hot2-indexed",
    "execution_environment": "idle-host-same-artifact-paired-source-mechanism-screen",
    "claim_boundary": "same x86 host and P8 artifact; only indexed delta flag differs",
})
identity["artifact_build"] = {
    "p4_x86_base_sha256": "66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b",
    "rocksdb_overlay_source_commit": source_commit,
    "entry_set_exact_base_plus_overlay": True,
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

control = config(root / "variants/hot2-a/flink-conf.yaml")
treatment = config(root / "variants/hot2-indexed/flink-conf.yaml")
differences = {
    key: {"control": control.get(key), "treatment": treatment.get(key)}
    for key in sorted(set(control) | set(treatment))
    if control.get(key) != treatment.get(key)
}
expected = {"state.backend.rocksdb.write-batch-with-index.enabled"}
audit = {
    "schema": "cachekit-p8-config-diff-v1",
    "differences": differences,
    "expected_difference_keys": sorted(expected),
    "unexpected_difference_keys": sorted(set(differences) - expected),
    "missing_difference_keys": sorted(expected - set(differences)),
    "valid": set(differences) == expected,
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audit, indent=2, sort_keys=True) + "\n"
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
python3 -m json.tool "$target_exp/identity.json" >/dev/null
python3 -m json.tool "$target_exp/CONFIG_DIFF_AUDIT.json" >/dev/null
echo "staged=$target_exp"
REMOTE
