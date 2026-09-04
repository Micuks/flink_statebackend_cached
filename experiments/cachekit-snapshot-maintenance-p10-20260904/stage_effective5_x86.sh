#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p10-effective5-100m-x86-20260904
source_project=ckx865a9p10
target_project=ckx865a9p10e5
source_scratch=/tmp/ckx865a9p10
target_scratch=/tmp/ckx865a9p10e5
source_rest_port=10834
target_rest_port=10842
source_prom_port=11917
target_prom_port=11933
source_push_port=11918
target_push_port=11934
source_commit=8780838608a9c4ef1f374b91873ad3be7f576782
artifact_sha=09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
summarizer=$script_dir/summarize_effective5.py
[[ -f $summarizer ]] || { echo "summarizer missing: $summarizer" >&2; exit 64; }

stage=/tmp/cachekit-p10-effective5-stage-20260904
ssh -o BatchMode=yes "$host" mkdir -p "$stage"
scp -q "$summarizer" "$host:$stage/"

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
[[ -f $source_exp/summarize_x86.py ]]
if [[ ! -f $source_exp/final/P10_SCREEN_SUMMARY.json ]]; then
  python3 "$source_exp/summarize_x86.py" "$source_exp" \
    >"$source_exp/logs/summarize-x86.stdout"
fi
python3 - "$source_exp/final/P10_SCREEN_SUMMARY.json" <<'PY'
import json
import sys

summary = json.load(open(sys.argv[1]))
assert summary["maintenance_64k_vs_overlay_64k_uplift_percent_kps_core"] > 0.0
assert summary["a_plus_b_vs_baseline_uplift_percent_kps_core"] > 0.0
PY
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

variants=(baseline hot2-a hot2-maintained-64k)
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in "${variants[@]}"; do
  mkdir -p "$target_exp/variants/$variant"
  cp "$source_exp/variants/$variant/flink-conf.yaml" \
    "$source_exp/variants/$variant/docker-compose.yml" \
    "$target_exp/variants/$variant/"
done
cp "$source_exp/audit_value_prefetch.py" "$source_exp/audit_hot_levels.py" \
  "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/summarize_effective5.py" "$target_exp/"

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
variants = {
    "baseline": {"hot_levels": "0", "map_cache": "0", "snapshot_cache": "2000", "overlay": "false", "maintenance": "false"},
    "hot2-a": {"hot_levels": "2", "map_cache": "0", "snapshot_cache": "2000", "overlay": "false", "maintenance": "false"},
    "hot2-maintained-64k": {"hot_levels": "2", "map_cache": "65536", "snapshot_cache": "65536", "overlay": "true", "maintenance": "true"},
}

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

def replace_once(old, new):
    global text
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)

replace_once("queries=(q9)", "queries=(q5 q11 q15 q18)")
replace_once(
    "variants=(baseline hot2-a hot2-overlay hot2-maintained hot2-overlay-64k hot2-maintained-64k)",
    "variants=(baseline hot2-a hot2-maintained-64k)",
)
replace_once(
    "    assert totals[0]>0 and totals[1]>0 and totals[2]>0\n"
    "    assert totals[3]+totals[4]+totals[5]>0\n",
    "",
)
replace_once("    assert totals[0]>0 and totals[2]>0\n", "")
replace_once(
    "if maintenance_enabled:\n"
    "    assert snapshot_activation['single_short_circuits']>0\n",
    "",
)
runner.write_text(text)

runtime_path = root / "inputs/artifacts/opt/RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(source_exp, target_exp)
runtime_payload = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
runtime_path.write_text(runtime_payload)
(runtime_path.parent / "RUNTIME_BUNDLE.sha256").write_text(
    hashlib.sha256(runtime_payload.encode()).hexdigest() + "  RUNTIME_BUNDLE.json\n"
)

screen_summary = pathlib.Path(source_exp) / "final" / "P10_SCREEN_SUMMARY.json"
identity = {
    "schema": "cachekit-p10-effective5-x86-v1",
    "phase": "fresh-same-artifact-baseline-a-a-plus-b-effective5",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "screen_experiment": source_exp,
    "screen_summary_sha256": hashlib.sha256(screen_summary.read_bytes()).hexdigest(),
    "queries": ["q5", "q11", "q15", "q18"],
    "reused_screen_query": "q9",
    "variants": list(variants),
    "baseline": "baseline",
    "a": "hot2-a",
    "a_plus_b": "hot2-maintained-64k",
    "execution_environment": "idle-host-same-artifact-effective5-promotion",
    "claim_boundary": "all five queries on x86 host 114 with fresh same-artifact baseline, A, and A+B legs",
    "variant_config_sha256": {
        variant: hashlib.sha256(
            (root / "variants" / variant / "flink-conf.yaml").read_bytes()
        ).hexdigest()
        for variant in variants
    },
    "variant_compose_sha256": {
        variant: hashlib.sha256(
            (root / "variants" / variant / "docker-compose.yml").read_bytes()
        ).hexdigest()
        for variant in variants
    },
}
(root / "identity.json").write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")

def config(path):
    result = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result

configs = {
    variant: config(root / "variants" / variant / "flink-conf.yaml")
    for variant in variants
}
config_differences = {}
for left, right in (("baseline", "hot2-a"), ("hot2-a", "hot2-maintained-64k")):
    config_differences[f"{left}_vs_{right}"] = {
        key: {left: configs[left].get(key), right: configs[right].get(key)}
        for key in sorted(set(configs[left]) | set(configs[right]))
        if configs[left].get(key) != configs[right].get(key)
    }
expected = {
    "baseline_vs_hot2-a": {
        "state.backend.rocksdb.compression.uncompressed-hot-levels"
    },
    "hot2-a_vs_hot2-maintained-64k": {
        "state.backend.cachekit.map.cache.max-entries",
        "state.backend.cachekit.map.snapshot.cache.max-entries",
    },
}
compose_env = {}
for variant, settings in variants.items():
    compose_text = (root / "variants" / variant / "docker-compose.yml").read_text()
    overlay = f"CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '{settings['overlay']}'"
    maintenance = (
        "CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED: "
        f"'{settings['maintenance']}'"
    )
    assert compose_text.count(overlay) == 4
    assert compose_text.count(maintenance) == 4
    compose_env[variant] = {
        "dirty_overlay": settings["overlay"],
        "snapshot_maintenance": settings["maintenance"],
    }
audit = {
    "schema": "cachekit-p10-effective5-config-diff-v1",
    "config_differences": config_differences,
    "expected_config_difference_keys": {
        pair: sorted(keys) for pair, keys in expected.items()
    },
    "compose_runtime_env": compose_env,
    "valid": all(set(config_differences[pair]) == keys for pair, keys in expected.items()),
}
if not audit["valid"]:
    raise SystemExit(f"invalid effective5 config diff: {audit}")
(root / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audit, indent=2, sort_keys=True) + "\n"
)
PY

(
  cd "$target_exp"
  sha256sum inputs/runtime/flink-dist-1.16.3.jar \
    inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
    inputs/artifacts/opt/libcachekit_native_request_plane_jni.so \
    >inputs/ARTIFACTS.SHA256SUMS
  sha256sum -c inputs/ARTIFACTS.SHA256SUMS
)
chmod +x "$target_exp/run_campaign.sh" "$target_exp/summarize_effective5.py"
bash -n "$target_exp/run_campaign.sh"
python3 -m json.tool "$target_exp/identity.json" >/dev/null
jq -e '.valid == true' "$target_exp/CONFIG_DIFF_AUDIT.json" >/dev/null
echo "staged=$target_exp"
REMOTE
