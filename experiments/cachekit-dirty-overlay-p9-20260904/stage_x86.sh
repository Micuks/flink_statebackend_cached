#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-p8-indexed-delta-q9-100m-x86-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p9-dirty-overlay-q9-100m-x86-20260904
source_project=ckx865a9p8
target_project=ckx865a9p9
source_scratch=/tmp/ckx865a9p8
target_scratch=/tmp/ckx865a9p9
source_rest_port=10818
target_rest_port=10826
source_prom_port=11885
target_prom_port=11901
source_push_port=11886
target_push_port=11902
source_commit=56aaafe595fa51df07a4ea371f255751375d7884
artifact_sha=2d9214f7afbe25997b2762e66025647acfc11258954eadb148940d55aceaafb5

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p9-x86.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

stage=/tmp/cachekit-p9-dirty-overlay-x86-stage-20260904
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
[[ -f $source_exp/final/P8_SCREEN_SUMMARY.json ]]
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

variants=(hot2-a hot2-cache hot2-overlay)
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
for variant in "${variants[@]}"; do
  mkdir -p "$target_exp/variants/$variant"
  cp "$source_exp/variants/hot2-a/flink-conf.yaml" \
    "$source_exp/variants/hot2-a/docker-compose.yml" \
    "$target_exp/variants/$variant/"
done
cp "$source_exp/identity.json" "$source_exp/audit_value_prefetch.py" \
  "$source_exp/audit_hot_levels.py" "$source_exp/run_campaign.sh" "$target_exp/"
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p9-x86.jar" \
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
variants = {
    "hot2-a": {"map_cache": "0", "overlay": "false"},
    "hot2-cache": {"map_cache": "65536", "overlay": "false"},
    "hot2-overlay": {"map_cache": "65536", "overlay": "true"},
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

for variant, settings in variants.items():
    compose = root / "variants" / variant / "docker-compose.yml"
    text = compose.read_text()
    old_mount = f"{target_exp}/variants/hot2-a/flink-conf.yaml"
    new_mount = f"{target_exp}/variants/{variant}/flink-conf.yaml"
    if text.count(old_mount) != 4:
        raise SystemExit(f"compose mount count {text.count(old_mount)} in {compose}; expected 4")
    text = text.replace(old_mount, new_mount)
    import re
    env_pattern = re.compile(r"(?m)^(\s*)NEXMARK_SQL_SUBMIT_TIMEOUT_SEC: '180'$" )
    matches = list(env_pattern.finditer(text))
    if len(matches) != 4:
        raise SystemExit(f"overlay env insertion count {len(matches)} in {compose}; expected 4")
    text = env_pattern.sub(
        lambda match: match.group(0)
        + "\n"
        + match.group(1)
        + "CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '"
        + settings["overlay"]
        + "'",
        text,
    )
    compose.write_text(text)

    config = root / "variants" / variant / "flink-conf.yaml"
    text = config.read_text()
    old_cache = "state.backend.cachekit.map.cache.max-entries: 0"
    if text.count(old_cache) != 1:
        raise SystemExit(f"map cache setting count {text.count(old_cache)} in {config}")
    text = text.replace(
        old_cache,
        "state.backend.cachekit.map.cache.max-entries: " + settings["map_cache"],
    )
    config.write_text(text)

runner = root / "run_campaign.sh"
text = runner.read_text()

def replace_once(old, new):
    global text
    if text.count(old) != 1:
        raise SystemExit(f"runner replacement count {text.count(old)} for {old!r}")
    text = text.replace(old, new)

replace_once(
    "source_commit=bff4e00f1c18783e58a29f270dac58b05e9f1f4b",
    f"source_commit={source_commit}",
)
replace_once(
    "artifact_sha=1100138998352b11a4d469b999e31de49f4dafba598393cf693e3db796b2bfa6",
    f"artifact_sha={artifact_sha}",
)
replace_once(
    "variants=(hot2-a hot2-indexed)",
    "variants=(hot2-a hot2-cache hot2-overlay)",
)
replace_once(
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']=='2'\n"
    "expected_indexed={'hot2-a':'false','hot2-indexed':'true'}[variant]\n"
    "assert values['state.backend.rocksdb.write-batch-with-index.enabled']==expected_indexed",
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']=='2'\n"
    "assert values['state.backend.rocksdb.write-batch-with-index.enabled']=='false'\n"
    "expected_map={'hot2-a':'0','hot2-cache':'65536','hot2-overlay':'65536'}[variant]\n"
    "assert values['state.backend.cachekit.map.cache.max-entries']==expected_map",
)
replace_once(
    "assert values['state.backend.cachekit.map.cache.max-entries']=='0'",
    "assert values['state.backend.cachekit.map.cache.max-entries']==expected_map",
)
replace_once(
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
    "marker='[CACHEKIT MAP DIRTY OVERLAY]'\n"
    "activation={'enabled':variant=='hot2-overlay','marker_count':logs.count(marker)}\n"
    "if variant=='hot2-overlay':\n"
    "    import re\n"
    "    pattern=(r'\\[CACHEKIT MAP DIRTY OVERLAY\\] enabled=true iteratorRequests=(\\d+) '\n"
    "             r'flushesAvoided=(\\d+) dirtyEntriesSnapshotted=(\\d+) '\n"
    "             r'delegateOverrides=(\\d+) appendedDirtyEntries=(\\d+) '\n"
    "             r'tombstonesSuppressed=(\\d+)')\n"
    "    rows=[tuple(map(int,row)) for row in re.findall(pattern,logs)]\n"
    "    assert rows and activation['marker_count']>=len(rows)\n"
    "    totals=[sum(row[i] for row in rows) for i in range(len(rows[0]))]\n"
    "    activation.update({'rows':len(rows),'totals':totals})\n"
    "    assert totals[0]>0 and totals[1]>0 and totals[2]>0\n"
    "    assert totals[3]+totals[4]+totals[5]>0\n"
    "else:\n"
    "    assert marker not in logs",
)
replace_once(
    "'schema':'cachekit-indexed-mapstate-delta-p8-leg-v1','valid':True,",
    "'schema':'cachekit-dirty-overlay-p9-leg-v1','valid':True,",
)
replace_once(
    " 'indexed_write_batch_activation':activation,",
    " 'dirty_overlay_activation':activation,",
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
    "schema": "cachekit-dirty-overlay-p9-x86-screen-v1",
    "phase": "x86-q9-same-artifact-a-vs-a-plus-b",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "queries": ["q9"],
    "variants": list(variants),
    "primary_control": "hot2-a",
    "mechanism_control": "hot2-cache",
    "treatment": "hot2-overlay",
    "execution_environment": "idle-host-same-artifact-paired-source-mechanism-screen",
    "claim_boundary": "same x86 host and P9 artifact; map cache capacity and dirty-overlay runtime gate are explicit",
})
identity["artifact_build"] = {
    "p4_x86_base_sha256": "66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b",
    "runtime_overlay_source_commit": source_commit,
    "entry_set_exact_base_plus_overlay": True,
    "non_overlay_entries_identical_to_p4": True,
}
identity["variant_config_sha256"] = {
    variant: hashlib.sha256(
        (root / "variants" / variant / "flink-conf.yaml").read_bytes()
    ).hexdigest()
    for variant in variants
}
identity["variant_compose_sha256"] = {
    variant: hashlib.sha256(
        (root / "variants" / variant / "docker-compose.yml").read_bytes()
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

configs = {
    variant: config(root / "variants" / variant / "flink-conf.yaml")
    for variant in variants
}
config_differences = {}
for left, right in (("hot2-a", "hot2-cache"), ("hot2-cache", "hot2-overlay")):
    values = {
        key: {left: configs[left].get(key), right: configs[right].get(key)}
        for key in sorted(set(configs[left]) | set(configs[right]))
        if configs[left].get(key) != configs[right].get(key)
    }
    config_differences[f"{left}_vs_{right}"] = values
expected_config = {
    "hot2-a_vs_hot2-cache": {"state.backend.cachekit.map.cache.max-entries"},
    "hot2-cache_vs_hot2-overlay": set(),
}
compose_env = {}
for variant in variants:
    compose_text = (root / "variants" / variant / "docker-compose.yml").read_text()
    expected_overlay = variants[variant]["overlay"]
    token = f"CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '{expected_overlay}'"
    assert compose_text.count(token) == 4, (variant, token, compose_text.count(token))
    compose_env[variant] = expected_overlay
valid_config = all(
    set(config_differences[pair]) == expected
    for pair, expected in expected_config.items()
)
audit = {
    "schema": "cachekit-p9-config-diff-v1",
    "config_differences": config_differences,
    "expected_config_difference_keys": {
        pair: sorted(expected) for pair, expected in expected_config.items()
    },
    "compose_dirty_overlay_env": compose_env,
    "valid": valid_config,
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
