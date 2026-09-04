#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-p9-dirty-overlay-q9-100m-x86-20260904
target_exp=/home/wuql/flink-cluster/experiments/cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904
source_project=ckx865a9p9
target_project=ckx865a9p10
source_scratch=/tmp/ckx865a9p9
target_scratch=/tmp/ckx865a9p10
source_rest_port=10826
target_rest_port=10834
source_prom_port=11901
target_prom_port=11917
source_push_port=11902
target_push_port=11918
source_commit=8780838608a9c4ef1f374b91873ad3be7f576782
artifact_sha=09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p10-x86.jar
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ $(sha256sum "$candidate" | awk '{print $1}') == "$artifact_sha" ]] || {
  echo "candidate hash mismatch" >&2
  exit 65
}

stage=/tmp/cachekit-p10-snapshot-maintenance-stage-20260904
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

[[ -f $source_exp/identity.json ]]
[[ -f $source_exp/CONFIG_DIFF_AUDIT.json ]]
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
for port in "$target_rest_port" "$target_prom_port" "$target_push_port"; do
  ! ss -ltnH "sport = :$port" | grep -q . || { echo "port already in use: $port" >&2; exit 74; }
done

variants=(hot2-a hot2-overlay hot2-maintained)
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
cp "$stage/flink-statebackend-cachekit-1.16-SNAPSHOT-p10-x86.jar" \
  "$target_exp/inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"

python3 - "$target_exp" "$source_exp" "$target_exp" "$source_project" \
  "$target_project" "$source_scratch" "$target_scratch" "$source_rest_port" \
  "$target_rest_port" "$source_prom_port" "$target_prom_port" \
  "$source_push_port" "$target_push_port" "$source_commit" "$artifact_sha" <<'PY'
import hashlib
import json
import pathlib
import re
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
    "hot2-a": {"map_cache": "0", "overlay": "false", "maintenance": "false"},
    "hot2-overlay": {"map_cache": "65536", "overlay": "true", "maintenance": "false"},
    "hot2-maintained": {"map_cache": "65536", "overlay": "true", "maintenance": "true"},
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
    overlay_pattern = re.compile(
        r"(?m)^(\s*)CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: 'false'$"
    )
    matches = list(overlay_pattern.finditer(text))
    if len(matches) != 4:
        raise SystemExit(f"overlay env count {len(matches)} in {compose}; expected 4")
    text = overlay_pattern.sub(
        lambda match: match.group(1)
        + "CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '"
        + settings["overlay"]
        + "'\n"
        + match.group(1)
        + "CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED: '"
        + settings["maintenance"]
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
    "source_commit=56aaafe595fa51df07a4ea371f255751375d7884",
    f"source_commit={source_commit}",
)
replace_once(
    "artifact_sha=2d9214f7afbe25997b2762e66025647acfc11258954eadb148940d55aceaafb5",
    f"artifact_sha={artifact_sha}",
)
replace_once(
    "variants=(hot2-a hot2-cache hot2-overlay)",
    "variants=(hot2-a hot2-overlay hot2-maintained)",
)
replace_once(
    "expected_map={'hot2-a':'0','hot2-cache':'65536','hot2-overlay':'65536'}[variant]",
    "expected_map={'hot2-a':'0','hot2-overlay':'65536','hot2-maintained':'65536'}[variant]",
)

activation_start = "marker='[CACHEKIT MAP DIRTY OVERLAY]'\n"
before, separator, remainder = text.partition(activation_start)
if not separator:
    raise SystemExit("dirty overlay activation start not found")
discarded, separator, after = remainder.partition("result={\n")
if not separator:
    raise SystemExit("result block after activation not found")
activation = r'''overlay_marker='[CACHEKIT MAP DIRTY OVERLAY]'
overlay_enabled=variant in ('hot2-overlay','hot2-maintained')
overlay_activation={'enabled':overlay_enabled,'marker_count':logs.count(overlay_marker)}
if overlay_enabled:
    import re
    pattern=(r'\[CACHEKIT MAP DIRTY OVERLAY\] enabled=true iteratorRequests=(\d+) '
             r'flushesAvoided=(\d+) dirtyEntriesSnapshotted=(\d+) '
             r'delegateOverrides=(\d+) appendedDirtyEntries=(\d+) '
             r'tombstonesSuppressed=(\d+)')
    rows=[tuple(map(int,row)) for row in re.findall(pattern,logs)]
    assert rows and overlay_activation['marker_count']>=len(rows)
    totals=[sum(row[i] for row in rows) for i in range(len(rows[0]))]
    overlay_activation.update({'rows':len(rows),'totals':totals})
    assert totals[0]>0 and totals[1]>0 and totals[2]>0
    assert totals[3]+totals[4]+totals[5]>0
else:
    assert overlay_marker not in logs

maintenance_marker='[CACHEKIT MAP SNAPSHOT MAINTENANCE]'
maintenance_enabled=variant=='hot2-maintained'
maintenance_activation={'enabled':maintenance_enabled,'marker_count':logs.count(maintenance_marker)}
if maintenance_enabled:
    pattern=(r'\[CACHEKIT MAP SNAPSHOT MAINTENANCE\] enabled=true putAttempts=(\d+) '
             r'removeAttempts=(\d+) knownNoops=(\d+) updates=(\d+) '
             r'overflowInvalidations=(\d+)')
    rows=[tuple(map(int,row)) for row in re.findall(pattern,logs)]
    assert rows and maintenance_activation['marker_count']>=len(rows)
    totals=[sum(row[i] for row in rows) for i in range(len(rows[0]))]
    maintenance_activation.update({'rows':len(rows),'totals':totals})
    assert totals[0]>0 and totals[2]>0
else:
    assert maintenance_marker not in logs

snapshot_pattern=(r'\[CACHEKIT MAP SNAPSHOT CACHE\] enabled=(true|false) probes=(\d+) '
                  r'hits=(\d+) misses=(\d+) emptyShortCircuits=(\d+) '
                  r'singleShortCircuits=(\d+) smallShortCircuits=(\d+)')
snapshot_rows=re.findall(snapshot_pattern,logs)
assert snapshot_rows and all(row[0]=='true' for row in snapshot_rows)
snapshot_activation={
    'rows':len(snapshot_rows),
    'probes':sum(int(row[1]) for row in snapshot_rows),
    'hits':sum(int(row[2]) for row in snapshot_rows),
    'misses':sum(int(row[3]) for row in snapshot_rows),
    'empty_short_circuits':sum(int(row[4]) for row in snapshot_rows),
    'single_short_circuits':sum(int(row[5]) for row in snapshot_rows),
    'small_short_circuits':sum(int(row[6]) for row in snapshot_rows),
}
if maintenance_enabled:
    assert snapshot_activation['single_short_circuits']>0
'''
text = before + activation + "result={\n" + after
replace_once(
    "'schema':'cachekit-dirty-overlay-p9-leg-v1','valid':True,",
    "'schema':'cachekit-snapshot-maintenance-p10-leg-v1','valid':True,",
)
replace_once(
    " 'dirty_overlay_activation':activation,",
    " 'dirty_overlay_activation':overlay_activation,\n"
    " 'snapshot_maintenance_activation':maintenance_activation,\n"
    " 'map_snapshot_activation':snapshot_activation,",
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
    "schema": "cachekit-snapshot-maintenance-p10-x86-screen-v1",
    "phase": "x86-q9-same-artifact-incremental-exact-snapshot-screen",
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "artifact_size_bytes": (
        root / "inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
    ).stat().st_size,
    "queries": ["q9"],
    "variants": list(variants),
    "primary_control": "hot2-a",
    "mechanism_control": "hot2-overlay",
    "treatment": "hot2-maintained",
    "execution_environment": "idle-host-same-artifact-paired-source-mechanism-screen",
    "claim_boundary": "same x86 host and P10 artifact; dirty overlay and incrementally maintained exact snapshot gates are explicit",
})
identity["artifact_build"] = {
    "p9_x86_base_sha256": "2d9214f7afbe25997b2762e66025647acfc11258954eadb148940d55aceaafb5",
    "runtime_overlay_source_commit": source_commit,
    "entry_set_identical_to_p9": True,
    "non_overlay_entries_identical_to_p9": True,
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
for left, right in (("hot2-a", "hot2-overlay"), ("hot2-overlay", "hot2-maintained")):
    values = {
        key: {left: configs[left].get(key), right: configs[right].get(key)}
        for key in sorted(set(configs[left]) | set(configs[right]))
        if configs[left].get(key) != configs[right].get(key)
    }
    config_differences[f"{left}_vs_{right}"] = values
expected_config = {
    "hot2-a_vs_hot2-overlay": {"state.backend.cachekit.map.cache.max-entries"},
    "hot2-overlay_vs_hot2-maintained": set(),
}
compose_env = {}
for variant, settings in variants.items():
    compose_text = (root / "variants" / variant / "docker-compose.yml").read_text()
    overlay_token = f"CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '{settings['overlay']}'"
    maintenance_token = (
        "CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED: "
        f"'{settings['maintenance']}'"
    )
    assert compose_text.count(overlay_token) == 4
    assert compose_text.count(maintenance_token) == 4
    compose_env[variant] = {
        "dirty_overlay": settings["overlay"],
        "snapshot_maintenance": settings["maintenance"],
    }
valid_config = all(
    set(config_differences[pair]) == expected
    for pair, expected in expected_config.items()
)
audit = {
    "schema": "cachekit-p10-config-diff-v1",
    "config_differences": config_differences,
    "expected_config_difference_keys": {
        pair: sorted(expected) for pair, expected in expected_config.items()
    },
    "compose_runtime_env": compose_env,
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
