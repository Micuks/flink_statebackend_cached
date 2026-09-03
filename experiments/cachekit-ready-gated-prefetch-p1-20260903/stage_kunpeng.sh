#!/usr/bin/env bash
set -euo pipefail

host=root@173.154.10.2
control_path=/tmp/cachekit-kp-ssh-20260903.sock
source_exp=/home/wuql/flink-cluster/experiments/cachekit-native-stage123-javafullopt-15q-100m-r1-r3-kunpeng-20260903
target_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-kunpeng-20260903
source_commit=5a9d1e656715a403afac72ee1a516876ddbfb7f1

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
materialized=$script_dir/materialized/kunpeng
candidate=${1:-$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-aarch64.jar}

[[ -S $control_path ]] || { echo "Kunpeng SSH control socket missing" >&2; exit 63; }
ssh -S "$control_path" -O check "$host" >/dev/null
[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ -f $materialized/identity.json && -x $materialized/run_campaign.sh ]] || {
  echo "materialized Kunpeng campaign missing" >&2
  exit 65
}
candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
identity_sha=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["artifact_sha256"])' "$materialized/identity.json")
[[ $candidate_sha == "$identity_sha" ]] || {
  echo "candidate identity mismatch: $candidate_sha != $identity_sha" >&2
  exit 66
}

ssh -S "$control_path" -o BatchMode=yes "$host" bash -s -- "$source_exp" "$target_exp" <<'REMOTE'
set -euo pipefail
source_exp=$1
target_exp=$2
assert_idle() {
  [[ -z $(docker ps -q) ]] || {
    echo "host has running containers; refusing P1 staging" >&2
    docker ps --format '{{.Names}} {{.Image}}' >&2
    return 1
  }
  if pgrep -af 'measurement_integrity_leg.py|/opt/nexmark/bin/run_query.sh|com.github.nexmark.flink.Benchmark|run_campaign.sh' >&2; then
    echo "host has a live benchmark process; refusing P1 staging" >&2
    return 1
  fi
}
assert_idle || exit 72
sleep 15
assert_idle || exit 72
[[ -d $source_exp/inputs ]] || { echo "source inputs missing: $source_exp" >&2; exit 75; }
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
REMOTE

scp -q -o ControlPath="$control_path" \
  "$materialized/identity.json" "$materialized/CONFIG_DIFF_AUDIT.json" \
  "$materialized/run_campaign.sh" "$host:$target_exp/"
scp -qr -o ControlPath="$control_path" "$materialized/variants" "$host:$target_exp/"
scp -q -o ControlPath="$control_path" "$candidate" \
  "$host:$target_exp/inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
scp -q -o ControlPath="$control_path" \
  "$script_dir/audit_value_prefetch.py" "$script_dir/audit_ready_gate.py" \
  "$script_dir/summarize.py" "$host:$target_exp/"

ssh -S "$control_path" -o BatchMode=yes "$host" python3 - \
  "$source_exp" "$target_exp" "$source_commit" "$candidate_sha" <<'PY'
import hashlib,json,pathlib,sys,zipfile
source=pathlib.Path(sys.argv[1]); target=pathlib.Path(sys.argv[2])
commit=sys.argv[3]; expected=sys.argv[4]
jar=target/'inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar'
actual=hashlib.sha256(jar.read_bytes()).hexdigest()
assert actual==expected,(actual,expected)
with zipfile.ZipFile(jar) as archive:
    native=archive.read('META-INF/native/libcachekit_snapshot_jni.so')
native_sha=hashlib.sha256(native).hexdigest()
assert native[:4]==b'\x7fELF'
assert native[18:20]==bytes((183,0)),native[18:20]
assert native_sha=='6765775306c7ed00b1606de5340f0ba18ae5adc8fe242b49b336f79f96749fe2'
manifest=target/'inputs/runtime/RUNTIME_BUNDLE.json'
data=json.loads(manifest.read_text())
data['source_commit']=commit
data['architecture']='aarch64'
for artifact in data['artifacts']:
    artifact['path']=artifact['path'].replace(str(source),str(target))
    if artifact['role']=='flink_dist':
        artifact['covers_modules']=['flink-statebackend-rocksdb']
    if artifact['role']=='cachekit_module':
        artifact['sha256']=actual
        artifact['size_bytes']=jar.stat().st_size
        artifact['covers_modules']=[
            'flink-statebackend-cachekit',
            'flink-streaming-java',
        ]
        artifact['authoritative_streaming_overlay']=True
manifest.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
artifact_paths=[
 target/'inputs/runtime/flink-dist-1.16.3.jar',
 jar,
 target/'inputs/runtime/libcachekit_native_request_plane_jni.so',
]
checksum_lines=[]
for path in artifact_paths:
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    checksum_lines.append(f"{digest}  {path.relative_to(target)}")
(target/'inputs/ARTIFACTS.SHA256SUMS').write_text('\n'.join(checksum_lines)+'\n')
provenance={
 'schema':'cachekit-ready-gated-prefetch-p1-staging-v1',
 'architecture':'aarch64',
 'source_commit':commit,
 'candidate_sha256':actual,
 'candidate_size_bytes':jar.stat().st_size,
 'embedded_native_entry_sha256':native_sha,
 'reused_input_bundle_from':str(source),
 'reused_input_role':'measurement harness and unchanged runtime dependencies',
 'source_campaign_completion_required':False,
 'source_campaign_completion_exception':'user authorized previous baseline and RocksDB reuse for quick validation',
}
(target/'P1_STAGING.json').write_text(json.dumps(provenance,indent=2,sort_keys=True)+'\n')
identity=json.loads((target/'identity.json').read_text())
assert identity['source_commit']==commit and identity['artifact_sha256']==actual
assert identity['architecture']=='aarch64' and identity['platform']=='kunpeng'
for line in (target/'inputs/ARTIFACTS.SHA256SUMS').read_text().splitlines():
    digest,relative=line.split('  ',1)
    assert hashlib.sha256((target/relative).read_bytes()).hexdigest()==digest
for variant in identity['variants']:
    assert (target/'variants'/variant/'flink-conf.yaml').is_file()
    assert (target/'variants'/variant/'docker-compose.yml').is_file()
print(json.dumps(provenance,sort_keys=True))
PY

ssh -S "$control_path" -o BatchMode=yes "$host" python3 \
  "$target_exp/inputs/runner-scripts/runtime_bundle.py" \
  --manifest "$target_exp/inputs/runtime/RUNTIME_BUNDLE.json" \
  --expdir "$target_exp" --expected-source-commit "$source_commit" \
  --output "$target_exp/P1_RUNTIME_BUNDLE_AUDIT.json"

if [[ ${LAUNCH:-0} == 1 ]]; then
  ssh -S "$control_path" -o BatchMode=yes "$host" bash -s -- "$target_exp" <<'REMOTE'
set -euo pipefail
target_exp=$1
[[ -z $(docker ps -q) ]] || { echo "host became occupied before launch" >&2; exit 74; }
if pgrep -af 'measurement_integrity_leg.py|/opt/nexmark/bin/run_query.sh|com.github.nexmark.flink.Benchmark|run_campaign.sh' >&2; then
  echo "host became occupied by a benchmark process before launch" >&2
  exit 74
fi
nohup "$target_exp/run_campaign.sh" >"$target_exp/logs/nohup-r1.log" 2>&1 &
echo $! >"$target_exp/R1.pid"
echo "launched pid=$(cat "$target_exp/R1.pid")"
REMOTE
else
  echo "staged=$target_exp"
  echo "set LAUNCH=1 to launch after a fresh host-idle check"
fi
