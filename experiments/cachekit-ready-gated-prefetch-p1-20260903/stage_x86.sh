#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
source_exp=/home/wuql/flink-cluster/experiments/cachekit-native-stage123-javafullopt-15q-100m-r1-r3-x86-20260903
target_exp=/home/wuql/flink-cluster/experiments/cachekit-ready-gated-prefetch-p1-q9-100m-x86-20260903
source_commit=5a9d1e656715a403afac72ee1a516876ddbfb7f1

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
materialized=$script_dir/materialized/x86
candidate=${1:-$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar}

[[ -f $candidate ]] || { echo "candidate missing: $candidate" >&2; exit 64; }
[[ -f $materialized/identity.json && -x $materialized/run_campaign.sh ]] || {
  echo "materialized x86 campaign missing" >&2
  exit 65
}
candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
identity_sha=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["artifact_sha256"])' "$materialized/identity.json")
[[ $candidate_sha == "$identity_sha" ]] || {
  echo "candidate identity mismatch: $candidate_sha != $identity_sha" >&2
  exit 66
}

ssh -o BatchMode=yes "$host" bash -s -- "$source_exp" "$target_exp" <<'REMOTE'
set -euo pipefail
source_exp=$1
target_exp=$2
p=$(cat "$source_exp/R1.pid" 2>/dev/null || true)
if [[ -n $p ]] && kill -0 "$p" 2>/dev/null; then
  echo "source activation campaign still running: $p" >&2
  exit 70
fi
[[ -f $source_exp/R1_COMPLETE ]] || {
  echo "source activation campaign lacks R1_COMPLETE" >&2
  exit 71
}
[[ -z $(docker ps -q) ]] || {
  echo "host has running containers; refusing P1 staging" >&2
  docker ps --format '{{.Names}} {{.Image}}' >&2
  exit 72
}
[[ ! -e $target_exp ]] || { echo "target already exists: $target_exp" >&2; exit 73; }
mkdir -p "$target_exp/logs"
cp -a "$source_exp/inputs" "$target_exp/inputs"
REMOTE

scp -q "$materialized/identity.json" "$materialized/CONFIG_DIFF_AUDIT.json" \
  "$materialized/run_campaign.sh" "$host:$target_exp/"
scp -qr "$materialized/variants" "$host:$target_exp/"
scp -q "$candidate" \
  "$host:$target_exp/inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
scp -q "$script_dir/audit_value_prefetch.py" "$script_dir/audit_ready_gate.py" \
  "$script_dir/summarize.py" "$host:$target_exp/"

ssh -o BatchMode=yes "$host" python3 - "$source_exp" "$target_exp" "$source_commit" "$candidate_sha" <<'PY'
import hashlib,json,pathlib,sys
source=pathlib.Path(sys.argv[1]); target=pathlib.Path(sys.argv[2])
commit=sys.argv[3]; expected=sys.argv[4]
jar=target/'inputs/artifacts/opt/flink-statebackend-cachekit-1.16-SNAPSHOT.jar'
actual=hashlib.sha256(jar.read_bytes()).hexdigest()
assert actual==expected,(actual,expected)
manifest=target/'inputs/artifacts/opt/RUNTIME_BUNDLE.json'
data=json.loads(manifest.read_text())
data['source_commit']=commit
for artifact in data['artifacts']:
    artifact['path']=artifact['path'].replace(str(source),str(target))
    if artifact['role']=='cachekit_module':
        artifact['sha256']=actual
        artifact['size_bytes']=jar.stat().st_size
manifest.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
artifact_paths=[
 target/'inputs/runtime/flink-dist-1.16.3.jar',
 jar,
 target/'inputs/artifacts/opt/libcachekit_native_request_plane_jni.so',
]
checksum_lines=[]
for path in artifact_paths:
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    checksum_lines.append(f"{digest}  {path.relative_to(target)}")
(target/'inputs/ARTIFACTS.SHA256SUMS').write_text('\n'.join(checksum_lines)+'\n')
provenance={
 'schema':'cachekit-ready-gated-prefetch-p1-staging-v1',
 'source_commit':commit,
 'candidate_sha256':actual,
 'candidate_size_bytes':jar.stat().st_size,
 'reused_input_bundle_from':str(source),
 'reused_input_role':'measurement harness and unchanged runtime dependencies',
}
(target/'P1_STAGING.json').write_text(json.dumps(provenance,indent=2,sort_keys=True)+'\n')
identity=json.loads((target/'identity.json').read_text())
assert identity['source_commit']==commit and identity['artifact_sha256']==actual
for line in (target/'inputs/ARTIFACTS.SHA256SUMS').read_text().splitlines():
    digest,relative=line.split('  ',1)
    assert hashlib.sha256((target/relative).read_bytes()).hexdigest()==digest
for variant in identity['variants']:
    assert (target/'variants'/variant/'flink-conf.yaml').is_file()
    assert (target/'variants'/variant/'docker-compose.yml').is_file()
print(json.dumps(provenance,sort_keys=True))
PY

if [[ ${LAUNCH:-0} == 1 ]]; then
  ssh -o BatchMode=yes "$host" bash -s -- "$target_exp" <<'REMOTE'
set -euo pipefail
target_exp=$1
[[ -z $(docker ps -q) ]] || { echo "host became occupied before launch" >&2; exit 74; }
nohup "$target_exp/run_campaign.sh" >"$target_exp/logs/nohup-r1.log" 2>&1 &
echo $! >"$target_exp/R1.pid"
echo "launched pid=$(cat "$target_exp/R1.pid")"
REMOTE
else
  echo "staged=$target_exp"
  echo "set LAUNCH=1 to launch after a fresh host-idle check"
fi
