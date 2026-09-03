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
assert_target_numa_disjoint() {
  python3 - '38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74' <<'PY'
import json
import subprocess
import sys


def expand(spec):
    result = set()
    for part in spec.split(','):
        bounds = [int(value) for value in part.split('-', 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


target = expand(sys.argv[1])
ids = subprocess.check_output(['docker', 'ps', '-q'], text=True).split()
for container_id in ids:
    data = json.loads(subprocess.check_output(['docker', 'inspect', container_id]))[0]
    cpus = data['HostConfig'].get('CpusetCpus', '')
    name = data['Name']
    if not cpus or expand(cpus) & target:
        raise SystemExit(
            f"container overlaps target NUMA CPU set: {name} cpus={cpus or 'unbounded'}"
        )
    print(f"allowed disjoint container: {name} cpus={cpus}")
PY
}
assert_target_numa_disjoint || exit 72
sleep 15
assert_target_numa_disjoint || exit 72
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
import datetime,hashlib,json,pathlib,re,subprocess,sys,zipfile
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
def expand(spec):
    result=set()
    for part in spec.split(','):
        bounds=[int(value) for value in part.split('-',1)]
        result.update(range(bounds[0],bounds[-1]+1))
    return result
target_cpu_spec='38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74'
target_cpus=expand(target_cpu_spec)
node0_spec=pathlib.Path('/sys/devices/system/node/node0/cpulist').read_text().strip()
assert target_cpus <= expand(node0_spec),(target_cpu_spec,node0_spec)
node0_meminfo=pathlib.Path('/sys/devices/system/node/node0/meminfo').read_text()
node0_memfree_kb=int(re.search(r'MemFree:\s+(\d+) kB',node0_meminfo).group(1))
required_process_memory_kb=72*1024*1024
assert node0_memfree_kb>=required_process_memory_kb,(node0_memfree_kb,required_process_memory_kb)
foreign=[]
for container_id in subprocess.check_output(['docker','ps','-q'],text=True).split():
    container=json.loads(subprocess.check_output(['docker','inspect',container_id]))[0]
    cpus=container['HostConfig'].get('CpusetCpus','')
    assert cpus and not (expand(cpus) & target_cpus),(container['Name'],cpus)
    foreign.append({
        'name':container['Name'],
        'image':container['Config']['Image'],
        'cpuset_cpus':cpus,
        'cpuset_mems':container['HostConfig'].get('CpusetMems',''),
    })
mount=json.loads(subprocess.check_output(
    ['findmnt','-T','/tmp','-J','-o','TARGET,SOURCE,FSTYPE,OPTIONS'],text=True
))['filesystems'][0]
numa_audit={
 'schema':'cachekit-ready-gated-prefetch-p1-numa-isolation-v1',
 'captured_at':datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat(),
 'valid_for_quick_validation':True,
 'valid_for_idle_host_final_evidence':False,
 'target_numa_node':0,
 'target_node_cpulist':node0_spec,
 'target_allocated_cpus':target_cpu_spec,
 'target_cpuset_mems':'0',
 'target_node_memfree_kb_before_launch':node0_memfree_kb,
 'configured_jm_and_tm_process_memory_kb':required_process_memory_kb,
 'foreign_containers':foreign,
 'foreign_cpu_sets_disjoint':True,
 'foreign_memory_hard_isolation':all(item['cpuset_mems'] not in ('','0-3') for item in foreign),
 'scratch_mount':mount,
 'claim_boundary':'co-located NUMA-isolated quick validation only; not idle-host final evidence',
}
(target/'P1_NUMA_ISOLATION_AUDIT.json').write_text(
    json.dumps(numa_audit,indent=2,sort_keys=True)+'\n'
)
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
 'execution_environment':'numa-isolated-colocated-quick-validation',
 'numa_isolation_audit':'P1_NUMA_ISOLATION_AUDIT.json',
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
python3 - '38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74' <<'PY'
import json
import subprocess
import sys


def expand(spec):
    result = set()
    for part in spec.split(','):
        bounds = [int(value) for value in part.split('-', 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


target = expand(sys.argv[1])
for container_id in subprocess.check_output(['docker', 'ps', '-q'], text=True).split():
    data = json.loads(subprocess.check_output(['docker', 'inspect', container_id]))[0]
    cpus = data['HostConfig'].get('CpusetCpus', '')
    if not cpus or expand(cpus) & target:
        raise SystemExit(
            f"host became occupied on target NUMA CPUs: {data['Name']} "
            f"cpus={cpus or 'unbounded'}"
        )
PY
nohup "$target_exp/run_campaign.sh" >"$target_exp/logs/nohup-r1.log" 2>&1 &
echo $! >"$target_exp/R1.pid"
echo "launched pid=$(cat "$target_exp/R1.pid")"
REMOTE
else
  echo "staged=$target_exp"
  echo "set LAUNCH=1 to launch after a fresh host-idle check"
fi
