"""Stage ONLY the new treatment over an archived FullOpt runtime, without launch."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import zipfile
import yaml
from zip_overlay import copy_entry

p = argparse.ArgumentParser()
p.add_argument('platform', choices=['x86', 'kunpeng'])
p.add_argument('--stage', type=Path, required=True)
a = p.parse_args()
base = Path('/home/wuql/flink-cluster/experiments')
old = base / ('cachekit-native-stage23-fullopt-100m-r1-'+a.platform+'-20260907')
root = base / ('cachekit-fullopt-p29-p30-100m-'+a.platform+'-20260908')
oldproject = 'cks23x86907' if a.platform=='x86' else 'cks23kp907'
project = 'ckp31x860908' if a.platform=='x86' else 'ckp31kp0908'
payload = json.loads((a.stage/'PAYLOAD.json').read_text())
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
save = lambda p, x: p.write_text(json.dumps(x, indent=2, sort_keys=True)+'\n')
assert sha(a.stage/'tested-classes.zip') == payload['payload_sha256']
assert not root.exists(), root
for port in [12980, 12981, 12982]:
    assert not subprocess.check_output(['ss', '-ltnH', 'sport = :'+str(port)]).strip(), port
root.mkdir()
shutil.copytree(old/'inputs', root/'inputs')
shutil.copytree(old/'variants/java', root/'variants/p29p30')
for name in ['PAYLOAD.json', 'SOURCE_DELTA.patch', 'tested-classes.zip']:
    shutil.copy2(a.stage/name, root/'inputs'/name)
for name in ['audit_leg.py', 'export.py', 'preflight.py', 'capture_memory_gc.py', 'export_baseline.py', 'cpu_capacity.py']:
    shutil.copy2(a.stage/name, root/name)

def remap(text):
    text = text.replace(str(old), str(root)).replace(oldproject, project)
    return text.replace('12888', '12980').replace('12921', '12981').replace('12922', '12982')

config = root/'variants/p29p30/flink-conf.yaml'
assert sha(config) == sha(old/'variants/java/flink-conf.yaml')
reference = (a.stage/'cachekit-optimization-definitions.md').read_text()
contract = yaml.safe_load(reference.split('## Portable `fullopt`')[1].split('```yaml')[1].split('```')[0])
class UniqueLoader(yaml.SafeLoader): pass
def unique(loader, node):
    result = {}
    for key, value in node.value:
        k = loader.construct_object(key)
        assert k not in result, ('duplicate config', k)
        result[k] = loader.construct_object(value)
    return result
UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique)
values = yaml.load(config.read_text(), Loader=UniqueLoader)
assert 'execution.checkpointing.interval' not in values
# The user explicitly permits any MultiGet threshold; retain the reused control's value.
for k, v in contract.items():
    if k == 'state.backend.cachekit.bp-prefetch.multiget.min-batch-size':
        assert int(values[k]) > 0
    else:
        assert values.get(k) == v, (k, values.get(k), v)
compose_path = root/'variants/p29p30/docker-compose.yml'
compose = yaml.safe_load(remap(compose_path.read_text()).replace('/variants/java/', '/variants/p29p30/'))
flinkhome = '/opt/flink' if a.platform=='x86' else '/opt/flink-1.16.3'
disabled = ['CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED', 'CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED',
    'CACHEKIT_MAP_SNAPSHOT_VALUE_AUTHORITY_ENABLED', 'CACHEKIT_MAP_POINT_VALUE_MEMO_ENABLED',
    'CACHEKIT_MAP_POINT_VALUE_MEMO_USEFULNESS_GATE_ENABLED', 'CACHEKIT_MAP_POINT_VALUE_MEMO_FIRST_ENABLED',
    'CACHEKIT_VALUE_PREFETCH_EARLY_SMALL_BATCH_ENABLED', 'CACHEKIT_VALUE_PREFETCH_JAVA_ACCESS_GUIDED_ENABLED',
    'CACHEKIT_VALUE_OWNED_KEY_HASH_CACHE_ENABLED', 'CACHEKIT_VALUE_STICKY_L1_EPOCH_ENABLED']
table = root/'inputs/runtime/flink-table-api-java-uber-1.16.3.jar'
table_base = a.stage/'table-base.jar'
assert sha(table_base) == '76363871aa4230c80f0d3c0591e357388572bea688081a52e8f46f638872eb62'
shutil.copy2(table_base, table)
for service in [compose.get('x-flink-common', {})]+[compose['services'][n] for n in ['jobmanager','taskmanager1','taskmanager2']]:
    env = service.setdefault('environment', {})
    env.update({k:'false' for k in disabled})
    env.update(CACHEKIT_MAP_POINT_VALUE_MEMO_MAX_OWNERS='0', CACHEKIT_VALUE_EVICTION_WRITE_BATCH_ENABLED='true',
               FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED='true')
    service['volumes'] = [x for x in service.get('volumes', []) if 'flink-table-api-java-uber-1.16.3.jar:' not in x]
    service['volumes'].append(str(table)+':'+flinkhome+'/lib/'+table.name+':ro')
compose_path.write_text(yaml.safe_dump(compose, sort_keys=False))

manifest_path = root/('inputs/artifacts/opt/RUNTIME_BUNDLE.json' if a.platform=='x86' else 'inputs/runtime/RUNTIME_BUNDLE.json')
manifest = json.loads(remap(manifest_path.read_text()))
original_manifest = json.loads((old/manifest_path.relative_to(root)).read_text())
for item in manifest['artifacts']:
    assert sha(Path(item['path'])) == item['sha256'], item['path']
overlay_audits = []
with zipfile.ZipFile(a.stage/'tested-classes.zip') as z:
    all_classes = {name:z.read(name) for name in z.namelist()}
for item in manifest['artifacts']+[{'path':str(table),'role':'table_common'}]:
    path = Path(item['path'])
    groups = ['cachekit','rocksdb'] if item['role']=='cachekit_module' else ['rocksdb'] if item['role']=='flink_dist' else ['table']
    with zipfile.ZipFile(path) as src:
        old_names = set(src.namelist())
        actual_groups = [g for g in groups if any(stem+'.class' in old_names for stem in payload['stems'][g])]
        overlays = {name.split('/',1)[1]:data for name,data in all_classes.items() if name.split('/',1)[0] in actual_groups}
        assert overlays, (path, groups)
        replace = lambda n: n.endswith('.class') and any(n==s+'.class' or n.startswith(s+'$') for g in actual_groups for s in payload['stems'][g])
        before = sha(path)
        temp = path.with_suffix('.p31-tmp.jar')
        with zipfile.ZipFile(temp,'w') as dst:
            for info in src.infolist():
                if not replace(info.filename): copy_entry(src,dst,info)
            for name,data in overlays.items(): dst.writestr(name,data)
        with zipfile.ZipFile(temp) as dst:
            assert dst.testzip() is None
            assert set(dst.namelist()) == {n for n in old_names if not replace(n)} | set(overlays)
            assert all(dst.read(n)==src.read(n) for n in old_names if not replace(n))
    temp.replace(path)
    overlay_audits.append({'path':str(path),'base_sha256':before,'sha256':sha(path),
                           'non_overlay_entries_identical':True,'classes':{n:hashlib.sha256(b).hexdigest() for n,b in overlays.items()}})
    item.update(sha256=sha(path),size_bytes=path.stat().st_size)
    if item['role']=='table_common':
        item.update(container_filename=path.name,covers_modules=['flink-table-common'],outer_hash_key='table_common_sha256')
        manifest['artifacts'].append(item)
manifest.update(source_commit=payload['source_commit'])
save(manifest_path, manifest)
helper = root/'inputs/runner-scripts/runtime_bundle.py'
text = helper.read_text()
assert text.count('REQUIRED_MODULES = {')==1
helper.write_text(text.replace('REQUIRED_MODULES = {','REQUIRED_MODULES = {\n    "flink-table-common",'))
manifest_path.with_suffix('.sha256').write_text(sha(manifest_path)+'  '+manifest_path.name+'\n')
identity = json.loads((old/'identity.json').read_text())
identity.update(schema='fullopt-p29-p30-reused-baselines-v1', source_commit=payload['source_commit'],
    variants=['p29p30'], rounds=[1], campaign=str(root), project=project,
    reference_campaign=str(old), reference_source_commit=payload['fullopt_base_commit'],
    variant_config_sha256={'p29p30':sha(config)}, variant_compose_sha256={'p29p30':sha(compose_path)},
    reused_fullopt_config_sha256=sha(config), runtime_manifest=str(manifest_path),
    comparison='ONLY new FullOpt+P29+P30; historical FullOpt and RocksDB reused, non-contemporaneous',
    optimized_label='FullOpt+P29+P30', disabled_exploratory_environment=disabled,
    claim_boundary=payload['boundary'], overlay_audits=overlay_audits)
save(root/'identity.json', identity)
save(root/'FULLopt_CONFIG_AUDIT.json', {'valid':True,'unchanged_from_reused_fullopt':True,'config_sha256':sha(config),'contract_keys':contract})
save(root/'CONFIG_DIFF_AUDIT.json', {'valid':True,'same_fullopt_config':True,'only_treatment':'p29p30'})
runner = remap((old/'run_campaign.sh').read_text()).replace(identity['reference_source_commit'],payload['source_commit'])
runner = runner.replace('/variants/java/', '/variants/p29p30/').replace('all_variants=(java stage23)', 'all_variants=(p29p30)')
runner = runner.replace('${VARIANTS_TEXT:-java stage23}', '${VARIANTS_TEXT:-p29p30}')
runner = runner.replace(" 'java':", " 'p29p30':").replace('java) echo 1', 'p29p30) echo 1')
# Keep existing real-job, native-off, CPU and NUMA checks, add a separate strict treatment audit.
needle='  cp "$d/measurement-result.json" "$d/MEASUREMENT_EVIDENCE.json"'
assert runner.count(needle)==1
runner=runner.replace(needle,'  python3 "$expdir/capture_memory_gc.py" "$d" "$prom" >"$d/memory-gc.stdout"\n  python3 "$expdir/audit_leg.py" "$expdir" "$d"\n'+needle)
needle='  local id owner_project name cpus mems image metadata foreign=0'
assert runner.count(needle)==1
runner=runner.replace(needle,needle+'\n  if [[ $platform == kunpeng ]]; then\n    python3 "$expdir/preflight.py" --wutb-only --project "$project" || return 1\n  fi')
# One requested sweep only, never allow environment variables to launch old arms or more rounds.
runner=runner.replace('read -r -a rounds <<<"${ROUNDS_TEXT:-1}"','rounds=(1)')
runner=runner.replace('read -r -a variants <<<"${VARIANTS_TEXT:-p29p30}"','variants=(p29p30)')
runner=runner.replace('    idx=$(( (round-1)*60 + (qpos-1)*4 + vpos ))','    idx=$qpos')
(root/'run_campaign.sh').write_text(runner)
paths = [Path(x['path']) for x in manifest['artifacts']]+[manifest_path,helper,root/'audit_leg.py',root/'preflight.py']
(root/'inputs/ARTIFACTS.SHA256SUMS').write_text(''.join(sha(f)+'  '+str(f.relative_to(root))+'\n' for f in paths))
subprocess.run(['bash','-n',str(root/'run_campaign.sh')],check=True)
subprocess.run(['/home/wuql/bin/docker-compose','-p',project,'-f',str(compose_path),'config','-q'],check=True)
subprocess.run(['python3',str(helper),'--manifest',str(manifest_path),'--expdir',str(root),'--expected-source-commit',payload['source_commit'],'--output',str(root/'RUNTIME_VALIDATION.json')],check=True)
subprocess.run(['python3',str(root/'export_baseline.py'),str(old),str(root/'REUSED_BASELINES.json')],check=True)
print(json.dumps({'root':str(root),'project':project,'config_unchanged':True,'staged_only':True}))
