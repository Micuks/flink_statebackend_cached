"""Freeze tested class overlays; never include credentials or build outputs in Git."""
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / 'local-artifacts'
OUT.mkdir(exist_ok=True)
old = ROOT / 'experiments/cachekit-point-value-memo-p20-20260905'
audit = json.loads((old / 'p30-build/BUILD_AUDIT.json').read_text())
sha = lambda b: hashlib.sha256(b).hexdigest()
commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
sources = audit['runtime_sources']
for name, expected in sources.items():
    assert sha((ROOT / name).read_bytes()) == expected, name
    assert subprocess.check_output(['git', 'show', commit+':'+name], cwd=ROOT) == (ROOT/name).read_bytes()
stems = {
    'cachekit': ['org/apache/flink/contrib/streaming/state/cachekit/state/'+n for n in ['CachedInternalMapState', 'CachedInternalValueState']],
    'rocksdb': ['org/apache/flink/contrib/streaming/state/'+n for n in ['RocksDBBatchValueReader', 'RocksDBValueState']],
    'table': ['org/apache/flink/table/data/binary/BinaryStringData'],
}
inputs = {
    'cachekit': ROOT / 'flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p29-x86.jar',
    'rocksdb': ROOT / 'flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p29-x86.jar',
    'table': old / 'p30-build/flink-table-api-java-uber-1.16.3-p30.jar',
}
assert sha(inputs['cachekit'].read_bytes()) == audit['p29_artifact_sha256']
assert sha(inputs['table'].read_bytes()) == audit['table_artifact']['sha256']
entries = {}
with zipfile.ZipFile(OUT/'tested-classes.zip', 'w') as dest:
    for group, path in inputs.items():
        with zipfile.ZipFile(path) as src:
            for name in src.namelist():
                if name.endswith('.class') and any(name == s+'.class' or name.startswith(s+'$') for s in stems[group]):
                    data = src.read(name)
                    dest.writestr(group+'/'+name, data)
                    entries[group+'/'+name] = sha(data)
patch = subprocess.check_output(['git', 'diff', '7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000', commit, '--']+list(sources), cwd=ROOT)
(OUT/'SOURCE_DELTA.patch').write_bytes(patch)
result = {'schema':'fullopt-p29-p30-tested-overlay-v1', 'source_commit':commit,
          'fullopt_base_commit':'7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000',
          'runtime_sources':sources, 'stems':stems, 'class_sha256':entries,
          'payload_sha256':sha((OUT/'tested-classes.zip').read_bytes()),
          'source_delta_sha256':sha(patch),
          'tests':{'p29_distinct':128,'p29_executions':255,'p30_distinct':123,'p30_executions':246},
          'boundary':'Current committed source classes over preserved FullOpt runtime; dormant exploratory gates OFF. Historical baselines are non-contemporaneous, not a fresh causal A/B.'}
(OUT/'PAYLOAD.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result, indent=2))
