"""Audit and freeze existing FullOpt rows; retain explicit historical RocksDB provenance."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import re

root,output=map(Path,sys.argv[1:])
identity=json.loads((root/'identity.json').read_text())
rows=[]
for marker in sorted((root/'results/raw').glob('*-r1-*-java/LEG_COMPLETE')):
    leg=marker.parent
    subprocess.run(['sha256sum','--status','-c','LEG.sha256'],cwd=leg,check=True)
    r=json.loads((leg/'result.json').read_text());m=r['measurement']
    assert r['valid'] and r['variant']=='java' and r['round']==1
    assert r['source_commit']==identity['source_commit']
    assert m['events']==100000000 and m['job_status']['real_job_completed'] and m['job_status']['measurement_integrity_valid']
    assert 0<m['cores']<=16.05 and all(x['tms']==8 and 0<x['cores']<=16.05 for x in m['cpu_metric_samples'])
    cpu=json.loads((leg/'CPU_METRIC_OWNERSHIP_PREFLIGHT.json').read_text())
    assert cpu['valid'] and not (leg/'NUMA_COLOCATION_VIOLATION').exists()
    assert hashlib.sha256((leg/'flink-conf.yaml').read_bytes()).hexdigest()==identity['variant_config_sha256']['java']
    rows.append({'query':r['query'],'raw_kps':m['raw_throughput_kps'],'cores':m['cores'],'kps_core':m['throughput_kps_core'],
                 'source_commit':r['source_commit'],'leg':str(leg),'config_sha256':r['config_sha256']})
assert len(rows)==15 and {r['query'] for r in rows}==set(identity['queries'])
rocks=json.loads((root/'inputs/reused-rocksdb-r1.json').read_text())
assert rocks['events']==100000000 and rocks['platform']==identity['platform']
assert set(rocks['throughput_kps_core'])==set(identity['queries'])
rocksroot=Path(rocks['source_campaign'])
raw=rocksroot if rocksroot.name=='raw' else rocksroot/'results/raw'
rocksrows=[]
for marker in sorted(raw.glob('*-r1-*-rdb/LEG_COMPLETE')):
    leg=marker.parent
    subprocess.run(['sha256sum','--status','-c','LEG.sha256'],cwd=leg,check=True)
    r=json.loads((leg/'result.json').read_text());m=r['measurement']
    assert r['valid'] and r['variant']=='rdb' and r['round']==1
    assert m['events']==100000000 and m['job_status']['real_job_completed'] and m['job_status']['measurement_integrity_valid']
    observed=[int(x.replace(',','')) for x in re.findall(r'EventsNum=([0-9,]+), Cores=',(leg/'query.log').read_text())]
    assert observed and all(x==100000000 for x in observed)
    assert 0<m['cores']<=16.05 and all(x['tms']==8 and 0<x['cores']<=16.05 for x in m['cpu_metric_samples'])
    cpu=json.loads((leg/'CPU_METRIC_OWNERSHIP_PREFLIGHT.json').read_text());assert cpu['valid']
    assert abs(m['throughput_kps_core']-rocks['throughput_kps_core'][r['query']])<.001
    assert 'execution.checkpointing.interval:' not in (leg/'flink-conf.yaml').read_text()
    rocksrows.append({'query':r['query'],'raw_kps':m['raw_throughput_kps'],'cores':m['cores'],
                     'kps_core':m['throughput_kps_core'],'leg':str(leg),'source_commit':r['source_commit'],
                     'config_sha256':hashlib.sha256((leg/'flink-conf.yaml').read_bytes()).hexdigest(),
                     'cpu_ownership':cpu})
assert len(rocksrows)==15 and {r['query'] for r in rocksrows}==set(identity['queries'])
rocks['audited_raw_rows']=rocksrows
output.write_text(json.dumps({'platform':identity['platform'],'fullopt_identity':identity,'fullopt':rows,'rocksdb':rocks,
    'boundary':'Both historical baselines raw-leg hashes, events and CPU ownership revalidated. RocksDB remains same-host historical secondary reference; temporal/NUMA/runtime mismatches are not fresh causal control.'},indent=2)+'\n')
print(json.dumps({'fullopt_valid':len(rows),'rocksdb_reference_queries':len(rocks['throughput_kps_core'])}))
