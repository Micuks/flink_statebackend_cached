"""Independent new-treatment checks in addition to frozen FullOpt runner gates."""
import hashlib
import json
from pathlib import Path
import re
import sys
import yaml

root,leg=map(Path,sys.argv[1:])
identity=json.loads((root/'identity.json').read_text())
r=json.loads((leg/'result.json').read_text())
m=r['measurement']
assert r['valid'] and r['variant']=='p29p30' and r['source_commit']==identity['source_commit']
assert m['events']==100000000
observed=[int(n.replace(',','')) for n in re.findall(r'EventsNum=([0-9,]+), Cores=',(leg/'query.log').read_text())]
assert observed and all(n==100000000 for n in observed)
assert hashlib.sha256((leg/'flink-conf.yaml').read_bytes()).hexdigest()==identity['reused_fullopt_config_sha256']
compose=yaml.safe_load((leg/'docker-compose.yml').read_text())
for name in ['jobmanager','taskmanager1','taskmanager2']:
    env=compose['services'][name]['environment']
    assert env['CACHEKIT_VALUE_EVICTION_WRITE_BATCH_ENABLED']=='true'
    assert env['FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED']=='true'
    assert all(env[k]=='false' for k in identity['disabled_exploratory_environment'])
cpu=json.loads((leg/'CPU_METRIC_OWNERSHIP_PREFLIGHT.json').read_text())
assert cpu['valid'] and len(cpu['rows'])==2
assert all(x['accounted_receiver_roots']==1 and x['taskmanager_sender_roots']==4 for x in cpu['rows'])
assert not (leg/'NUMA_COLOCATION_VIOLATION').exists()
memory=json.loads((leg/'JVM_MEMORY_GC.json').read_text())
assert memory['valid']
actual=m['variant_files']['runtime_bundle']['artifacts']
assert len(actual)==3 and {x['role'] for x in actual}=={'cachekit_module','flink_dist','table_common'}
assert all(x['sha256']==x['actual_sha256'] for x in actual)
logs='\n'.join(p.read_text(errors='replace') for p in (leg/'container-logs').glob('*.log'))
for marker in ['[CACHEKIT MAP SNAPSHOT VALUE AUTHORITY]','[CACHEKIT MAP POINT VALUE MEMO]',
               '[CACHEKIT JAVA ACCESS GUIDED]','[CACHEKIT JAVA PREFETCH EARLY DROP]',
               '[CACHEKIT OWNED KEY HASH]','[CACHEKIT STICKY L1 EPOCH]',
               'UnsatisfiedLinkError','OutOfMemoryError','NoSuchMethodError','AbstractMethodError']:
    assert marker not in logs,marker
batch=[list(map(int,row)) for row in re.findall(r'\[CACHEKIT EVICTION WRITE BATCH\] enabled=true batches=(\d+) keys=(\d+) singles=(\d+) policy=synchronous-overflow',logs)]
assert len(batch)==logs.count('[CACHEKIT EVICTION WRITE BATCH]')
assert all(k>=2*b and min(k,b,s)>=0 for b,k,s in batch)
copies=re.findall(r'\[FLINK LAZY STRING COPY\] enabled=true jvm=(\S+) thread=(\d+) observedCopies=(\d+) policy=immutable-java-only origin=',logs)
assert len(copies)==logs.count('[FLINK LAZY STRING COPY]')
counts={}
for j,t,n in copies:
    assert int(n)>0
    counts[j+':'+t]=max(counts.get(j+':'+t,0),int(n))
assert not copies or 'flink-table-api-java-uber-1.16.3.jar' in logs
r.update(schema='fullopt-p29-p30-audited-leg-v1',
         batch_keys=sum(row[1] for row in batch),copy_lower_bound=sum(counts.values()),
         effective=sum(row[1] for row in batch)>0 or sum(counts.values())>0,
         activation_scope='lifecycle including warmup; copy counters sampled lower bounds',
         jvm_memory_gc=memory,cpu_metric_ownership=cpu)
(leg/'result.json').write_text(json.dumps(r,indent=2,sort_keys=True)+'\n')
print(json.dumps({'valid':True,'query':r['query'],'batch_keys':r['batch_keys'],'copy_lower_bound':r['copy_lower_bound']}))
