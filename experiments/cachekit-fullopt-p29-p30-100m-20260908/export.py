"""Read-only raw audit, then emit requested historical-baseline comparison order."""
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys

root=Path(sys.argv[1]); identity=json.loads((root/'identity.json').read_text())
refs=json.loads((root/'REUSED_BASELINES.json').read_text())
rows={};pending=[]
for leg in sorted((root/'results/raw').glob('*')):
    if not (leg/'LEG_COMPLETE').exists(): pending.append(leg.name);continue
    subprocess.run(['sha256sum','--status','-c','LEG.sha256'],cwd=leg,check=True)
    r=json.loads((leg/'result.json').read_text())
    assert r['schema']=='fullopt-p29-p30-audited-leg-v1' and r['valid'] and r['source_commit']==identity['source_commit']
    assert r['query'] not in rows and r['round']==1 and r['variant']=='p29p30'
    rows[r['query']]=r
full={r['query']:r for r in refs['fullopt']}; results=[]
for q in identity['queries']:
    f=full[q]['kps_core'];b=refs['rocksdb']['throughput_kps_core'][q]
    leg=rows.get(q);o=leg['measurement']['throughput_kps_core'] if leg else None
    results.append({'query':q,'rocksdb':b,'fullopt':f,'optimized':o,'fullopt_vs_rocksdb':100*(f/b-1),
        'optimized_vs_rocksdb':100*(o/b-1) if o else None,'optimized_vs_fullopt':100*(o/f-1) if o else None,
        'effective':leg['effective'] if leg else None,'leg':leg})
valid=[r for r in results if r['optimized'] is not None]
effective=[r for r in valid if r['effective']]
capacity=json.loads((root/'CPU_CAPACITY_CROSSCHECK.json').read_text()) if (root/'CPU_CAPACITY_CROSSCHECK.json').exists() else None
if valid: assert capacity and capacity['valid'], 'CPU calibration required before per-core publication'
out={'identity':identity,'references':refs,'valid_legs':len(valid),'expected_legs':15,'complete':len(valid)==15 and not pending,
     'pending':pending,'results':results,'effective_queries':[r['query'] for r in effective],
     'all_query_mean_vs_fullopt':statistics.mean(r['optimized_vs_fullopt'] for r in valid) if valid else None,
     'effective_mean_vs_fullopt':statistics.mean(r['optimized_vs_fullopt'] for r in effective) if effective else None,
     'cpu_capacity_crosscheck':capacity}
(root/'final').mkdir(exist_ok=True)
(root/'final/RESULTS.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k not in ['identity','references','results']},indent=2))
