"""Read-only host admission; on Kunpeng any foreign experiment means wait."""
import argparse
import json
from pathlib import Path
import subprocess
import time

p=argparse.ArgumentParser()
p.add_argument('--project', required=True)
p.add_argument('--wutb-only', action='store_true')
p.add_argument('--root', type=Path)
a=p.parse_args()
containers=json.loads(subprocess.check_output(['docker','inspect']+subprocess.check_output(['docker','ps','-q'],text=True).split(),text=True)) if subprocess.check_output(['docker','ps','-q'],text=True).strip() else []
foreign=[{'name':c['Name'],'image':c['Config']['Image']} for c in containers if c['Config'].get('Labels',{}).get('com.docker.compose.project')!=a.project]
rows=subprocess.check_output(['ps','-u','wutb','-o','pid=,comm=,pcpu='],text=True).splitlines()
busy=[r for r in rows if r.split()[1].lower() in ['java','db_bench','sysbench','fio','stress-ng','flink','mvn']]
if foreign or busy:
    print(json.dumps({'valid':False,'reason':'WAIT_FOREIGN_EXPERIMENT','foreign':foreign,'wutb_experiment_processes':busy}))
    raise SystemExit(75)
if a.wutb_only:
    raise SystemExit(0)
assert a.root
identity=json.loads((a.root/'identity.json').read_text())
cpus=set()
for value in identity['cpus'].values():
    for token in value.split(','):
        bounds=list(map(int,token.split('-')))
        cpus.update(range(bounds[0],bounds[-1]+1))
def read():
    return {int(l.split()[0][3:]):list(map(int,l.split()[1:])) for l in Path('/proc/stat').read_text().splitlines() if l.startswith('cpu') and l[3:4].isdigit()}
first=read(); time.sleep(2); second=read()
usage={str(c):100*(1-sum(second[c][i]-first[c][i] for i in [3,4])/sum(second[c][i]-first[c][i] for i in range(8))) for c in cpus}
valid=max(usage.values())<10
out={'valid':valid,'foreign':foreign,'wutb_experiment_processes':busy,'cpu_busy_percent':usage,'timestamp':time.time()}
(a.root/'IDLE_PREFLIGHT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))
raise SystemExit(0 if valid else 75)
