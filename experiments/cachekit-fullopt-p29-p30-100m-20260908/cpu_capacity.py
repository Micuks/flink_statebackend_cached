"""Check the two accounted PID-1 trees against cgroup CPU during controlled q4."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args()
identity=json.loads((a.root/'identity.json').read_text());project=identity['project']
def job():
    with urllib.request.urlopen('http://127.0.0.1:12980/jobs/overview',timeout=3) as r: jobs=json.load(r)['jobs']
    active=[j for j in jobs if j['state']=='RUNNING']
    assert len(active)==1 and active[0]['duration']>=60000 and any(j['state']=='FINISHED' for j in jobs)
    return active[0]['jid']
deadline=time.monotonic()+600
while True:
    try: jid=job();break
    except (OSError,AssertionError):
        if time.monotonic()>deadline:raise
        time.sleep(2)
containers=[]
for service in ['taskmanager1','taskmanager2']:
    meta=json.loads(subprocess.check_output(['docker','inspect',project+'_'+service+'_1'],text=True))[0]
    pid=meta['State']['Pid'];cg=Path('/proc/'+str(pid)+'/cgroup').read_text().splitlines()
    unified=next((l[3:] for l in cg if l.startswith('0::')),None)
    if unified:
        directory=Path('/sys/fs/cgroup')/unified.lstrip('/')
        usage=directory/'cpu.stat';version=2
        cpus=(directory/'cpuset.cpus.effective').read_text().strip()
        mems=(directory/'cpuset.mems.effective').read_text().strip()
        quota=(directory/'cpu.max').read_text().strip()
    else:
        path=next(l.split(':',2)[2] for l in cg if 'cpuacct' in l.split(':',2)[1].split(','))
        directory=Path('/sys/fs/cgroup/cpu,cpuacct')/path.lstrip('/')
        if not directory.exists(): directory=Path('/sys/fs/cgroup/cpuacct')/path.lstrip('/')
        usage=directory/'cpuacct.usage';version=1
        cp=next(l.split(':',2)[2] for l in cg if l.split(':',2)[1]=='cpuset')
        cpdir=Path('/sys/fs/cgroup/cpuset')/cp.lstrip('/')
        cpus=(cpdir/'cpuset.cpus').read_text().strip();mems=(cpdir/'cpuset.mems').read_text().strip()
        quota=(directory/'cpu.cfs_quota_us').read_text().strip()
    def expand(spec):
        result=set()
        for token in spec.split(','):
            bounds=list(map(int,token.split('-')));result.update(range(bounds[0],bounds[-1]+1))
        return result
    assert expand(cpus)==expand(identity['cpus'][service])
    assert mems=='0'
    containers.append({'service':service,'pid':pid,'usage':str(usage),'version':version,'cpus':cpus,'mems':mems,'quota':quota})
def sample(c):
    lines=subprocess.check_output(['docker','top',project+'_'+c['service']+'_1','-eo','pid,ppid'],text=True).splitlines()[1:]
    links=[tuple(map(int,l.split())) for l in lines]
    owned={c['pid']}
    while True:
        new=owned|{pid for pid,parent in links if parent in owned}
        if new==owned:break
        owned=new
    ticks=0
    for pid in owned:
        try:
            fields=Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()
            ticks+=int(fields[11])+int(fields[12])
        except FileNotFoundError:pass
    text=Path(c['usage']).read_text()
    usage=int(dict(l.split() for l in text.splitlines())['usage_usec'])/1e6 if c['version']==2 else int(text)/1e9
    return {'tree_seconds':ticks/os.sysconf('SC_CLK_TCK'),'cgroup_seconds':usage,'monotonic':time.monotonic(),'owned_pids':sorted(owned)}
before=[sample(c) for c in containers];time.sleep(35);after=[sample(c) for c in containers]
assert job()==jid,'job boundary during CPU validation'
rows=[]
for c,b,e in zip(containers,before,after):
    dt=e['monotonic']-b['monotonic'];tree=(e['tree_seconds']-b['tree_seconds'])/dt;cg=(e['cgroup_seconds']-b['cgroup_seconds'])/dt
    error=abs(tree/cg-1)
    rows.append({**c,'tree_cores':tree,'cgroup_cores':cg,'relative_error':error,'valid':1<cg<=8.05 and error<.05,'before':b,'after':e})
out={'valid':all(r['valid'] for r in rows),'job_id':jid,'rows':rows,'boundary':'35s controlled steady Nexmark workload; checks the same two recursive PID-1 ownership trees against total cgroup CPU. Not eight disjoint per-TM trees.'}
(a.root/'CPU_CAPACITY_CROSSCHECK.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2));assert out['valid']
