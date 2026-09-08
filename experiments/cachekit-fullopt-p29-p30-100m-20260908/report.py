"""User-requested column order, historical controls, native Feishu tables."""
import argparse
import json
from pathlib import Path
import statistics
import urllib.request

COLUMNS=[('query','Query/Group'),('rocksdb','RocksDB'),('fullopt','FullOpt'),('optimized','FullOpt+P29+P30'),
         ('fullopt_vs_rocksdb','FullOpt uplift vs RocksDB'),('optimized_vs_rocksdb','组合 uplift vs RocksDB'),
         ('optimized_vs_fullopt','组合 uplift vs FullOpt')]
def table(columns,rows):
    return {'tag':'table','page_size':10,'freeze_first_column':True,'row_height':'low',
            'header_style':{'text_align':'center','background_style':'grey','bold':True,'lines':1},
            'columns':[{'name':k,'display_name':label,'data_type':'text','width':'150px'} for k,label in columns],'rows':rows}
def render(s):
    def fmt(k,v):return '—' if v is None else str(v) if k=='query' else f'{v:+.2f}%' if '_vs_' in k else f'{v:.2f}'
    main=[{k:fmt(k,r[k]) for k,_ in COLUMNS} for r in s['results']]
    for label,members in [('大状态 8Q',s['results'][:8]),('小状态 7Q',s['results'][8:]),('15Q',s['results'])]:
        valid=[r for r in members if r['optimized'] is not None]
        row={'query':label+(f' partial {len(valid)}/{len(members)}' if len(valid)!=len(members) else '')}
        for k,_ in COLUMNS[1:]:
            selected=valid if valid else members
            vals=[r[k] for r in selected if r[k] is not None]
            row[k]=fmt(k,statistics.mean(vals) if vals else None)
        main.append(row)
    refs=s['references'];full={r['query']:r for r in refs['fullopt']};rdb={r['query']:r for r in refs['rocksdb']['audited_raw_rows']}
    raw=[];activity=[]
    for row in s['results']:
        q=row['query'];leg=row['leg'];m=leg['measurement'] if leg else {}
        raw.append({'query':q,'rk':fmt('raw',rdb[q]['raw_kps']),'rc':fmt('cores',rdb[q]['cores']),
                    'fk':fmt('raw',full[q]['raw_kps']),'fc':fmt('cores',full[q]['cores']),
                    'ok':fmt('raw',m.get('raw_throughput_kps')),'oc':fmt('cores',m.get('cores'))})
        memory=leg['jvm_memory_gc']['queries'] if leg else {}
        def total(k,scale):
            return fmt(k,sum(float(v['value'][1]) for v in memory[k]['response']['data']['result'])/scale) if memory else '—'
        activity.append({'query':q,'batch':str(leg['batch_keys']) if leg else '—','copy':str(leg['copy_lower_bound']) if leg else '—',
                         'effective':str(leg['effective']) if leg else '—',
                         'heap':total('heap_peak_per_tm_bytes_lifecycle',1024**2),'gc':total('young_gc_time_ms_lifecycle',1)})
    identity=s['identity'];title=identity['platform']+' FullOpt+P29+P30 '+('RESULT' if s['complete'] else 'PARTIAL')
    intro=f"100M/no periodic checkpoint；8 TM/16 slots；NUMA0，K/s/core。仅新跑组合 {s['valid_legs']}/15，RocksDB与FullOpt均复用历史R1。"
    if s['effective_mean_vs_fullopt'] is not None:
        intro+=f" 当前生效{len(s['effective_queries'])}Q，对FullOpt等权均值{s['effective_mean_vs_fullopt']:+.2f}%；全部完整新query均值{s['all_query_mean_vs_fullopt']:+.2f}%。"
    if s.get('monitoring_status'):
        intro+='\n状态：'+s['monitoring_status']
    elements=[{'tag':'markdown','content':intro},
              {'tag':'markdown','content':'复用边界：不是同期配对，也不是纯P29/P30因果消融。新组合沿用FullOpt配置/NUMA/其余运行包，仅覆盖记录的当前源码类；RocksDB是同主机历史参考，NUMA/产物差异保留。组提升为查询百分比等权均值，保留负值；partial组的三组数值使用已有新结果的共同query集合。'},
              table(COLUMNS,main),
              table([('query','Query'),('rk','RDB raw K/s'),('rc','RDB cores'),('fk','FullOpt raw K/s'),('fc','FullOpt cores'),('ok','组合 raw K/s'),('oc','组合 cores')],raw),
              table([('query','Query'),('batch','P29 batch keys'),('copy','P30 copy lower bound'),('effective','Effective'),('heap','sum TM heap peaks MiB'),('gc','sum young GC ms')],activity),
              {'tag':'markdown','content':'生效计数包含warmup，P30为按JVM/thread累计最大值的采样下界。heap是各TM生命周期峰值之和，不是同时峰值；GC和不是job暂停。CPU按两棵不重叠容器PID1树统计，8 TM身份检查只表示覆盖；另有35s cgroup交叉验证。'},
              {'tag':'markdown','content':'Source: '+identity['source_commit']+'\nNew: '+identity['campaign']+'\nFullOpt: '+identity['reference_campaign']+'\nRocksDB: '+refs['rocksdb']['source_campaign']+'\nConfig SHA: '+identity['reused_fullopt_config_sha256']}]
    return {'msg_type':'interactive','card':{'config':{'wide_screen_mode':True},'header':{'title':{'tag':'plain_text','content':title},'template':'green' if s['complete'] else 'blue'},'elements':elements}}
def markdown(payload):
    lines=['# '+payload['card']['header']['title']['content'],'']
    for e in payload['card']['elements']:
        if e['tag']=='markdown':lines.extend([e['content'],''])
        if e['tag']=='table':
            cols=e['columns'];lines.extend(['| '+' | '.join(c['display_name'] for c in cols)+' |','|'+'|'.join('---' for c in cols)+'|'])
            lines.extend('| '+' | '.join(row[c['name']] for c in cols)+' |' for row in e['rows']);lines.append('')
    return '\n'.join(lines).rstrip()+'\n'
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('input',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--send',action='store_true');p.add_argument('--status');a=p.parse_args()
    data=json.loads(a.input.read_text())
    if a.status:data['monitoring_status']=a.status
    payload=render(data);a.output.write_text(markdown(payload))
    status={'native_tables':3,'delivery':'not_requested'}
    if a.send:
        path=Path('/mnt/data1/wuql/.codex/feishu.json');cfg=json.loads(path.read_text()) if path.exists() else {}
        if cfg.get('mode','off')!='off' and cfg.get('webhook_url'):
            try:
                request=urllib.request.Request(cfg['webhook_url'],data=json.dumps(payload,ensure_ascii=False).encode(),headers={'Content-Type':'application/json'})
                with urllib.request.urlopen(request,timeout=20) as r:reply=json.load(r)
                status.update(delivery='acknowledged' if reply.get('code',reply.get('StatusCode'))==0 else 'rejected',code=reply.get('code',reply.get('StatusCode')))
            except Exception as e:status.update(delivery='failed',error_type=type(e).__name__)
    print(json.dumps(status))
