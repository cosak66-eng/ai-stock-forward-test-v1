"""Append-only run/attempt evidence for the V1 acquisition adapter."""
from pathlib import Path
import argparse,json,os,re
import shared_market_data as m

def preserve(root,run_id,attempt,code_sha):
 if not re.fullmatch(r'[A-Za-z0-9_-]+',run_id) or not str(attempt).isdigit():raise ValueError('Invalid run identifier')
 root=Path(root);root.mkdir(parents=True,exist_ok=True);target=root/f'run_evidence_{run_id}_{attempt}.json'
 files=[]
 for p in sorted(root.rglob('*')):
  if p.is_symlink():raise RuntimeError('Do not follow evidence symlinks')
  if p.is_file() and not p.name.startswith('run_evidence_'):
   files.append({'path':p.relative_to(root).as_posix(),'size_bytes':p.stat().st_size,'sha256':m.sha256_file(p)})
 record={'shared_collector_version':m.VERSION,'shared_module_sha256':m.sha256_file(m.__file__),'run_id':run_id,'run_attempt':int(attempt),'code_sha':code_sha,'status':'SOURCE_OUTPUT_PRESERVED' if files else 'NO_SOURCE_OUTPUT','files':files,'official_prediction':False,'orders_sent':0}
 if target.exists():
  if json.loads(target.read_text())!=record:raise RuntimeError('Refuse changed same-attempt evidence')
  return record
 with target.open('x') as f:f.write(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
 return record

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',default='output');p.add_argument('--code-sha',required=True);a=p.parse_args()
 r=preserve(a.root,os.getenv('GITHUB_RUN_ID','local'),os.getenv('GITHUB_RUN_ATTEMPT','1'),a.code_sha);print(json.dumps({'status':r['status'],'file_count':len(r['files'])}))
