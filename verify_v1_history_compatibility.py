"""Verify unchanged V1 model inputs against the pinned collector on saved raw bytes."""
from pathlib import Path
import argparse,importlib.util,json
from unittest.mock import patch
import shared_market_data as shared

def verify(baseline,raw_dir):
 spec=importlib.util.spec_from_file_location('v1_frozen_source_baseline',baseline);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
 root=Path(raw_dir);report=json.loads((root/'diagnostic_audit.json').read_text());asof=report['observed_at_kst'][:10].replace('-','');records=[]
 for p in sorted(root.glob('*_daum.raw')):
  ticker=p.name.split('_')[0];payload=json.loads(p.read_text())
  with patch.object(old,'http_json',return_value=payload):prior=old.daum_daily_prior(ticker,asof,140)
  current=shared.fetch_daum_prior(ticker,asof,140,get_json=lambda u,h:payload)
  profile,_=shared.v1_profile_history([],current,report['observed_at_kst'][:10])
  if prior!=current or profile!=prior:raise RuntimeError('V1 native model inputs changed: '+ticker)
  records.append({'ticker':ticker,'dated_rows':len(prior),'exact_original_model_inputs':True})
 if len(records)!=2:raise RuntimeError('Expected both actual diagnostic ticker responses')
 result={'status':'PASS','records':records,'official_prediction':False,'orders_sent':0,'note':'Source input parity only; other V1 quality and venue gates still required.'}
 (root/'v1_legacy_input_parity.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));return result

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--baseline',required=True);p.add_argument('--raw-dir',required=True);a=p.parse_args();verify(a.baseline,a.raw_dir)
