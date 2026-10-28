from datetime import datetime
import ast,hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import collector as c
from shared_run_manifest import preserve

class V1IntegrationTests(unittest.TestCase):
 def test_frozen_factors_thresholds_and_acceptance_predicate_are_unchanged(self):
  expected=json.loads(Path('frozen_v1_collection_contract.json').read_text());tree=ast.parse(Path(c.__file__).read_text())
  nodes={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
  for name,sha in expected['functions'].items():self.assertEqual(hashlib.sha256(ast.dump(nodes[name],include_attributes=False).encode()).hexdigest(),sha,name)
  for name,value in expected['constants'].items():self.assertEqual(getattr(c,name),value,name)
  gate=next(n for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='pass_all' for t in n.targets))
  self.assertEqual(hashlib.sha256(ast.dump(gate,include_attributes=False).encode()).hexdigest(),expected['pass_all_ast_sha256'])
 def test_missing_v1_native_history_cannot_be_replaced_with_v2_bars(self):
  p={'date':'2026-10-19','open':100.,'high':110.,'low':90.,'close':105.,'volume':20.,'trading_value':None,'price_history_route':'NAVER_FCHART_XML'}
  with patch.object(c.shared,'fetch_naver_history',return_value=[p,dict(p,date='2026-10-20')]),patch.object(c,'daum_daily_through_asof',side_effect=RuntimeError('value source unavailable')):
   t,rows,err=c.fetch_one_history({'ticker':'005930'},'20261020')
  self.assertIsNotNone(err);self.assertEqual(rows,[])
 def test_late_start_performs_no_market_fetch(self):
  late=datetime.fromisoformat('2026-10-20T22:01:00+09:00')
  with tempfile.TemporaryDirectory() as tmp,patch.object(c,'OUT',Path(tmp)),patch.object(c,'valid_prior_run',return_value=False),patch.object(c,'now_kst',return_value=late),patch.object(c,'index_history') as fetch:
   rc=c.run_scheduled('20261020');self.assertNotEqual(rc,0);fetch.assert_not_called()
   self.assertEqual(json.loads((Path(tmp)/'latest_status.json').read_text())['error'],'UNSAFE_CAPTURE_TIME')
 def test_early_runner_waits_for_2100_kst_window(self):
  early=datetime.fromisoformat('2026-10-20T20:58:00+09:00');safe=datetime.fromisoformat('2026-10-20T21:00:00+09:00')
  with tempfile.TemporaryDirectory() as tmp,patch.object(c,'OUT',Path(tmp)),patch.object(c,'valid_prior_run',return_value=False),patch.object(c,'now_kst',side_effect=[early,safe,safe]),patch.object(c.time,'sleep') as wait,patch.object(c,'index_history',side_effect=RuntimeError('fixture stops source access')) as fetch:
   self.assertNotEqual(c.run_scheduled('20261020'),0);wait.assert_called_once_with(120.0);fetch.assert_called_once()
 def test_prior_success_is_preserved_across_collector_revisions(self):
  with tempfile.TemporaryDirectory() as tmp,patch.object(c,'DATA',Path(tmp)):
   p=Path(tmp)/'pipeline_audit_20261020.json';p.write_text(json.dumps({'pass':True,'asof_date':'20261020','collector_revision':'FREEWEB-1.2.4-CONTROLLED'}))
   self.assertTrue(c.valid_prior_run('20261020'))

class AttemptEvidenceTests(unittest.TestCase):
 def test_retries_keep_prior_manifest_and_hashes(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'raw.json';p.write_text('{}');first=preserve(tmp,'123','1','sha')
   old=(Path(tmp)/'run_evidence_123_1.json').read_bytes();p.write_text('{"attempt":2}')
   second=preserve(tmp,'123','2','sha');self.assertNotEqual(first['files'],second['files']);self.assertEqual((Path(tmp)/'run_evidence_123_1.json').read_bytes(),old)
 def test_same_attempt_cannot_overwrite_changed_source(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'raw';p.write_text('before');preserve(tmp,'123','1','sha');p.write_text('after')
   with self.assertRaisesRegex(RuntimeError,'Refuse'):preserve(tmp,'123','1','sha')
 def test_empty_output_is_explicit_instead_of_source_success(self):
  with tempfile.TemporaryDirectory() as tmp:self.assertEqual(preserve(tmp,'123','1','sha')['status'],'NO_SOURCE_OUTPUT')

if __name__=='__main__':unittest.main()
