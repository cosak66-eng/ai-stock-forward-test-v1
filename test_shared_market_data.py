"""Identical deterministic shared-acquisition and version-boundary tests."""
from datetime import datetime
import hashlib,json,tempfile,unittest,urllib.error
from pathlib import Path
from unittest.mock import patch
import shared_market_data as m

class SharedTests(unittest.TestCase):
 def row(self,day='2026-10-19',**kwargs):return dict(date=day,open=100.,high=110.,low=90.,close=105.,volume=20.,trading_value=None,**kwargs)
 def audit(self):return {'asof_date':'20261020','capture_started_kst':'2026-10-20T15:30:30+09:00','capture_finished_kst':'2026-10-20T15:39:59+09:00','current_day_trade_type':'KRX','after_market_contamination':False,'same_day_generic_integrated_price_used':False,'trading_value_estimated_close_x_volume':False}
 def test_vendor_pin(self):
  root=Path(m.__file__).parent;expected=json.loads((root/'shared_market_data_manifest.json').read_text())
  self.assertEqual(hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest(),expected['sha256'])
 def test_permanent_404_switches_immediately(self):
  with patch.object(m.urllib.request,'urlopen',side_effect=urllib.error.HTTPError('https://example.com',404,'missing',{},None)) as http,patch.object(m.time,'sleep') as wait:
   with self.assertRaises(RuntimeError):m.request_bytes('https://example.com')
  self.assertEqual(http.call_count,1);wait.assert_not_called()
 def test_429_respects_bounded_retry_after(self):
  exc=urllib.error.HTTPError('https://example.com',429,'rate limit',{'Retry-After':'999'},None)
  with patch.object(m.urllib.request,'urlopen',side_effect=exc) as http,patch.object(m.time,'sleep') as wait,patch.object(m.random,'random',return_value=0):
   with self.assertRaises(RuntimeError):m.request_bytes('https://example.com',tries=2)
  self.assertEqual(http.call_count,2);wait.assert_called_once_with(60.)
 def test_invalid_json_is_not_retried(self):
  with patch.object(m,'request_text',return_value='not JSON') as text:
   with self.assertRaises(ValueError):m.request_json('https://example.com')
  self.assertEqual(text.call_count,1)
 def test_retry_policy_is_bounded(self):
  for kw in [dict(tries=5),dict(timeout=61)]:
   with self.assertRaises(ValueError):m.request_bytes('https://example.com',**kw)
 def test_non_utf8_source(self):self.assertEqual(m.decode_text('거래량'.encode('euc-kr'),'euc-kr'),'거래량')
 def test_xml_fallback_no_trading_value_estimate(self):
  xml='<protocol><item data="20261019|100|110|90|105|20"/></protocol>'
  calls=[]
  def get_json(url):raise RuntimeError('chart unavailable')
  rows=m.fetch_naver_history('005930',1,get_json=get_json,get_text=lambda url:xml,asof='2026-10-20')
  self.assertEqual(rows[0]['price_history_route'],'NAVER_FCHART_XML');self.assertIsNone(rows[0]['trading_value'])
  self.assertEqual(rows[0]['session_verification'],'UNVERIFIED')
 def test_future_source_date_cannot_pass(self):
  payload={'rows':[{'date':'2026-10-21','open':100,'high':110,'low':90,'close':105,'volume':20}]}
  with self.assertRaises(RuntimeError):m.fetch_naver_history('005930',1,get_json=lambda _:payload,get_text=lambda *a,**k:'',asof='2026-10-20')
 def test_missing_amount_stays_unavailable(self):
  out,a=m.v1_prior_history([self.row()],[],'2026-10-20');self.assertIsNone(out[0]['trading_value']);self.assertEqual(a['actual_value_matched_bars'],0)
 def test_actual_amount_is_joined_only_with_identical_ohlcv(self):
  other=self.row();other['trading_value']=2100
  out,a=m.v1_prior_history([self.row()],[other],'2026-10-20')
  self.assertEqual(out[0]['trading_value'],2100);self.assertEqual(a['actual_value_matched_bars'],1)
 def test_any_different_ohlcv_refuses_amount(self):
  for field in ('open','high','low','close','volume'):
   other=self.row();other[field]+=1;other['trading_value']=2100
   out,a=m.v1_prior_history([self.row()],[other],'2026-10-20')
   self.assertIsNone(out[0]['trading_value']);self.assertEqual(a['value_mismatch_bars'],1)
 def test_rounded_or_nan_amount_is_not_an_actual_numeric_field(self):
  for value in ('2,100',float('nan'),True):
   other=self.row();other['trading_value']=value
   out,a=m.v1_prior_history([self.row()],[other],'2026-10-20');self.assertIsNone(out[0]['trading_value'])
 def test_native_and_independent_amount_disagreement_is_not_silently_replaced(self):
  primary=self.row();primary['trading_value']=2000;other=self.row();other['trading_value']=2100
  out,a=m.v1_prior_history([primary],[other],'2026-10-20');self.assertIsNone(out[0]['trading_value'])
 def test_current_and_future_dates_are_not_used_as_v1_prior_bars(self):
  rows=[self.row(day=x) for x in ['2026-10-19','2026-10-20','2026-10-21']]
  out,_=m.v1_prior_history(rows,[],'2026-10-20');self.assertEqual([r['date'] for r in out],['20261019'])
 def test_duplicate_date_cannot_conceal_conflicting_values(self):
  with self.assertRaisesRegex(RuntimeError,'Duplicate'):m.v1_prior_history([self.row()]*2,[],'2026-10-20')
  with self.assertRaisesRegex(RuntimeError,'Duplicate'):m.v1_prior_history([self.row()],[self.row()]*2,'2026-10-20')
 def test_valid_capture_still_does_not_authorize_other_v1_gates(self):
  r=m.check_v1_capture(self.audit());self.assertEqual(r['status'],'PASS_CAPTURE_CONTRACT');self.assertFalse(r['official_prediction_allowed'])
 def test_v2_post_close_audit_cannot_authorize_v1(self):
  a={'source':'NAVER','mode':'full','started_at_kst':'2026-10-20T20:20:00+09:00','finished_at_kst':'2026-10-20T20:40:00+09:00','research_collection_eligible':True}
  self.assertEqual(m.check_v1_capture(a)['status'],'BLOCKED_V1_CAPTURE_CONTRACT')
 def test_late_capture_stays_blocked(self):
  for key,value in [('capture_started_kst','2026-10-20T15:46:00+09:00'),('capture_finished_kst','2026-10-20T15:40:00+09:00'),('current_day_trade_type','NXT'),('after_market_contamination',True)]:
   a=self.audit();a[key]=value;self.assertEqual(m.check_v1_capture(a)['status'],'BLOCKED_V1_CAPTURE_CONTRACT')
 def test_timezone_and_same_day_are_required(self):
  for value in ['2026-10-20T15:30:30','2026-10-19T15:30:30+09:00']:
   a=self.audit();a['capture_started_kst']=value;self.assertEqual(m.check_v1_capture(a)['status'],'BLOCKED_V1_CAPTURE_CONTRACT')

 def test_common_universe_stops_when_a_page_repeats(self):
  data={'rows':[{'itemCode':'005930','stockName':'삼성전자','marketType':'KOSPI'}]}
  with patch.object(m.time,'sleep'):
   calls=[]
   def get(url):calls.append(url);return data
   rows=m.fetch_naver_universe(get_json=get)
  self.assertEqual(len(rows),1);self.assertEqual(len(calls),2);self.assertIn('startIdx=1',calls[1])
 def test_v1_universe_reuses_frozen_metadata_only(self):
  capture={'005930':{'market':'0','company':'삼성전자','close':999}}
  r=m.v1_universe_from_capture(capture);self.assertEqual(r[0]['market'],'KOSPI');self.assertNotIn('close',r[0])

if __name__=='__main__':unittest.main()
