import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import collector as c
import market_crosscheck as m

class ResponseTests(unittest.TestCase):
 def setUp(self):
  self.raw=json.loads(Path('fixtures/naver_market_20261028.json').read_text())
  self.daily=json.loads(Path('fixtures/naver_daily_20261028.json').read_text())
 def regular(self):
  r=copy.deepcopy(self.raw);r['tradingSessionType']='REGULAR_MARKET';return r
 def test_current_fields_and_existing_fundamentals(self):
  r=c.parse_current_record(self.regular());self.assertEqual((r['close'],r['volume'],r['trading_value']),(245000,17720841,4240229000000));self.assertEqual(r['foreign_hold_ratio'],46.15);self.assertEqual(r['roe'],10.85)
 def test_explicit_after_hours_is_blocked(self):self.assertIsNone(c.parse_current_record(self.raw))
 def test_nested_nxt_cannot_fill_missing(self):
  r=self.regular();r.pop('nowPrice');r.pop('tradeAmount');r['nxt']={'nowPrice':'999','tradeAmount':'123'};p=c.parse_current_record(r);self.assertIsNone(p['close']);self.assertIsNone(p['trading_value'])
 def test_zero_signed_and_units(self):
  r=self.regular();r.update(nowPrice='0',tradeVolume='0',tradeAmount='0',foreignPureBuy='-10');p=c.parse_current_record(r);self.assertEqual((p['close'],p['volume'],p['trading_value'],p['foreign_net_buy']),(0,0,0,-10));r['tradeAmount']='4조 2402억';self.assertIsNone(c.parse_current_record(r)['trading_value'])
 def test_nonfinite_bool(self):
  for x in [True,False,'NaN','inf','-inf',None]:self.assertIsNone(c.nnum(x))
 def test_legacy_fields(self):
  r=c.parse_current_record({'itemCode':'005930','closePrice':'1000','accTradeVolume':'10','accTradePrice':'12000'});self.assertEqual((r['close'],r['volume'],r['trading_value']),(1000,10,12000))
 def test_dated_price_basis_and_exact_money(self):
  with patch.object(m,'get_json',return_value=self.daily):rows,meta=m.naver_dated_samples('20261028',sample=('005930',))
  self.assertEqual(rows['005930']['close'],245000);self.assertEqual(rows['005930']['trading_value'],4240229000000);self.assertFalse(meta['regular_session_proven'])
 def test_missing_date_never_inferred(self):
  self.daily['items'][0].pop('tradingDateKst')
  with patch.object(m,'get_json',return_value=self.daily):rows,_=m.naver_dated_samples('20261028',sample=('005930',))
  self.assertEqual(rows,{})
 def test_missing_value_never_estimated(self):
  self.daily['items'][0].pop('tradingValue')
  with patch.object(m,'get_json',return_value=self.daily):rows,_=m.naver_dated_samples('20261028',sample=('005930',))
  self.assertIsNone(rows['005930']['trading_value'])
 def test_wrong_schema_preserves_raw(self):
  with tempfile.TemporaryDirectory() as tmp,patch.object(m,'get_json',return_value={'other':[]}):
   rows,meta=m.naver_dated_samples('20261028',evidence_dir=tmp,sample=('005930',));self.assertEqual(rows,{});self.assertIn('005930',meta['errors']);self.assertTrue((Path(tmp)/'005930_naver_daily.json').exists())
 def test_market_aggregate_not_stock_flow(self):
  with tempfile.TemporaryDirectory() as tmp,patch.object(c,'OUT',Path(tmp)),patch.object(c,'http_json',return_value={'content':[{'bizdate':'20261028','netAmounts':[{'investorGubun':'1000','diffValue':'123'}]}]}):self.assertEqual(c.naver_investor_flow_map('20261028'),{})
