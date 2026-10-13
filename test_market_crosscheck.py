import tempfile,unittest
from pathlib import Path
from market_crosscheck import make_row,compare_primary,optional_csv,code,integer
class Tests(unittest.TestCase):
 def setUp(self):
  self.a=make_row('005930','20261013','NAVER',70000,1000,70000000)
  self.b=dict(self.a,source='DAUM')
 def test_match(self):self.assertEqual(compare_primary(self.a,self.b)['status'],'PRIMARY_MATCH')
 def test_missing(self):self.assertEqual(compare_primary(None,self.b)['status'],'PRIMARY_MISSING')
 def test_close_mismatch(self):self.assertEqual(compare_primary(self.a,dict(self.b,close=70001))['status'],'PRIMARY_MISMATCH')
 def test_volume_mismatch(self):self.assertEqual(compare_primary(self.a,dict(self.b,volume=999))['fields']['volume'],'MISMATCH')
 def test_value_mismatch(self):self.assertEqual(compare_primary(self.a,dict(self.b,trading_value=1))['fields']['trading_value'],'MISMATCH')
 def test_missing_value(self):self.assertEqual(compare_primary(self.a,dict(self.b,trading_value=None))['status'],'PRIMARY_INCOMPLETE')
 def test_date_mismatch(self):self.assertEqual(compare_primary(self.a,dict(self.b,date='20261012'))['status'],'PRIMARY_DATE_MISMATCH')
 def test_bad_ticker(self):self.assertIsNone(make_row('bad','20261013','NAVER',70000))
 def test_missing_close(self):self.assertIsNone(make_row('005930','20261013','NAVER',None))
 def test_negative_volume(self):self.assertIsNone(make_row('005930','20261013','NAVER',100,-1))
 def test_no_estimation(self):self.assertIsNone(make_row('005930','20261013','NAVER',None,100,1000))
 def test_no_float_rounding(self):self.assertIsNone(integer('123.4'))
 def test_code_prefix(self):self.assertEqual(code('A005930'),'005930')
 def test_optional_missing(self):
  with tempfile.TemporaryDirectory() as d:
   data,w=optional_csv(Path(d)/'absent.csv','20261013');self.assertEqual(data,{});self.assertIn('OPTIONAL_CSV_ABSENT',w)
 def test_optional_valid(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'e.csv';p.write_text('source,ticker,date,close\nFNGUIDE,005930,20261013,70000\n')
   data,w=optional_csv(p,'20261013');self.assertEqual(data[('FNGUIDE','005930')]['close'],70000)
 def test_optional_bad_csv(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'e.csv';p.write_bytes(b'\xff')
   data,w=optional_csv(p,'20261013');self.assertEqual(data,{});self.assertTrue(w)
if __name__=='__main__':unittest.main()
