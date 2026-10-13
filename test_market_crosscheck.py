import unittest
from market_crosscheck import normalize, compare

class TestCrosscheck(unittest.TestCase):
    def setUp(self):
        self.a=normalize('005930',70000,1000000,70000000000,'KRX','20261013')
        self.b=dict(self.a,source='DAUM')
    def test_match(self): self.assertEqual(compare(self.a,self.b)['status'],'MATCH_3_OF_3')
    def test_volume_mismatch(self):
        b=dict(self.b,volume=999999)
        self.assertEqual(compare(self.a,b)['fields']['volume'],'MISMATCH')
    def test_value_mismatch(self):
        b=dict(self.b,trading_value=69999999999)
        self.assertEqual(compare(self.a,b)['status'],'VALUE_MISMATCH')
    def test_missing(self): self.assertEqual(compare(self.a,None)['status'],'MISSING_SOURCE')
    def test_date_mismatch(self): self.assertEqual(compare(self.a,dict(self.b,date='20261012'))['status'],'DATE_OR_TICKER_MISMATCH')
    def test_no_estimation(self): self.assertIsNone(normalize('005930',70000,1000000,None,'KRX','20261013'))
    def test_no_invalid_ticker(self): self.assertIsNone(normalize('A005930',70000,1000000,70000000000,'KRX','20261013'))
    def test_no_negative_volume(self): self.assertIsNone(normalize('005930',70000,-1,70000000000,'KRX','20261013'))
if __name__=='__main__':unittest.main()
