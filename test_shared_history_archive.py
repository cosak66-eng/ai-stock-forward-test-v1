import datetime as dt,gzip,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import shared_history_archive as h

PRICE=b'<protocol><chartdata><item data="20230102|10|12|9|11|100"/><item data="20261028|20|22|19|21|200"/></chartdata></protocol>'
def flow(day,**kw):
 r={'localTradedAt':day,'krx':{'closingPrice':'21','tradingVolume':'200','foreignNetVolume':'-10','organizationNetVolume':'0'},'nxt':{'foreignNetVolume':'999'}};r['krx'].update(kw);return r

class HistoryArchiveTests(unittest.TestCase):
 def test_prices_date_range_and_no_money_estimation(self):
  rows,dates=h.parse_prices(PRICE,'2023-01-01','2026-10-28');self.assertEqual(len(rows),2);self.assertIsNone(rows[0]['trading_value']);self.assertEqual(rows[0]['date'],'2023-01-02')
 def test_xml_korean_encoding(self):
  raw=('<?xml version="1.0" encoding="euc-kr"?><protocol name="삼성"><item data="20230102|10|12|9|11|100"/></protocol>').encode('euc-kr')
  self.assertEqual(len(h.parse_prices(raw,'2023-01-01','2026-10-28')[0]),1)
 def test_prices_reject_duplicate_negative_future(self):
  for data in [PRICE.replace(b'20261028',b'20230102'),PRICE.replace(b'|200',b'|-200'),PRICE.replace(b'20261028',b'20990101')]:
   with self.assertRaises(ValueError):h.parse_prices(data,'2023-01-01','2026-10-28')
 def test_historical_end_filters_later_observations(self):
  rows,_=h.parse_prices(PRICE,'2023-01-01','2024-01-01');self.assertEqual(len(rows),1)
 def test_krx_only_signed_zero_missing(self):
  r,_,_=h.parse_flows({'items':[flow('2026-10-28')],'hasNext':False},'005930','2023-01-01','2026-10-28')
  self.assertEqual(r[0]['foreign_net_buy_quantity'],-10);self.assertEqual(r[0]['institution_net_buy_quantity'],0);self.assertIsNone(r[0]['individual_net_buy_quantity'])
 def test_no_nxt_fallback(self):
  x=flow('2026-10-28');x['krx']=None;r,_,_=h.parse_flows({'items':[x],'hasNext':False},'005930','2023-01-01','2026-10-28');self.assertEqual(r,[])
 def test_flow_schema_ticker_date_guards(self):
  for x in [{'items':[]},{'items':[dict(flow('2099-01-01'))],'hasNext':False},{'items':[dict(flow('2026-10-28'),itemCode='000660')],'hasNext':False}]:
   with self.assertRaises(ValueError):h.parse_flows(x,'005930','2023-01-01','2026-10-28')
 def fetch(self,url):
  if 'fchart' in url:return PRICE
  if 'index=0' in url:return json.dumps({'items':[flow('2026-10-28')],'hasNext':True}).encode()
  return json.dumps({'items':[flow('2023-01-02')],'hasNext':False}).encode()
 def test_paginated_history_preserved_and_compressed(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=h.collect_ticker('005930','2023-01-01','2026-10-28',tmp,get=self.fetch)
   self.assertEqual(r['status'],'AVAILABLE_HISTORY_ARCHIVED');self.assertEqual(r['flows_rows'],2);self.assertEqual(r['flow_pages'],2)
   self.assertEqual(gzip.decompress((Path(tmp)/'005930/price.raw.gz').read_bytes()),PRICE)
   with self.assertRaises(ValueError):h.collect_ticker('005930','2023-01-01','2026-10-28',tmp,get=self.fetch)
 def test_repeating_pagination_is_error_not_complete(self):
  def repeated(url):return PRICE if 'fchart' in url else json.dumps({'items':[flow('2026-10-28')],'hasNext':True}).encode()
  with tempfile.TemporaryDirectory() as tmp:
   r=h.collect_ticker('005930','2023-01-01','2026-10-28',tmp,get=repeated);self.assertEqual(r['status'],'PARTIAL_OR_MISSING');self.assertIn('flow',r['errors'])
 def test_page_bound_and_source_error_remain_partial(self):
  with tempfile.TemporaryDirectory() as tmp:
   r=h.collect_ticker('005930','2023-01-01','2026-10-28',tmp,get=self.fetch,max_pages=1);self.assertFalse(r['flow_range_boundary_reached']);self.assertEqual(r['status'],'PARTIAL_OR_MISSING')
 def test_bounds_and_no_operating_outputs(self):
  with tempfile.TemporaryDirectory() as tmp:
   with self.assertRaises(ValueError):h.collect('2022-01-01','2026-10-28',tmp,['005930'])
   with self.assertRaises(ValueError):h.collect('2023-01-01','2026-10-28',tmp,['005930'],workers=10)
   r=h.collect_ticker('005930','2023-01-01','2026-10-28',tmp,get=self.fetch)
   self.assertFalse(r['prediction_eligible']);self.assertEqual(r['orders_sent'],0);self.assertFalse((Path(tmp)/'v2_research_ledger').exists())

if __name__=='__main__':unittest.main()
