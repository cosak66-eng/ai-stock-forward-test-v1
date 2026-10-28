"""Observed NAVER history for V1/V2 research. No models, locks or order APIs.

Current listed universe only: not a point-in-time universe, not a backtest.
An archive is isolated from all operating input/ledger paths. Missing data is
reported; later observations never become historical prediction records.
"""
from __future__ import annotations
import argparse, concurrent.futures, datetime as dt, gzip, hashlib, json, math
import os, re, time, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from pathlib import Path
from zoneinfo import ZoneInfo

VERSION='COMMON-HISTORY-ARCHIVE-0.1.0'
TZ=ZoneInfo('Asia/Seoul')
HEADERS={'User-Agent':'Mozilla/5.0','Referer':'https://stock.naver.com/','Accept':'application/json, text/plain, */*'}
FLOW='https://stock.naver.com/api/stockSecurity/investor-trend/v2/domestic/{ticker}/paged'
PRICE='https://fchart.stock.naver.com/sise.nhn'
MAX_RESPONSE=8_000_000

def day(v):
 s=str(v or '')[:10].replace('-','')
 if not re.fullmatch(r'\d{8}',s): raise ValueError('Missing/invalid source date')
 return dt.datetime.strptime(s,'%Y%m%d').date().isoformat()

def number(v):
 if isinstance(v,(bool,dict,list)) or v is None:return None
 try:
  n=float(str(v).replace(',',''));return n if math.isfinite(n) else None
 except (ValueError,TypeError):return None

def iso(v):return dt.date.fromisoformat(v)
def code(t):
 if not re.fullmatch(r'[0-9A-Z]{6}',str(t)):raise ValueError('Invalid ticker')
 return t

def get_bytes(url):
 last=None
 for n in range(2):
  try:
   with urllib.request.urlopen(urllib.request.Request(url,headers=HEADERS),timeout=20) as r:
    b=r.read(MAX_RESPONSE+1)
    if len(b)>MAX_RESPONSE:raise ValueError('Source response exceeded bound')
    return b
  except Exception as e:
   last=e
   if getattr(e,'code',None) in (400,401,403,404):break
   if n==0:time.sleep(2)
 raise RuntimeError(type(last).__name__+': '+str(last)[:300])

def decode(b):
 for enc in ('utf-8-sig','euc-kr','cp949'):
  try:return b.decode(enc)
  except UnicodeError:pass
 raise ValueError('Undecodable source response')

def write_json(p,obj):p.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
def write_gzip(p,b):
 with p.open('xb') as raw:
  with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as f:f.write(b)

def parse_prices(raw,start,end):
 root=ET.fromstring(decode(raw));rows={};source_dates=[]
 for el in root.iter('item'):
  a=str(el.get('data') or '').split('|')
  if len(a)<6:raise ValueError('Invalid fchart row')
  d=day(a[0]);source_dates.append(d)
  if d>dt.datetime.now(TZ).date().isoformat():raise ValueError('Future price date')
  if not start<=d<=end:continue
  vals=[number(x) for x in a[1:6]]
  if any(x is None or x<0 for x in vals):raise ValueError('Invalid OHLCV value')
  op,hi,lo,cl,vol=vals
  if cl>0 and hi>0 and lo>hi:raise ValueError('Impossible price range')
  row={'date':d,'open':op,'high':hi,'low':lo,'close':cl,'volume':vol,'trading_value':None}
  if d in rows:raise ValueError('Duplicate price date')
  rows[d]=row
 return sorted(rows.values(),key=lambda r:r['date']),sorted(source_dates)

def parse_flows(payload,ticker,start,end):
 if not isinstance(payload,dict) or not isinstance(payload.get('items'),list):raise ValueError('Invalid flow items')
 if not isinstance(payload.get('hasNext'),bool):raise ValueError('Missing flow pagination status')
 rows=[];source_dates=[]
 for item in payload['items']:
  if not isinstance(item,dict):raise ValueError('Invalid flow item')
  d=day(item.get('localTradedAt'));source_dates.append(d)
  if d>dt.datetime.now(TZ).date().isoformat():raise ValueError('Future flow date')
  if item.get('itemCode') is not None and item['itemCode']!=ticker:raise ValueError('Ticker mismatch')
  if not start<=d<=end:continue
  krx=item.get('krx')
  if not isinstance(krx,dict):continue
  volume=number(krx.get('tradingVolume'));close=number(krx.get('closingPrice'))
  if volume is not None and volume<0 or close is not None and close<0:raise ValueError('Negative flow price/volume')
  rows.append({'date':d,'close':close,'volume':volume,
    'foreign_net_buy_quantity':number(krx.get('foreignNetVolume')),
    'institution_net_buy_quantity':number(krx.get('organizationNetVolume')),
    'individual_net_buy_quantity':number(krx.get('individualNetVolume')),
    'foreign_holding_quantity':number(item.get('foreignHoldingStockQuantity')),
    'foreign_holding_ratio':number(item.get('foreignHoldingRatio')),
    'venue':'KRX','quantity_unit':'shares_SOURCE_FIELD','unit_independently_verified':False})
 return rows,source_dates,payload['hasNext']

def raw_request(root,label,url,get=get_bytes):
 b=get(url);p=root/(label+'.raw.gz');write_gzip(p,b)
 record={'url':url,'observed_at_kst':dt.datetime.now(TZ).isoformat(),
  'raw_file':p.name,'uncompressed_bytes':len(b),'uncompressed_sha256':hashlib.sha256(b).hexdigest()}
 write_json(root/(label+'.request.json'),record)
 return b

def collect_ticker(ticker,start,end,root,*,get=get_bytes,max_pages=32):
 code(ticker);iso(start);iso(end);root=Path(root)/ticker;root.mkdir(parents=True,exist_ok=True)
 if (root/'audit.json').exists():raise ValueError('Refuse to overwrite ticker archive')
 result={'ticker':ticker,'start_date':start,'end_date':end,'version':VERSION,
  'research_only':True,'prediction_eligible':False,'official_prediction':False,'orders_sent':0,
  'session_verification':'UNVERIFIED','adjustment_verification':'UNVERIFIED',
  'exchange_calendar_verification':'UNVERIFIED','errors':{}}
 prices=[];flows=[]
 try:
  count=min(3000,max(160,(dt.datetime.now(TZ).date()-iso(start)).days+120))
  url=PRICE+'?'+urllib.parse.urlencode({'symbol':ticker,'timeframe':'day','count':count,'requestType':0})
  raw=raw_request(root,'price',url,get);prices,source_dates=parse_prices(raw,start,end)
  result['price_range_boundary_reached']=bool(source_dates and (source_dates[0]<=start or len(source_dates)<count))
  result['price_source_exhausted']=len(source_dates)<count
  result['price_source_min_date']=min(source_dates,default=None)
  if not source_dates:result['errors']['price']='DATA NOT AVAILABLE: empty source history'
  elif not result['price_range_boundary_reached']:result['errors']['price']='TRUNCATED_PRICE_HISTORY'
 except Exception as e:result['errors']['price']=type(e).__name__+': '+str(e)[:400]
 try:
  seen=set();previous_min=None;boundary=False
  for page in range(max_pages):
   url=FLOW.format(ticker=ticker)+'?'+urllib.parse.urlencode({'exchangeType':'krx','size':200,'index':page})
   raw=raw_request(root,f'flow_{page:03d}',url,get);payload=json.loads(raw)
   page_rows,dates,has_next=parse_flows(payload,ticker,start,end)
   for row in page_rows:
    if row['date'] in seen:raise ValueError('Duplicate flow date across pages')
    seen.add(row['date']);flows.append(row)
   if not dates:
    if has_next:raise ValueError('Empty flow page with hasNext')
    boundary=True;result['flow_source_exhausted']=True;break
   minimum=min(dates)
   if previous_min is not None and minimum>=previous_min:raise ValueError('Flow pagination made no date progress')
   previous_min=minimum
   if minimum<=start or not has_next:
    boundary=True;result['flow_source_exhausted']=not has_next;break
   time.sleep(.10)
  result['flow_range_boundary_reached']=boundary;result['flow_pages']=page+1
  if not boundary:result['errors']['flow']='TRUNCATED_FLOW_HISTORY: page bound reached'
  if not flows:result['errors']['flow']='DATA NOT AVAILABLE: no KRX rows in requested range'
 except Exception as e:result['errors']['flow']=type(e).__name__+': '+str(e)[:400]
 for name,rows in [('prices',prices),('flows',sorted(flows,key=lambda r:r['date']))]:
  write_gzip(root/(name+'.jsonl.gz'),b''.join((json.dumps(r,ensure_ascii=False,separators=(',',':'))+'\n').encode() for r in rows))
  result[name+'_rows']=len(rows);result[name+'_first_date']=min((r['date'] for r in rows),default=None);result[name+'_last_date']=max((r['date'] for r in rows),default=None)
 price_map={r['date']:r for r in prices};common=[r for r in flows if r['date'] in price_map]
 result['price_flow_common_dates']=len(common)
 result['price_flow_close_mismatches']=sum(r['close'] is not None and r['close']!=price_map[r['date']]['close'] for r in common)
 result['flow_complete_rows']=sum(all(r[k] is not None for k in ('foreign_net_buy_quantity','institution_net_buy_quantity','volume')) for r in flows)
 result['status']='AVAILABLE_HISTORY_ARCHIVED' if not result['errors'] else 'PARTIAL_OR_MISSING'
 write_json(root/'audit.json',result);return result

def collect(start,end,out_dir,tickers=None,*,workers=2,shard_index=0,shard_count=1):
 if not '2023-01-01'<=start<=end<=dt.datetime.now(TZ).date().isoformat():raise ValueError('Invalid prospective archive date range')
 if not 1<=workers<=4 or not 0<=shard_index<shard_count<=8:raise ValueError('Unbounded worker/shard settings')
 root=Path(out_dir)
 if (root/'archive_manifest.json').exists():raise ValueError('Refuse to overwrite sealed archive')
 root.mkdir(parents=True,exist_ok=True)
 if tickers is None:
  from shared_market_data import fetch_naver_universe
  universe=fetch_naver_universe();tickers=[r['ticker'] for r in universe]
  write_json(root/'current_universe.json',universe)
 tickers=sorted(set(code(t) for t in tickers));selected=[t for i,t in enumerate(tickers) if i%shard_count==shard_index]
 write_json(root/'request.json',{'start_date':start,'end_date':end,'selected_tickers':selected,'universe_size':len(tickers),
  'universe_basis':'CURRENT_LISTED_ONLY_NOT_POINT_IN_TIME','delisted_universe_verified':False,'shard_index':shard_index,'shard_count':shard_count})
 results=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
  futures={ex.submit(collect_ticker,t,start,end,root/'tickers'):t for t in selected}
  for future in concurrent.futures.as_completed(futures):
   try:result=future.result()
   except Exception as e:result={'ticker':futures[future],'status':'PARTIAL_OR_MISSING','errors':{'worker':str(e)}}
   results.append(result)
   with (root/'progress.jsonl').open('a') as f:f.write(json.dumps(result,ensure_ascii=False)+'\n')
   if len(results)%25==0:print(f'ARCHIVE_PROGRESS {len(results)}/{len(selected)}',flush=True)
 summary={'version':VERSION,'research_only':True,'official_prediction':False,'orders_sent':0,'start_date':start,'end_date':end,
  'run_id':os.getenv('GITHUB_RUN_ID'),'attempt':os.getenv('GITHUB_RUN_ATTEMPT'),'code_sha':os.getenv('GITHUB_SHA'),
  'selected':len(selected),'completed':len(results),'available_history':sum(r['status']=='AVAILABLE_HISTORY_ARCHIVED' for r in results),
  'partial_or_missing':sum(r['status']!='AVAILABLE_HISTORY_ARCHIVED' for r in results),'price_rows':sum(r.get('prices_rows',0) for r in results),
  'flow_rows':sum(r.get('flows_rows',0) for r in results),'limitations':['CURRENT_LISTED_UNIVERSE_ONLY','SESSION_ADJUSTMENT_CALENDAR_UNVERIFIED','TRADING_VALUE_NOT_PROVIDED','NO_HISTORICAL_PREDICTIONS_CREATED']}
 write_json(root/'archive_summary.json',summary)
 files=[]
 for p in sorted(root.rglob('*')):
  if p.is_file():files.append({'path':str(p.relative_to(root)),'size_bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
 write_json(root/'archive_manifest.json',{'files':files});print(json.dumps(summary,ensure_ascii=False),flush=True);return summary

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--start-date',default='2023-01-01');p.add_argument('--end-date',default=dt.datetime.now(TZ).date().isoformat());p.add_argument('--out-dir',required=True);p.add_argument('--tickers',nargs='+');p.add_argument('--full-universe',action='store_true');p.add_argument('--confirm-full-market',action='store_true');p.add_argument('--workers',type=int,default=2);p.add_argument('--shard-index',type=int,default=0);p.add_argument('--shard-count',type=int,default=1);a=p.parse_args()
 if a.full_universe != a.confirm_full_market or (not a.tickers and not a.full_universe) or (a.tickers and a.full_universe):p.error('Choose bounded tickers or explicitly confirm full universe')
 s=collect(a.start_date,a.end_date,a.out_dir,a.tickers,workers=a.workers,shard_index=a.shard_index,shard_count=a.shard_count)
 return 0 if s['selected']>0 and s['available_history']>0 else 2
if __name__=='__main__':raise SystemExit(main())
