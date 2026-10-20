"""Bounded live source evidence; never creates predictions or edits a journal."""
from pathlib import Path
from datetime import datetime
import argparse,hashlib,json
import shared_market_data as m

def run(out_dir,tickers):
 out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
 records=[];files=[];today=datetime.now(m.TZ).date().isoformat()
 for ticker in tickers:
  number=0
  def text(url,accept='application/json, text/plain, */*'):
   nonlocal number
   number+=1
   raw,charset=m.request_bytes(url,{'Accept':accept},timeout=15,tries=2)
   name=f'{ticker}_{number}.raw';(out/name).write_bytes(raw)
   files.append({'name':name,'url':url,'size':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
   return m.decode_text(raw,charset)
  try:
   prices=m.fetch_naver_history(ticker,140,get_json=lambda url:json.loads(text(url)),get_text=text,asof=today)
   record={'ticker':ticker,'price_rows':len(prices),'latest_date':prices[0]['date'],'route':prices[0]['price_history_route'],'price_probe_pass':len(prices)>=120}
   sym='A'+ticker
   url=f'https://finance.daum.net/api/quote/{sym}/days?symbolCode={sym}&page=1&perPage=140&pagination=true'
   try:
    raw,charset=m.request_bytes(url,{'Referer':f'https://finance.daum.net/quotes/{sym}'},timeout=15,tries=2)
    name=ticker+'_daum.raw';(out/name).write_bytes(raw)
    files.append({'name':name,'url':url,'size':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
    payload=json.loads(m.decode_text(raw,charset));values=[]
    for row in payload.get('data',[]):
     day=str(row.get('date',''))[:10]
     try:datetime.fromisoformat(day)
     except ValueError:continue
     def exact(k):
      x=row.get(k)
      if isinstance(x,bool):return None
      try:return float(str(x).replace(',',''))
      except (ValueError,TypeError):return None
     values.append({'date':day,'open':exact('openingPrice'),'high':exact('highPrice'),'low':exact('lowPrice'),'close':exact('tradePrice'),'volume':exact('accTradeVolume'),'trading_value':exact('accTradePrice')})
    error=None
   except (RuntimeError,ValueError,OSError) as exc:values=[];error=str(exc)[:400]
   _,parity=m.v1_prior_history(prices,values,today)
   record.update({'v1_prior_field_parity':parity,'actual_value_source_error':error})
  except (RuntimeError,ValueError,OSError) as exc:record={'ticker':ticker,'price_probe_pass':False,'error':str(exc)[:500]}
  records.append(record)
 result={'version':m.VERSION,'observed_at_kst':datetime.now(m.TZ).isoformat(),'diagnostic_only':True,'price_acquisition_pass':all(r['price_probe_pass'] for r in records),'official_prediction_allowed':False,'orders_sent':0,'records':records,'raw_files':files,'v1_from_v2_audit':m.check_v1_capture({'source':'NAVER','mode':'full','research_collection_eligible':True})}
 (out/'diagnostic_audit.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(result,ensure_ascii=False,indent=2));return result

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out-dir',default='output/shared_collector');p.add_argument('--tickers',nargs='+',default=['005930','000660']);a=p.parse_args()
 if len(a.tickers)>5:p.error('Maximum 5 diagnostic tickers')
 result=run(a.out_dir,a.tickers);raise SystemExit(0 if result['price_acquisition_pass'] else 2)
