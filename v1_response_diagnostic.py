"""Three bounded public reads; no collector execution or prediction writes."""
import argparse,json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import collector as c
import market_crosscheck as m

def run(out_dir):
 out=Path(out_dir);out.mkdir(parents=True,exist_ok=True)
 url='https://stock.naver.com/api/domestic/market/stock/default?tradeType=KRX&marketType=ALL&orderType=marketSum&startIdx=0&pageSize=20'
 payload=c.http_json(url,c.NAVER_HEADERS)
 (out/'market_response.json').write_text(json.dumps({'url':url,'payload':payload},ensure_ascii=False,indent=2))
 records=list(c.record_dicts(payload));recognized=sum(all(c.nnum(x.get(k)) is not None for k in ['nowPrice','tradeVolume','tradeAmount']) for x in records)
 today=datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%d')
 rows,metadata=m.naver_dated_samples(today,evidence_dir=out/'raw',sample=('005930','000660'))
 result={'diagnostic_only':True,'official_prediction_allowed':False,'orders_sent':0,'source_records':len(records),'recognized_current_fields':recognized,'regular_rows_accepted':sum(c.parse_current_record(x) is not None for x in records),'dated_samples':rows,'daily_metadata':metadata,'capture_window_certified':False}
 result['adapter_schema_pass']=recognized>0 and len(rows)==2
 (out/'response_diagnostic.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(result,ensure_ascii=False,indent=2));return 0 if result['adapter_schema_pass'] else 2
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--out-dir',default='output/response_diagnostic');a=p.parse_args();raise SystemExit(run(a.out_dir))
