#!/usr/bin/env python3
"""Read-only diagnostics for FREEWEB V1.2.4 Naver KRX collector.
No trading, no edits to data/, and no prediction creation.
"""
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = 'https://stock.naver.com/api/domestic/market/stock/default'
HEADERS = {'User-Agent':'Mozilla/5.0','Referer':'https://stock.naver.com/','Accept':'application/json, text/plain, */*'}
CODE_KEYS = ('itemCode','itemcode','stockCode','symbolCode','code','isuSrtCd','ISU_SRT_CD')
CLOSE_KEYS = ('closePrice','tradePrice','currentPrice','nowVal','price')
VOL_KEYS = ('accumulatedTradingVolume','accTradeVolume','tradingVolume','accQuant','volume','quant')
VALUE_KEYS = ('accumulatedTradingValue','accTradePrice','tradingValue','accAmount','amount','tradingAmount')

def code(v):
    s = str(v or '').strip().upper()
    if re.fullmatch(r'A[0-9A-Z]{6}',s): s=s[1:]
    return s if re.fullmatch(r'[0-9A-Z]{6}',s) else None

def getcode(d):
    return next((x for k in CODE_KEYS if (x:=code(d.get(k)))),None)

def rows(obj):
    if isinstance(obj,dict):
        if getcode(obj): yield obj
        for v in obj.values(): yield from rows(v)
    elif isinstance(obj,list):
        for v in obj: yield from rows(v)

def numeric(v):
    try: return float(str(v).replace(',','').replace('%','').strip())
    except (ValueError,TypeError): return None

def first(d, keys):
    for k in keys:
        if k in d and d[k] is not None and not isinstance(d[k],(list,dict)):
            n=numeric(d[k]);
            if n is not None:return n
    return None

def probe(page):
    query=urllib.parse.urlencode({'tradeType':'KRX','marketType':'ALL','orderType':'marketSum','startIdx':page,'pageSize':100})
    request=urllib.request.Request(BASE+'?'+query,headers=HEADERS)
    with urllib.request.urlopen(request,timeout=25) as r: data=json.load(r)
    found=list(rows(data)); unique={getcode(x) for x in found}
    return {'page':page,'root_type':type(data).__name__,
      'root_keys':sorted(data)[:40] if isinstance(data,dict) else [],
      'coded_rows':len(found),'distinct_codes':len(unique),
      'sample_codes':sorted(unique)[:8],
      'sample_fields':sorted(found[0])[:120] if found else [],
      'valid_close':sum(first(x,CLOSE_KEYS) is not None for x in found),
      'valid_volume':sum(first(x,VOL_KEYS) is not None for x in found),
      'valid_trading_value':sum(first(x,VALUE_KEYS) is not None for x in found),
      'sample_field_types':{k:type(v).__name__ for k,v in list(found[0].items())[:40]} if found else {}}

def main():
    report={'timestamp_kst':datetime.now(ZoneInfo('Asia/Seoul')).isoformat(),
            'diagnostic_only':True,'prediction_eligible':False,
            'note':'Out-of-window diagnostics do not certify KRX regular-session close or data integrity.',
            'pages':[]}
    for page in (0,1):
        try: report['pages'].append(probe(page))
        except Exception as e: report['pages'].append({'page':page,'error':str(e)})
    print(json.dumps(report,ensure_ascii=False,indent=2))
    with open('collector_diagnostic_report.json','w',encoding='utf-8') as f:
        json.dump(report,f,ensure_ascii=False,indent=2)
    return 0

if __name__=='__main__':raise SystemExit(main())
