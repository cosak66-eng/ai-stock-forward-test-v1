#!/usr/bin/env python3
"""Read-only V1 market close diagnostic. Optional sources never block primary checks."""
import argparse, csv, json, re, sys, urllib.parse, urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SAMPLE=('005930','000660','035420','005380','035720')
HEADERS={'User-Agent':'Mozilla/5.0','Accept':'application/json, text/plain, */*'}


def integer(v):
    if v is None or isinstance(v,bool): return None
    try:
        from decimal import Decimal
        x=Decimal(str(v).replace(',','').strip())
        return int(x) if x.is_finite() and x==int(x) else None
    except Exception: return None


def code(v):
    s=str(v or '').strip().upper()
    if s.startswith('A') and len(s)==7: s=s[1:]
    return s if re.fullmatch(r'\d{6}',s) else None


def make_row(ticker,date,source,close,volume=None,value=None):
    ticker=code(ticker);close=integer(close);volume=integer(volume);value=integer(value)
    if not ticker or not re.fullmatch(r'\d{8}',str(date)) or close is None or close<=0: return None
    if volume is not None and volume<0 or value is not None and value<0:return None
    return dict(ticker=ticker,date=date,source=source,close=close,volume=volume,trading_value=value)


def get_json(url,referer):
    h=dict(HEADERS,Referer=referer)
    with urllib.request.urlopen(urllib.request.Request(url,headers=h),timeout=20) as r:
        if r.status!=200: raise ValueError('HTTP_'+str(r.status))
        return json.loads(r.read(3_000_000).decode('utf-8'))


def daum_one(ticker,date):
    sym='A'+ticker
    q=urllib.parse.urlencode({'symbolCode':sym,'page':1,'perPage':15,'pagination':'true'})
    o=get_json(f'https://finance.daum.net/api/quote/{sym}/days?{q}',f'https://finance.daum.net/quotes/{sym}')
    for x in o.get('data',[]):
        if str(x.get('date',''))[:10].replace('-','')==date:
            return make_row(ticker,date,'DAUM',x.get('tradePrice'),x.get('accTradeVolume'),x.get('accTradePrice'))
    return None


def walk(o):
    if isinstance(o,list):
        for x in o: yield from walk(x)
    elif isinstance(o,dict):
        yield o
        for v in o.values():
            if isinstance(v,(list,dict)): yield from walk(v)


def pick(d,*keys):
    for k in keys:
        if k in d and d[k] not in (None,''):return d[k]
    return None


def naver_rows(date,max_pages=45):
    """Naver market list. A date must be explicitly provided by response for strict matching."""
    rows={}; metadata={'pages':0,'undated_rows':0,'raw_records':0,'sample_keys':[]}
    for page in range(max_pages):
        q=urllib.parse.urlencode({'tradeType':'KRX','marketType':'ALL','orderType':'marketSum','startIdx':page,'pageSize':100})
        o=get_json('https://stock.naver.com/api/domestic/market/stock/default?'+q,'https://stock.naver.com/')
        new=0; seen=0
        for d in walk(o):
            ticker=code(pick(d,'itemCode','itemcode','stockCode','code','symbolCode','ticker'))
            if not ticker:continue
            seen+=1;metadata['raw_records']+=1
            if not metadata['sample_keys']:metadata['sample_keys']=list(d.keys())[:35]
            raw_date=pick(d,'bizdate','bizDate','tradeDate','tradingDate','baseDate','date')
            ds=re.sub(r'\D','',str(raw_date or ''))[:8]
            if ds!=date:
                metadata['undated_rows']+=1
                continue
            row=make_row(ticker,date,'NAVER',pick(d,'closePrice','close','tradePrice','currentPrice','nowPrice'),pick(d,'accTradeVolume','tradeVolume','volume'),pick(d,'accTradePrice','tradeAmount','tradingValue','tradingAmount'))
            if row:
                if ticker not in rows:new+=1
                rows[ticker]=row
        metadata['pages']+=1
        if seen==0 or (page>0 and new==0):break
    return rows,metadata


def naver_dated_samples(date, evidence_dir=None, sample=SAMPLE):
    """Bounded official-page daily response; never date-stamp an undated quote.

    closingPrice/volume/value are kept from the SAME daily response. Do not
    combine regularMarketClosingPrice with post-close volume/value. This
    diagnostic remains unverified for regular-session/cutoff purposes.
    """
    rows = {}
    metadata = {'route': 'NAVER_ITEMS_V2_DAILY_PRICES', 'requests': 0,
                'undated_rows': 0, 'errors': {}, 'regular_session_proven': False}
    for ticker in sample:
        if code(ticker) != ticker:
            raise ValueError('Invalid diagnostic ticker')
        url = ('https://stock.naver.com/api/stockSecurity/items/v2/domestic/'
               + ticker + '/daily-prices/paged?size=5&index=0')
        try:
            payload = get_json(url, 'https://stock.naver.com/domestic/stock/' + ticker + '/price')
            metadata['requests'] += 1
            if evidence_dir is not None:
                root = Path(evidence_dir); root.mkdir(parents=True, exist_ok=True)
                (root / (ticker + '_naver_daily.json')).write_text(
                    json.dumps({'request_url': url, 'payload': payload}, ensure_ascii=False, indent=2), encoding='utf-8')
            if not isinstance(payload, dict) or not isinstance(payload.get('items'), list):
                raise ValueError('Unexpected NAVER daily schema')
            for raw in payload['items']:
                day = str(raw.get('tradingDateKst') or '').replace('-', '')
                if day != date:
                    metadata['undated_rows'] += 1
                    continue
                row = make_row(ticker, day, 'NAVER', raw.get('closingPrice'),
                               raw.get('tradingVolume'), raw.get('tradingValue'))
                if row:
                    rows[ticker] = row
                break
        except Exception as exc:
            metadata['errors'][ticker] = type(exc).__name__ + ':' + str(exc)[:250]
    return rows, metadata


def compare_primary(n,d):
    if n is None or d is None:return {'status':'PRIMARY_MISSING','fields':{}}
    if n['ticker']!=d['ticker'] or n['date']!=d['date']:return {'status':'PRIMARY_DATE_MISMATCH','fields':{}}
    fields={}
    for k in ('close','volume','trading_value'):
        if n[k] is None or d[k] is None:fields[k]='UNAVAILABLE'
        else:fields[k]='MATCH' if n[k]==d[k] else 'MISMATCH'
    status='PRIMARY_MATCH' if all(v=='MATCH' for v in fields.values()) else 'PRIMARY_MISMATCH' if 'MISMATCH' in fields.values() else 'PRIMARY_INCOMPLETE'
    return {'status':status,'fields':fields}


def optional_csv(path,date):
    """Optional evidence: source,ticker,date,close. Missing file/rows are SKIPPED."""
    result={};warnings=[]
    if not path.exists():return result,['OPTIONAL_CSV_ABSENT']
    try:
        with path.open(encoding='utf-8-sig',newline='') as f:
            for r in csv.DictReader(f):
                source=str(r.get('source','')).strip().upper()
                if source not in ('FNGUIDE','INVESTING','YAHOO','NATE'):continue
                if str(r.get('date','')).replace('-','')!=date:continue
                row=make_row(r.get('ticker'),date,source,r.get('close'))
                if row:result[(source,row['ticker'])]=row
                else:warnings.append('OPTIONAL_INVALID_ROW_'+source)
    except Exception as e:warnings.append('OPTIONAL_CSV_READ_FAILED:'+type(e).__name__)
    return result,warnings


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--date');p.add_argument('--output',default='output/market_crosscheck');p.add_argument('--optional-csv',default='evidence/four_source_observations.csv');a=p.parse_args(argv)
    date=a.date or datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%d')
    if not re.fullmatch(r'\d{8}',date):p.error('date must be YYYYMMDD')
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    report={'date':date,'diagnostic_only':True,'prediction_eligible':False,'regular_session_proven':False,'cutoff_18_kst_proven':False,'primary':{},'optional':{},'comparisons':{},'warnings':[],'errors':[]}
    try:
        naver,meta=naver_dated_samples(date, evidence_dir=out / "raw")
        report['primary']['naver']={'rows':len(naver),'metadata':meta}
    except Exception as e:
        naver={};report['primary']['naver']={'rows':0,'error':type(e).__name__+':'+str(e)[:250]};report['errors'].append('NAVER_FETCH_FAILED')
    daum={}
    for ticker in SAMPLE:
        try:
            row=daum_one(ticker,date)
            if row:daum[ticker]=row
        except Exception as e:report['errors'].append('DAUM_'+ticker+':'+type(e).__name__+':'+str(e)[:100])
    report['primary']['daum']={'rows':len(daum)}
    opt,warnings=optional_csv(Path(a.optional_csv),date);report['warnings']+=warnings
    for ticker in SAMPLE:
        comparison=compare_primary(naver.get(ticker),daum.get(ticker))
        comparison['optional']={}
        for source in ('FNGUIDE','INVESTING','YAHOO','NATE'):
            r=opt.get((source,ticker))
            base=daum.get(ticker) or naver.get(ticker)
            state='SKIPPED' if r is None else 'NO_PRIMARY' if base is None else 'MATCH' if r['close']==base['close'] else 'WARNING_MISMATCH'
            comparison['optional'][source]=state
        report['comparisons'][ticker]=comparison
    report['optional']['available_rows']=len(opt)
    report['primary_match_count']=sum(x['status']=='PRIMARY_MATCH' for x in report['comparisons'].values())
    report['primary_pass']=report['primary_match_count']==len(SAMPLE)
    report['status']='PRIMARY_MATCH_DIAGNOSTIC_ONLY' if report['primary_pass'] else 'PRIMARY_NOT_VERIFIED'
    (out/'market_crosscheck_report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    with (out/'market_crosscheck_sample.csv').open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f);w.writerow(['ticker','source','date','close','volume','trading_value'])
        for t in SAMPLE:
            for r in (naver.get(t),daum.get(t)):
                if r:w.writerow([r[k] for k in ('ticker','source','date','close','volume','trading_value')])
    print(json.dumps({'status':report['status'],'primary':report['primary'],'optional':report['optional'],'warnings':report['warnings'],'errors':report['errors']},ensure_ascii=False,indent=2))
    # Exit code 2 is reserved for genuine primary failure; optional failures never affect it.
    return 0 if report['primary_pass'] else 2

if __name__=='__main__':sys.exit(main())
