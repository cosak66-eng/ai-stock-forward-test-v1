#!/usr/bin/env python3
"""V1.0 market data DIAGNOSTIC. Never authorizes predictions or edits data/."""
import argparse, csv, json, os, re, sys, traceback, urllib.parse, urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SAMPLE = ('005930', '000660', '035420', '005380', '035720')
FIELDS = ('close', 'volume', 'trading_value')


def parse_num(value):
    if isinstance(value, bool) or value is None: return None
    try:
        x = float(str(value).replace(',', ''))
        return int(x) if x.is_integer() else None
    except (ValueError, TypeError, OverflowError): return None


def normalize(ticker, close, volume, trading_value, source, date):
    if not re.fullmatch(r'\d{6}', str(ticker)): return None
    row = dict(ticker=str(ticker), date=date, source=source)
    for k, v in zip(FIELDS, (close, volume, trading_value)):
        row[k] = parse_num(v)
    if row['close'] is None or row['close'] <= 0: return None
    if row['volume'] is None or row['volume'] < 0: return None
    if row['trading_value'] is None or row['trading_value'] < 0: return None
    return row


def krx_fetch(asof, diagnostics=None):
    from pykrx import stock
    result = {}
    for market in ('KOSPI', 'KOSDAQ'):
        try:
            df = stock.get_market_ohlcv_by_ticker(asof, market=market, alternative=False)
        except Exception as exc:
            if diagnostics is not None:
                diagnostics[market] = {'status': 'FETCH_EXCEPTION', 'exception_type': type(exc).__name__, 'error': str(exc)[:500], 'traceback': traceback.format_exc(limit=7)[-2500:]}
            raise RuntimeError(f'KRX_{market}_FETCH_EXCEPTION:{type(exc).__name__}:{exc}') from exc
        if diagnostics is not None:
            diagnostics[market] = {'status': 'RETURNED', 'shape': list(df.shape) if df is not None else None, 'columns': [str(c) for c in df.columns] if df is not None else [], 'index_sample': [str(v) for v in df.index[:3]] if df is not None else []}
        if df is None or df.empty: raise RuntimeError(f'KRX_{market}_EMPTY')
        required = {'종가', '거래량', '거래대금'}
        if not required.issubset(df.columns): raise RuntimeError(f'KRX_{market}_MISSING_FIELDS:{list(df.columns)}')
        count = 0
        for code, r in df.iterrows():
            rec = normalize(str(code).zfill(6), r['종가'], r['거래량'], r['거래대금'], 'PYKRX_KRX_DAILY', asof)
            if rec is not None:
                if rec['ticker'] in result: raise RuntimeError('DUPLICATE_TICKER:'+rec['ticker'])
                result[rec['ticker']] = rec
                count += 1
        if count < 100: raise RuntimeError(f'KRX_{market}_TOO_FEW:{count}')
    return result


def daum_fetch_one(ticker, asof):
    sym = 'A'+ticker
    q = urllib.parse.urlencode(dict(symbolCode=sym, page=1, perPage=10, pagination='true'))
    url = f'https://finance.daum.net/api/quote/{sym}/days?{q}'
    req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0', 'Referer':f'https://finance.daum.net/quotes/{sym}', 'Accept':'application/json, text/plain, */*'})
    with urllib.request.urlopen(req, timeout=18) as response:
        if response.status != 200: raise RuntimeError('HTTP_'+str(response.status))
        obj = json.loads(response.read(2_000_000).decode('utf-8'))
    if not isinstance(obj, dict) or not isinstance(obj.get('data'), list): raise RuntimeError('INVALID_DAUM_SCHEMA')
    for x in obj['data']:
        if str(x.get('date',''))[:10].replace('-','') == asof:
            return normalize(ticker, x.get('tradePrice'), x.get('accTradeVolume'), x.get('accTradePrice'), 'DAUM_DAILY', asof)
    return None


def compare(a, b):
    if not a or not b: return {'status':'MISSING_SOURCE', 'fields':{}}
    if a['ticker'] != b['ticker'] or a['date'] != b['date']:
        return {'status':'DATE_OR_TICKER_MISMATCH', 'fields':{}}
    fields = {k: ('MATCH' if a[k] == b[k] else 'MISMATCH') for k in FIELDS}
    return {'status':'MATCH_3_OF_3' if all(v=='MATCH' for v in fields.values()) else 'VALUE_MISMATCH', 'fields':fields}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='YYYYMMDD; default current KST date')
    ap.add_argument('--output', default='output/market_crosscheck')
    args = ap.parse_args()
    asof = args.date or datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%d')
    if not re.fullmatch(r'\d{8}', asof): ap.error('invalid date')
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    report = {'date':asof,'created_kst':datetime.now(ZoneInfo('Asia/Seoul')).isoformat(), 'diagnostic_only':True, 'prediction_eligible':False, 'data_cutoff_18_kst_proven':False, 'regular_session_proven':False, 'sources':{}, 'comparisons':{}, 'errors':[]}
    krx = {}
    report['pykrx_diagnostics'] = {}
    try:
        krx = krx_fetch(asof, report['pykrx_diagnostics'])
        report['sources']['pykrx']={'status':'OK', 'rows':len(krx)}
    except Exception as exc:
        report['sources']['pykrx']={'status':'ERROR', 'error':str(exc)[:500]}
        report['errors'].append('PYKRX_FETCH_FAILED')
    daum = {}
    for ticker in SAMPLE:
        try:
            rec = daum_fetch_one(ticker, asof)
            if rec: daum[ticker] = rec
            else: report['errors'].append('DAUM_MISSING_'+ticker)
        except Exception as exc:
            report['errors'].append('DAUM_'+ticker+':'+str(exc)[:150])
    report['sources']['daum']={'status':'OK' if len(daum)==len(SAMPLE) else 'PARTIAL_OR_FAILED','rows':len(daum)}
    for ticker in SAMPLE:
        report['comparisons'][ticker] = compare(krx.get(ticker),daum.get(ticker))
    match_count=sum(x['status']=='MATCH_3_OF_3' for x in report['comparisons'].values())
    report['sample_match_count']=match_count
    report['sample_count']=len(SAMPLE)
    report['crosscheck_sample_pass']=(match_count==len(SAMPLE))
    report['full_universe_count']=len(krx)
    report['full_universe_price_coverage_diagnostic']=(len(krx)>=1000)
    report['status']='SAMPLE_MATCH_DIAGNOSTIC_ONLY' if report['crosscheck_sample_pass'] else 'CROSSCHECK_FAILED'
    # A 20:00 retrieval cannot independently prove a snapshot existed before the 18:00 information cutoff.
    # No automatic write to data/, no revision of historical audit, no Top20/prediction generation.
    (output/'market_crosscheck_report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    with (output/'market_crosscheck_sample.csv').open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.writer(f);writer.writerow(['ticker','source','close','volume','trading_value','date'])
        for ticker in SAMPLE:
            for rec in (krx.get(ticker),daum.get(ticker)):
                if rec:writer.writerow([rec['ticker'],rec['source'],rec['close'],rec['volume'],rec['trading_value'],rec['date']])
    print(json.dumps({k:report[k] for k in ('status','sources','sample_match_count','sample_count','errors')},ensure_ascii=False,indent=2))
    return 0 if report['crosscheck_sample_pass'] else 2


if __name__=='__main__': sys.exit(main())
