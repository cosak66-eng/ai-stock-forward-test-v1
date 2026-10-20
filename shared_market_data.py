"""Shared acquisition primitives with V1/V2 data profiles, vendored identically.

Acquisition is shared; model eligibility, capture windows and journals are not.
No orders, imputation, close*volume monetary estimates or source certification.
"""
from __future__ import annotations
import ast,csv,io,json,math,random,re,time
import urllib.request,urllib.error,urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime,timedelta,date
from typing import Any,Iterable
from zoneinfo import ZoneInfo
VERSION='COMMON-MARKET-DATA-0.1.1'
TZ=ZoneInfo('Asia/Seoul')
DEFAULT_HEADERS={'User-Agent':'Mozilla/5.0','Referer':'https://stock.naver.com/','Accept':'application/json, text/plain, */*'}
PERMANENT_HTTP={400,401,403,404,410,422}

def request_bytes(url,headers=None,timeout=30,tries=4):
    if not 1<=tries<=4 or not 0<timeout<=60:raise ValueError('Unbounded HTTP policy')
    last=None
    for attempt in range(tries):
        try:
            req=urllib.request.Request(url,headers={**DEFAULT_HEADERS,**(headers or {})})
            with urllib.request.urlopen(req,timeout=timeout) as response:
                if response.status!=200:raise RuntimeError('HTTP '+str(response.status))
                raw=response.read(16*1024*1024+1)
                if len(raw)>16*1024*1024:raise RuntimeError('Source response exceeds 16 MiB')
                return raw,response.headers.get_content_charset() or ''
        except urllib.error.HTTPError as exc:
            last=exc
            if exc.code in PERMANENT_HTTP:break
            retry=exc.headers.get('Retry-After','') if exc.headers else ''
        except (urllib.error.URLError,TimeoutError,OSError) as exc:
            last=exc;retry=''
        if attempt+1<tries:
            delay=min(20.0,1.5*2**attempt)
            if retry.isdecimal():delay=max(delay,min(60.0,float(retry)))
            time.sleep(delay+random.random()*.5)
    raise RuntimeError(f'request failed after {attempt+1} attempts: {url} :: {last}')

def decode_text(raw,charset=''):
    for encoding in (charset,'utf-8-sig','euc-kr','cp949'):
        if not encoding:continue
        try:return raw.decode(encoding)
        except (UnicodeError,LookupError):pass
    return raw.decode('utf-8',errors='replace')

def request_text(url,headers=None,timeout=30,tries=4,*,accept=None):
    h={**(headers or {})}
    if accept:h['Accept']=accept
    raw,charset=request_bytes(url,h,timeout,tries)
    return decode_text(raw,charset)

def request_json(url,headers=None,timeout=30,tries=4):
    return json.loads(request_text(url,headers,timeout,tries))

CHART_PRICE_ENDPOINT = "https://api.stock.naver.com/chart/domestic/item/{ticker}"

FCHART_PRICE_ENDPOINT = "https://fchart.stock.naver.com/sise.nhn"

SISE_JSON_PRICE_ENDPOINT = "https://api.finance.naver.com/siseJson.naver"

DATE_KEYS = (
    "bizdate", "bizDate", "date", "tradeDate", "tradingDate",
    "tradingDateKst", "localTradedAt", "localDate", "localDateTime",
    "baseDate", "standardDate", "tradeBaseAt",
)

OPEN_KEYS = ("openPrice", "openingPrice", "open")

HIGH_KEYS = ("highPrice", "high")

LOW_KEYS = ("lowPrice", "low")

CLOSE_KEYS = ("closePrice", "closingPrice", "tradePrice", "currentPrice", "price")

VOLUME_KEYS = (
    "accumulatedTradingVolume", "accTradeVolume",
    "tradingVolume", "tradeVolume", "volume", "quant",
)

VALUE_KEYS = (
    "accumulatedTradingValue", "accTradePrice",
    "tradingValue", "tradingAmount", "amount",
)

def normalize_number(v: Any) -> float | None:
    if v is None or isinstance(v, bool) or isinstance(v, (dict, list, tuple)):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return x if math.isfinite(x) else None

    s = str(v).strip()
    if not s or s.lower() in {
        "null", "none", "nan", "-", "--", "data not available"
    }:
        return None

    negative = s.startswith("(") and s.endswith(")")
    if negative:
        s = s[1:-1]

    s = (
        s.replace(",", "")
         .replace("%", "")
         .replace("+", "")
         .replace("원", "")
         .replace("주", "")
         .strip()
    )

    multiplier = 1.0
    for unit, factor in (
        ("조", 1_000_000_000_000.0),
        ("억", 100_000_000.0),
        ("만", 10_000.0),
        ("천", 1_000.0),
    ):
        if s.endswith(unit):
            multiplier = factor
            s = s[:-1].strip()
            break

    try:
        x = float(s) * multiplier
        return -x if negative else x
    except ValueError:
        return None

def normalize_date(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    m = re.match(r"^(20\d{2})[-/.]?(\d{2})[-/.]?(\d{2})", s)
    if not m:
        return None
    y, mo, d = m.groups()
    return f"{y}-{mo}-{d}"

def scalar_first(d: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in d and not isinstance(d[key], (dict, list, tuple)):
            if d[key] not in (None, ""):
                return d[key]
    return None

def recursive_dicts(
    obj: Any, path: str = "$"
) -> Iterable[tuple[str, dict[str, Any]]]:
    if isinstance(obj, dict):
        yield path, obj
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                yield from recursive_dicts(value, f"{path}.{key}")
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            if isinstance(value, (dict, list)):
                yield from recursive_dicts(value, f"{path}[{i}]")

def find_dated_price_rows(payload: Any) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []

    for _, d in recursive_dicts(payload):
        date = normalize_date(scalar_first(d, DATE_KEYS))
        if not date:
            continue

        close = normalize_number(scalar_first(d, CLOSE_KEYS))
        if close is None:
            continue

        candidates.append(
            {
                "date": date,
                "open": normalize_number(scalar_first(d, OPEN_KEYS)),
                "high": normalize_number(scalar_first(d, HIGH_KEYS)),
                "low": normalize_number(scalar_first(d, LOW_KEYS)),
                "close": close,
                "volume": normalize_number(
                    scalar_first(d, VOLUME_KEYS)
                ),
                "trading_value": normalize_number(
                    scalar_first(d, VALUE_KEYS)
                ),
                "raw_keys": sorted(d.keys()),
            }
        )

    # Deduplicate by date. Prefer row with more non-null normalized fields.
    best: dict[str, dict[str, Any]] = {}

    for row in candidates:
        score = sum(
            row.get(k) is not None
            for k in (
                "open", "high", "low", "close",
                "volume", "trading_value",
            )
        )
        old = best.get(row["date"])
        if old is None:
            row["_score"] = score
            best[row["date"]] = row
        elif score > old.get("_score", -1):
            row["_score"] = score
            best[row["date"]] = row

    rows = []
    for row in best.values():
        row.pop("_score", None)
        rows.append(row)

    rows.sort(key=lambda r: r["date"], reverse=True)
    return rows

def _valid_price_rows(rows: list[dict[str, Any]], target_rows: int) -> list[dict[str, Any]]:
    """Accept only observed daily OHLCV; never interpolate, forward-fill, or invent."""
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        date = normalize_date(row.get("date"))
        op, hi, lo, cl, vol = (row.get(k) for k in ("open", "high", "low", "close", "volume"))
        if not date or not all(isinstance(x, (int, float)) and math.isfinite(x)
                               for x in (op, hi, lo, cl, vol)):
            continue
        if min(op, hi, lo, cl) <= 0 or vol < 0:
            continue
        if hi < max(op, cl, lo) or lo > min(op, cl, hi):
            continue
        row = dict(row)
        row["date"] = date
        unique[date] = row
    return sorted(unique.values(), key=lambda r: r["date"], reverse=True)[:target_rows]

def _parse_sisejson(raw: str, target_rows: int) -> list[dict[str, Any]]:
    """NAVER siseJson often contains a JS-like array, not strict JSON."""
    rows: list[dict[str, Any]] = []
    try:
        parsed = ast.literal_eval(raw.strip().rstrip(";"))
        candidates = parsed if isinstance(parsed, (list, tuple)) else []
    except (ValueError, SyntaxError, TypeError):
        # Restrict fallback matching to single flat array rows.
        candidates = []
        for group in re.findall(r"\[([^\[\]]+)\]", raw):
            try:
                candidates.append(next(csv.reader(io.StringIO(group), skipinitialspace=True)))
            except (csv.Error, StopIteration):
                continue
    for fields in candidates:
        if not isinstance(fields, (list, tuple)) or len(fields) < 6:
            continue
        date = normalize_date(fields[0])
        if not date:
            continue  # skip localized column header
        vals = [normalize_number(x) for x in fields[1:6]]
        if any(x is None for x in vals):
            continue
        op, hi, lo, cl, vol = vals
        rows.append({
            "date": date, "open": op, "high": hi, "low": lo,
            "close": cl, "volume": vol, "trading_value": None,
            "raw_keys": ["siseJson:array"],
        })
    return _valid_price_rows(rows, target_rows)

def fetch_naver_history(ticker,target_rows=130,*,get_json=None,get_text=None,asof=None):
    if not re.fullmatch(r'[0-9A-Z]{6}',str(ticker)) or not 1<=target_rows<=500:
        raise ValueError('Invalid ticker or bounded history length')
    get_json=get_json or request_json;get_text=get_text or request_text
    today=date.fromisoformat(asof) if asof else datetime.now(TZ).date()
    first=today-timedelta(days=max(420,target_rows*4))
    start,end=first.strftime('%Y%m%d'),today.strftime('%Y%m%d')
    def chart():
        url=CHART_PRICE_ENDPOINT.format(ticker=ticker)+'?'+urllib.parse.urlencode({'periodType':'dayCandle','startDateTime':start,'endDateTime':end})
        return _valid_price_rows(find_dated_price_rows(get_json(url)),target_rows)
    def xml():
        url=FCHART_PRICE_ENDPOINT+'?'+urllib.parse.urlencode({'symbol':ticker,'timeframe':'day','count':max(target_rows+20,160),'requestType':0})
        root=ET.fromstring(get_text(url));rows=[]
        for el in root.iter('item'):
            parts=(el.get('data') or '').strip().split('|')
            if len(parts)<6:continue
            day=normalize_date(parts[0]);values=[normalize_number(x) for x in parts[1:6]]
            if day and all(x is not None for x in values):
                op,hi,lo,cl,vol=values
                rows.append({'date':day,'open':op,'high':hi,'low':lo,'close':cl,'volume':vol,'trading_value':None,'raw_keys':['fchart:item:data']})
        return _valid_price_rows(rows,target_rows)
    def sisejson():
        url=SISE_JSON_PRICE_ENDPOINT+'?'+urllib.parse.urlencode({'symbol':ticker,'requestType':1,'startTime':start,'endTime':end,'timeframe':'day'})
        return _parse_sisejson(get_text(url,accept='text/plain, */*'),target_rows)
    best=[];best_name='';errors=[];minimum=min(target_rows,120)
    for route,fetch in [('NAVER_CHART',chart),('NAVER_FCHART_XML',xml),('NAVER_SISEJSON',sisejson)]:
        try:
            rows=fetch()
            # A source returning future or impossible calendar dates is refused.
            if any(date.fromisoformat(r['date'])>today for r in rows):raise RuntimeError('Future history date')
            if len(rows)>len(best):best,best_name=rows,route
            if len(rows)>=minimum:best,best_name=rows,route;break
            errors.append(f'{route}: only {len(rows)} valid bars')
        except (RuntimeError,ValueError,OSError,ET.ParseError) as exc:
            errors.append(f'{route}: {type(exc).__name__}: {str(exc)[:180]}')
    if not best:raise RuntimeError('NAVER historical sources unavailable: '+' | '.join(errors))
    observed=datetime.now(TZ).isoformat()
    for row in best:
        row.update({'price_history_route':best_name,'observed_at_kst':observed,
                    'shared_collector_version':VERSION,'session_verification':'UNVERIFIED',
                    'adjustment_verification':'UNVERIFIED',
                    'trading_value_status':'SOURCE_FIELD_PRESENT_UNIT_UNVERIFIED' if row.get('trading_value') is not None else 'NOT_PROVIDED_BY_HISTORY_ROUTE'})
    return best


def v1_prior_history(price_rows,value_rows,asof):
    """Prior dates only. Monetary values require numeric source + identical OHLCV.

    This is field-level parity, not independent venue/adjustment certification.
    Missing or mismatched monetary data stays unavailable for the V1 gate.
    """
    day=date.fromisoformat(asof).isoformat()
    other={};seen=set()
    for row in value_rows:
        ds=date.fromisoformat(row['date'][:4]+'-'+row['date'][4:6]+'-'+row['date'][6:8]).isoformat() if re.fullmatch(r'\d{8}',row['date']) else date.fromisoformat(row['date']).isoformat()
        if ds>=day:continue
        if ds in other:raise RuntimeError('Duplicate monetary-value date')
        other[ds]=row
    out=[];matched=missing=mismatched=0
    for raw in price_rows:
        ds=date.fromisoformat(raw['date']).isoformat()
        if ds>=day:continue
        if ds in seen:raise RuntimeError('Duplicate NAVER history date')
        seen.add(ds);r=dict(raw);r['date']=ds.replace('-','');candidate=other.get(ds)
        value=candidate.get('trading_value') if candidate else None
        finite=lambda x:isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)
        parity=candidate is not None and all(finite(r.get(k)) and finite(candidate.get(k)) and r[k]==candidate[k] for k in ('open','high','low','close','volume'))
        observed=finite(value) and value>=0
        native=r.get('trading_value')
        value_agrees=native is None or (finite(native) and native==value)
        if parity and observed and value_agrees:
            r['trading_value']=value;r['trading_value_status']='OBSERVED_NUMERIC_FIELD_OHLCV_MATCHED';r['trading_value_source']='DAUM_DAILY';matched+=1
        else:
            r['trading_value']=None;r['trading_value_status']='DATA_NOT_AVAILABLE'
            if candidate and (not parity or not value_agrees):mismatched+=1
            else:missing+=1
        out.append(r)
    out.sort(key=lambda r:r['date'])
    return out,{'source':VERSION,'price_bars':len(out),'actual_value_matched_bars':matched,'value_missing_bars':missing,'value_mismatch_bars':mismatched,'venue_independently_verified':False,'adjustment_independently_verified':False}


def check_v1_capture(audit):
    """A V2 post-close research audit never authorizes a V1 regular capture."""
    problems=[]
    for key,value in [('current_day_trade_type','KRX'),('after_market_contamination',False),('same_day_generic_integrated_price_used',False),('trading_value_estimated_close_x_volume',False)]:
        matches=(audit.get(key) is value) if isinstance(value,bool) else (audit.get(key)==value)
        if not matches:problems.append(key)
    try:
        start=datetime.fromisoformat(audit['capture_started_kst']);end=datetime.fromisoformat(audit['capture_finished_kst'])
        if start.tzinfo is None or end.tzinfo is None:raise ValueError('timezone missing')
        start,end=start.astimezone(TZ),end.astimezone(TZ)
        expected=str(audit['asof_date']).replace('-','')
        if start.strftime('%Y%m%d')!=expected or end.date()!=start.date() or not start<=end:problems.append('capture_date')
        if not '15:30:30'<=start.strftime('%H:%M:%S')<='15:39:30' or end.strftime('%H:%M:%S')>='15:40:00':problems.append('capture_window')
    except (KeyError,TypeError,ValueError):problems.append('capture_timestamps')
    return {'status':'PASS_CAPTURE_CONTRACT' if not problems else 'BLOCKED_V1_CAPTURE_CONTRACT','blocking_fields':problems,'official_prediction_allowed':False,'orders_sent':0,'other_v1_quality_gates_still_required':True}

CODE_KEYS=("itemCode","itemcode","stockCode","symbolCode","code","isuSrtCd","ISU_SRT_CD")
UNIVERSE_ENDPOINT="https://stock.naver.com/api/domestic/market/stock/default"


def normalize_code(v: Any) -> str | None:
    s = str(v or "").strip().upper()
    if re.fullmatch(r"A[0-9A-Z]{6}", s):
        s = s[1:]
    return s if re.fullmatch(r"[0-9A-Z]{6}", s) else None


def code_from_dict(d: dict[str, Any]) -> str | None:
    for key in CODE_KEYS:
        c = normalize_code(d.get(key))
        if c:
            return c
    return None


def record_dicts(obj: Any) -> Iterable[dict[str, Any]]:
    for _, d in recursive_dicts(obj):
        if code_from_dict(d):
            yield d


def parse_universe_record(d: dict[str, Any]) -> dict[str, Any] | None:
    ticker = code_from_dict(d)
    if not ticker:
        return None

    return {
        "ticker": ticker,
        "company": str(
            scalar_first(
                d,
                (
                    "stockName", "itemName", "itemname",
                    "name", "korName",
                ),
            )
            or ""
        ).strip(),
        "market": str(
            scalar_first(
                d,
                ("marketType", "market", "sosok", "marketName"),
            )
            or ""
        ).upper(),
        "security_type": str(
            scalar_first(
                d,
                ("type", "securityType", "stockType", "itemType"),
            )
            or ""
        ).upper(),
        "market_cap": normalize_number(
            scalar_first(d, ("marketCap", "marketSum", "marketValue"))
        ),
        "foreign_hold_ratio": normalize_number(
            scalar_first(
                d,
                ("foreignHoldRatio", "foreignRate", "frgnRate"),
            )
        ),
        "status_tag": scalar_first(
            d, ("statusTag", "marketStatus", "status")
        ),
        "trade_stop": scalar_first(
            d, ("tradeStopYn", "tradeStop", "halted")
        ),
        "source": "NAVER",
        "source_endpoint": "/api/domestic/market/stock/default",
    }


def fetch_naver_universe(max_pages=80,*,get_json=None):
    get_json=get_json or request_json
    if not 1<=max_pages<=80:raise ValueError("Unbounded universe pagination")
    out: dict[str, dict[str, Any]] = {}
    page_size = 100

    for page in range(max_pages):
        query = urllib.parse.urlencode(
            {
                "tradeType": "KRX",
                "marketType": "ALL",
                "orderType": "marketSum",
                "startIdx": page,
                "pageSize": page_size,
            }
        )
        payload = get_json(UNIVERSE_ENDPOINT + "?" + query)

        found: dict[str, dict[str, Any]] = {}
        for d in record_dicts(payload):
            row = parse_universe_record(d)
            if row:
                found[row["ticker"]] = row

        if not found:
            break

        new_count = 0
        for ticker, row in found.items():
            if ticker not in out:
                new_count += 1
            out[ticker] = row

        if page > 0 and new_count == 0:
            break

        time.sleep(0.08)

    rows = list(out.values())
    rows.sort(key=lambda r: r["ticker"])
    return rows

def v1_universe_from_capture(capture):
    """Reuse the frozen NAVER universe metadata; no later price is fetched."""
    rows=[]
    for ticker,record in sorted(capture.items()):
        market={'0':'KOSPI','1':'KOSDAQ','KOSPI':'KOSPI','KOSDAQ':'KOSDAQ'}.get(str(record.get('market','')).upper())
        if normalize_code(ticker) and ticker.isdigit() and market:
            rows.append({'ticker':ticker,'company':record.get('company',''),'market':market,'universe_source':'NAVER_FROZEN_CAPTURE'})
    return rows

def sha256_file(path):
    import hashlib
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def numeric_source_field(value):
    if value is None or isinstance(value,(bool,dict,list,tuple)):return None
    try:x=float(str(value).replace(',','').strip())
    except (ValueError,TypeError):return None
    return x if math.isfinite(x) else None

def fetch_daum_prior(ticker,asof,count=140,*,get_json=None,headers=None):
    """Existing V1 whole OHLCV+actual-value series; no NAVER field transplant."""
    datetime.strptime(asof,'%Y%m%d')
    if not re.fullmatch(r'[0-9A-Z]{6}',ticker) or not 1<=count<=500:raise ValueError('Invalid history request')
    symbol='A'+ticker
    query=urllib.parse.urlencode({'symbolCode':symbol,'page':1,'perPage':count,'pagination':'true'})
    h={**DEFAULT_HEADERS,**(headers or {}),'Referer':f'https://finance.daum.net/quotes/{symbol}'}
    payload=(get_json or (lambda url,headers:request_json(url,headers)))(f'https://finance.daum.net/api/quote/{symbol}/days?{query}',h)
    if not isinstance(payload,dict) or not isinstance(payload.get('data'),list):raise RuntimeError('Invalid DAUM history schema')
    out=[];seen=set()
    for raw in payload['data']:
        day=str(raw.get('date') or '')[:10].replace('-','')
        if not re.fullmatch(r'\d{8}',day) or day>=asof:continue
        datetime.strptime(day,'%Y%m%d')
        if day in seen:raise RuntimeError('Duplicate DAUM history date')
        seen.add(day)
        row={'date':day}
        for target,key in [('open','openingPrice'),('high','highPrice'),('low','lowPrice'),('close','tradePrice'),('volume','accTradeVolume'),('trading_value','accTradePrice')]:row[target]=numeric_source_field(raw.get(key))
        out.append(row)
    out.sort(key=lambda r:r['date'])
    return out

def v1_profile_history(naver_rows,original_v1_rows,asof):
    """Preserve the existing V1 provider's entire series when vendor bases differ.

    NAVER/DAUM equality is diagnostic. A mismatch never transfers DAUM monetary
    value to a NAVER bar and never replaces an existing V1 model input.
    """
    _,parity=v1_prior_history(naver_rows,original_v1_rows,asof)
    cutoff=date.fromisoformat(asof).strftime('%Y%m%d');out=[];seen=set()
    for raw in original_v1_rows:
        day=str(raw['date']).replace('-','')
        datetime.strptime(day,'%Y%m%d')
        if day>=cutoff:continue
        if day in seen:raise RuntimeError('Duplicate V1 native history date')
        seen.add(day)
        row={'date':day}
        for k in ('open','high','low','close','volume','trading_value'):row[k]=numeric_source_field(raw.get(k))
        out.append(row)
    out.sort(key=lambda r:r['date'])
    audit={**parity,'selected_profile':'V1_EXISTING_DAUM_SERIES','selected_native_rows':len(out),
           'selected_actual_value_rows':sum(r['trading_value'] is not None for r in out),
           'cross_vendor_merge_performed':False,'naver_comparison_probed':bool(naver_rows)}
    return out,audit
