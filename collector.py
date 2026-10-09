#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Stock Prediction Forward Test — Model V1.0
FREEWEB-1.2-CONTROLLED collector

Purpose
-------
Prospective paper-trading data collection only. No real orders.

Hard integrity principles
-------------------------
1) Prediction-date reference data must be KRX regular-session specific.
2) A scheduled run may be accepted only when the current-day KRX snapshot is
   frozen after regular close (15:30 KST) and before after-hours executions
   begin changing post-close volume/value fields (15:40 KST).
3) Historical website-facing bars are filtered to date < as-of; the current
   day is merged only from the frozen KRX-specific capture.
4) No missing-value imputation. No close*volume trading-value estimation.
5) ETF/ETN/management/halt exclusions are hard-gated.
6) At least one stock-specific NON-PRICE group (Flow OR Fundamental/Quality OR
   Earnings/Growth) must reach >=70% coverage across the core eligible universe.
   Otherwise the pipeline is NOT prediction-ready.
7) Every accepted dataset has a schema version and SHA256 manifest.

This is an operational/data-integrity revision only. V1.0 model weights and
ranking logic are NOT changed here.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import re
import statistics
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Seoul")
OUT = Path("output")
DATA = Path("data")
OUT.mkdir(parents=True, exist_ok=True)

COLLECTOR_REVISION = "FREEWEB-1.2.2-CONTROLLED"
SCHEMA_VERSION = "V10-DATA-1.2"

# Frozen V1.0 screening thresholds
MIN_PRICE = 1_000
MIN_MEDIAN_VALUE20 = 3_000_000_000
MIN_NORMAL20 = 18
MIN_HISTORY_BARS = 120
MIN_SNAPSHOT_ROWS = 1_000
MIN_SECTOR_COVERAGE = 0.70
MIN_FEATURE_COVERAGE = 0.70
MAX_HISTORY_ERROR_RATIO = 0.05
MAX_HISTORY_ERRORS_ABS = 50
MAX_WORKERS = 6

# Safe freeze window: regular session has closed, but post-close executions
# have not yet started. We intentionally fail rather than guess.
SAFE_CAPTURE_START = dt_time(15, 30, 30)
SAFE_CAPTURE_END = dt_time(15, 39, 30)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)
NAVER_HEADERS = {
    "User-Agent": UA,
    "Referer": "https://stock.naver.com/",
    "Accept": "application/json, text/plain, */*",
}
DAUM_HEADERS = {
    "User-Agent": UA,
    "Referer": "https://finance.daum.net/",
    "Accept": "application/json, text/plain, */*",
}


def now_kst() -> datetime:
    return datetime.now(TZ)


def nnum(v):
    if v is None or isinstance(v, (dict, list, tuple)):
        return None
    s = str(v).replace(",", "").replace("%", "").strip()
    if s in {"", "-", "null", "None", "N/A", "DATA NOT AVAILABLE"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def scalar_first(d: dict, *keys):
    for k in keys:
        if k in d:
            v = d.get(k)
            if not isinstance(v, (dict, list, tuple)) and v not in (None, ""):
                return v
    return None


def normalize_code(v):
    s = str(v or "").strip().upper()
    if s.startswith("A") and len(s) == 7:
        s = s[1:]
    return s if re.fullmatch(r"[0-9A-Z]{6}", s) else None


def code_from_dict(d: dict):
    for k in ("itemCode", "itemcode", "stockCode", "symbolCode", "code", "isuSrtCd", "ISU_SRT_CD"):
        c = normalize_code(d.get(k))
        if c:
            return c
    return None


def record_dicts(obj):
    """Yield dict objects that themselves carry a recognizable security code."""
    if isinstance(obj, dict):
        if code_from_dict(obj):
            yield obj
        for v in obj.values():
            yield from record_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from record_dicts(v)


def http_bytes(url, headers=None, timeout=30, tries=4):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read(), (r.headers.get_content_charset() or "")
        except Exception as e:
            last = e
            if i + 1 < tries:
                time.sleep(1.0 + i * 1.4 + random.random() * 0.4)
    raise RuntimeError(f"HTTP failed: {url} :: {last}")


def http_text(url, headers=None, timeout=30):
    b, charset = http_bytes(url, headers, timeout)
    for enc in (charset, "utf-8", "euc-kr", "cp949"):
        if not enc:
            continue
        try:
            return b.decode(enc)
        except Exception:
            pass
    return b.decode("utf-8", errors="replace")


def http_json(url, headers=None, timeout=30):
    return json.loads(http_text(url, headers, timeout))


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def sha256_file(path: Path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def valid_prior_run(asof):
    p = DATA / f"pipeline_audit_{asof}.json"
    if not p.exists():
        return False
    try:
        x = json.loads(p.read_text(encoding="utf-8"))
        return (
            x.get("pass") is True
            and x.get("collector_revision") == COLLECTOR_REVISION
            and x.get("asof_date") == asof
        )
    except Exception:
        return False


# ---------------------------------------------------------------------------
# KRX-specific regular-session freeze via Naver market endpoint
# ---------------------------------------------------------------------------

def parse_current_record(d):
    c = code_from_dict(d)
    if not c:
        return None

    return {
        "ticker": c,
        "company": str(scalar_first(d, "stockName", "itemName", "itemname", "name", "korName") or "").strip(),
        "market": str(scalar_first(d, "marketType", "market", "sosok", "marketName") or "").upper(),
        "close": nnum(scalar_first(d, "closePrice", "tradePrice", "currentPrice", "nowVal", "price")),
        "open": nnum(scalar_first(d, "openPrice", "openingPrice", "open")),
        "high": nnum(scalar_first(d, "highPrice", "high")),
        "low": nnum(scalar_first(d, "lowPrice", "low")),
        "volume": nnum(scalar_first(
            d, "accumulatedTradingVolume", "accTradeVolume", "tradingVolume",
            "accQuant", "volume", "quant"
        )),
        "trading_value": nnum(scalar_first(
            d, "accumulatedTradingValue", "accTradePrice", "tradingValue",
            "accAmount", "amount", "tradingAmount"
        )),
        "market_cap": nnum(scalar_first(d, "marketCap", "marketSum", "marketValue")),
        "foreign_hold_ratio": nnum(scalar_first(d, "foreignHoldRatio", "foreignRate", "frgnRate")),
        "foreign_net_buy": nnum(scalar_first(d, "foreignPureBuy", "foreignNetBuy", "foreignerNetBuy")),
        "institution_net_buy": nnum(scalar_first(d, "organizationPureBuy", "institutionNetBuy", "orgNetBuy")),
        "per": nnum(scalar_first(d, "per", "PER")),
        "pbr": nnum(scalar_first(d, "pbr", "PBR")),
        "eps": nnum(scalar_first(d, "eps", "EPS")),
        "roe": nnum(scalar_first(d, "roe", "ROE")),
        "security_type": str(scalar_first(d, "type", "securityType", "stockType", "itemType") or "").upper(),
        "raw_market_status": str(scalar_first(d, "marketStatus", "marketState", "status") or ""),
    }


def naver_krx_regular_snapshot():
    """
    Fetch KRX-only market list using tradeType=KRX.
    startIdx is treated as a zero-based PAGE INDEX, not a row offset.
    """
    out = {}
    page_size = 100
    for page in range(0, 80):
        q = urllib.parse.urlencode({
            "tradeType": "KRX",
            "marketType": "ALL",
            "orderType": "marketSum",
            "startIdx": page,
            "pageSize": page_size,
        })
        url = "https://stock.naver.com/api/domestic/market/stock/default?" + q
        obj = http_json(url, NAVER_HEADERS)
        recs = {}
        for d in record_dicts(obj):
            r = parse_current_record(d)
            if r:
                recs[r["ticker"]] = r

        new = 0
        for c, r in recs.items():
            if c not in out:
                new += 1
            out[c] = r

        if not recs:
            break
        if page > 0 and new == 0:
            break
        time.sleep(0.06)

    return out


def naver_investor_flow_map(asof):
    """
    KRX investor-trend daily rows captured in the same safe window.
    startIdx is treated as a zero-based page index. Field names are parsed
    conservatively; unrecognized/missing values remain unavailable.
    """
    out = {}
    page_size = 100
    for page in range(0, 80):
        q = urllib.parse.urlencode({
            "tradeType": "KRX",
            "marketType": "ALL",
            "bizdate": asof,
            "startIdx": page,
            "pageSize": page_size,
        })
        url = "https://stock.naver.com/api/domestic/market/trend/daily?" + q
        obj = http_json(url, NAVER_HEADERS)
        recs = {}
        for d in record_dicts(obj):
            c = code_from_dict(d)
            if not c:
                continue
            foreign = nnum(scalar_first(
                d, "foreignPureBuy", "foreignNetBuy", "foreignerNetBuy",
                "foreignerPureBuy", "foreignNetPurchase", "foreignerNetPurchase"
            ))
            institution = nnum(scalar_first(
                d, "organizationPureBuy", "organizationNetBuy",
                "institutionNetBuy", "institutionPureBuy", "orgNetBuy"
            ))
            if foreign is not None or institution is not None:
                recs[c] = {
                    "foreign_net_buy": foreign,
                    "institution_net_buy": institution,
                }

        new = 0
        for c, r in recs.items():
            if c not in out:
                new += 1
            out[c] = r

        if not recs:
            break
        if page > 0 and new == 0:
            break
        time.sleep(0.05)

    return out


# ---------------------------------------------------------------------------
# Static-ish universe and sector enrichment (price fields ignored)
# ---------------------------------------------------------------------------

def daum_market_universe(market):
    rows, page, total_pages = [], 1, 1
    while page <= total_pages:
        q = urllib.parse.urlencode({
            "page": page, "perPage": 100, "fieldName": "marketCap",
            "order": "desc", "market": market, "pagination": "true",
        })
        obj = http_json(
            "https://finance.daum.net/api/trend/market_capitalization?" + q,
            DAUM_HEADERS,
        )
        data = obj.get("data") or []
        total_pages = int(obj.get("totalPages") or 1)
        for x in data:
            c = normalize_code(x.get("symbolCode"))
            if c and c.isdigit():  # listed common-equity universe is numeric-coded
                rows.append({
                    "ticker": c,
                    "company": str(x.get("name") or "").strip(),
                    "market": market,
                })
        if not data:
            break
        page += 1
        time.sleep(0.05)
    return rows


def daum_sector_map(market):
    q = urllib.parse.urlencode({
        "fieldName": "", "order": "", "perPage": 100, "market": market, "page": 1,
        "changes": "UPPER_LIMIT,RISE,EVEN,FALL,LOWER_LIMIT",
    })
    obj = http_json("https://finance.daum.net/api/quotes/sectors?" + q, DAUM_HEADERS)
    out = {}
    for cat in obj.get("data") or []:
        sector = str(cat.get("name") or cat.get("sectorName") or "").strip()
        for x in cat.get("includedStocks") or []:
            c = normalize_code(x.get("symbolCode"))
            if c and c.isdigit() and sector:
                out.setdefault(c, sector)
    return out


# ---------------------------------------------------------------------------
# Historical bars: current date is deliberately excluded
# ---------------------------------------------------------------------------

def daum_daily_prior(ticker, asof, count=140):
    sym = "A" + ticker
    q = urllib.parse.urlencode({
        "symbolCode": sym, "page": 1, "perPage": count, "pagination": "true",
    })
    h = dict(DAUM_HEADERS)
    h["Referer"] = f"https://finance.daum.net/quotes/{sym}"
    obj = http_json(f"https://finance.daum.net/api/quote/{sym}/days?{q}", h)
    out = []
    for x in obj.get("data") or []:
        ds = str(x.get("date") or "")[:10].replace("-", "")
        if not re.fullmatch(r"\d{8}", ds) or ds >= asof:
            continue
        out.append({
            "date": ds,
            "open": nnum(x.get("openingPrice")),
            "high": nnum(x.get("highPrice")),
            "low": nnum(x.get("lowPrice")),
            "close": nnum(x.get("tradePrice")),
            "volume": nnum(x.get("accTradeVolume")),
            "trading_value": nnum(x.get("accTradePrice")),
        })
    out.sort(key=lambda z: z["date"])
    return out


def fetch_one_history(item, asof):
    t = item["ticker"]
    try:
        return t, daum_daily_prior(t, asof, 140), None
    except Exception as e:
        return t, [], str(e)


# ---------------------------------------------------------------------------
# Protection lists: current Naver list semantics + hard source gates
# ---------------------------------------------------------------------------

def codes_from_market_ranking(order_type, max_pages=30):
    codes, ok = set(), False
    for page in range(max_pages):
        q = urllib.parse.urlencode({
            "tradeType": "KRX", "marketType": "ALL",
            "orderType": order_type, "startIdx": page, "pageSize": 100,
        })
        obj = http_json(
            "https://stock.naver.com/api/domestic/market/stock/default?" + q,
            NAVER_HEADERS,
        )
        ok = True
        found = {code_from_dict(d) for d in record_dicts(obj)}
        found.discard(None)
        new = found - codes
        codes |= found
        if not found or (page > 0 and not new):
            break
        time.sleep(0.05)
    return codes, ok


def etn_codes(max_pages=30):
    codes, ok = set(), False
    for page in range(max_pages):
        q = urllib.parse.urlencode({
            "orderType": "AMOUNT_ETN", "startIdx": page, "pageSize": 100,
        })
        obj = http_json("https://stock.naver.com/api/domestic/market/etn?" + q, NAVER_HEADERS)
        ok = True
        found = {code_from_dict(d) for d in record_dicts(obj)}
        found.discard(None)
        new = found - codes
        codes |= found
        if not found or (page > 0 and not new):
            break
        time.sleep(0.05)
    return codes, ok


def etf_snapshot(asof):
    """Current Naver ETF v2 list, captured inside the safe regular-close window."""
    rows = []
    seen = set()
    for page in range(0, 30):
        q = urllib.parse.urlencode({
            "listingType": "tradingValueDesc", "size": 100, "index": page,
        })
        obj = http_json(
            "https://stock.naver.com/api/stockSecurity/etfs/v2/domestic?" + q,
            NAVER_HEADERS,
        )
        found = 0
        for d in record_dicts(obj):
            c = code_from_dict(d)
            if not c or c in seen:
                continue
            seen.add(c)
            found += 1
            rows.append({
                "asof_date": asof,
                "ticker": c,
                "etf_name": str(scalar_first(d, "itemName", "itemname", "name", "stockName") or "").strip(),
                "price": nnum(scalar_first(d, "closePrice", "tradePrice", "nowVal", "price")),
                "nav": nnum(scalar_first(d, "nav", "NAV")),
                "change_rate_pct": nnum(scalar_first(d, "changeRate", "fluctuationRate")),
                "return_1m_pct": nnum(scalar_first(d, "returnRate1m", "oneMonthEarnRate")),
                "return_3m_pct": nnum(scalar_first(d, "returnRate3m", "threeMonthEarnRate")),
                "market_cap": nnum(scalar_first(d, "marketCap", "marketSum", "aum")),
                "source": "NAVER_ETF_V2_SAFE_CAPTURE",
                "ranking_universe": "EXCLUDED_FROM_V1_0_STOCK_RANKING",
            })
        has_next = None
        if isinstance(obj, dict):
            has_next = obj.get("hasNext")
        if found == 0 or has_next is False:
            break
        time.sleep(0.05)
    return rows


# ---------------------------------------------------------------------------
# Benchmark: KOSPI / KOSDAQ index data (regular-session index)
# ---------------------------------------------------------------------------

def index_history(code, asof, page_size=100):
    url = f"https://stock.naver.com/api/securityFe/api/index/{code}/price?page={1}&pageSize={page_size}"
    obj = http_json(url, NAVER_HEADERS)
    rows = []
    if not isinstance(obj, list):
        return rows
    for x in obj:
        ds = str(x.get("localTradedAt") or x.get("date") or "")[:10].replace("-", "")
        if not re.fullmatch(r"\d{8}", ds) or ds > asof:
            continue
        rows.append({
            "date": ds,
            "close": nnum(scalar_first(x, "closePrice", "tradePrice", "close")),
            "open": nnum(scalar_first(x, "openPrice", "openingPrice", "open")),
            "high": nnum(scalar_first(x, "highPrice", "high")),
            "low": nnum(scalar_first(x, "lowPrice", "low")),
        })
    rows.sort(key=lambda z: z["date"])
    return rows


# ---------------------------------------------------------------------------
# Deterministic calculations
# ---------------------------------------------------------------------------

def pct_return(bars, h):
    if len(bars) <= h:
        return None
    a, b = bars[-1-h].get("close"), bars[-1].get("close")
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0


def ma_gap(bars, n):
    vals = [b.get("close") for b in bars[-n:] if b.get("close") not in (None, 0)]
    if len(vals) < n or not bars[-1].get("close"):
        return None
    m = statistics.mean(vals)
    return bars[-1]["close"] / m - 1.0 if m else None


def rv20(bars):
    if len(bars) < 21:
        return None
    rs = []
    for i in range(len(bars)-20, len(bars)):
        p0, p1 = bars[i-1].get("close"), bars[i].get("close")
        if p0 and p1 and p0 > 0 and p1 > 0:
            rs.append(math.log(p1 / p0))
    return statistics.stdev(rs) if len(rs) >= 15 else None


def median20_value(bars):
    vals = [b.get("trading_value") for b in bars[-20:] if b.get("trading_value") is not None]
    return statistics.median(vals) if len(vals) >= 18 else None


def volume_ratio20(bars):
    if len(bars) < 20 or bars[-1].get("volume") is None:
        return None
    prior = [b.get("volume") for b in bars[-20:-1] if b.get("volume") not in (None, 0)]
    if len(prior) < 15:
        return None
    m = statistics.mean(prior)
    return bars[-1]["volume"] / m if m else None


def mdd60(bars):
    closes = [b.get("close") for b in bars[-60:] if b.get("close") not in (None, 0)]
    if len(closes) < 40:
        return None
    peak = closes[0]
    dd = 0.0
    for x in closes:
        peak = max(peak, x)
        dd = min(dd, x / peak - 1.0)
    return dd


def max_gap20(bars):
    if len(bars) < 21:
        return None
    vals = []
    for i in range(len(bars)-20, len(bars)):
        prev = bars[i-1].get("close")
        opn = bars[i].get("open")
        if prev and opn:
            vals.append(abs(opn / prev - 1.0))
    return max(vals) if vals else None


def coverage(rows, predicate):
    return (
        sum(1 for r in rows if predicate(r)) / len(rows)
        if rows else 0.0
    )


def excluded_name(company):
    n = (company or "").strip()
    u = n.upper()
    if "스팩" in n or "SPAC" in u:
        return "SPAC"
    if any(k in u for k in ("REIT", "FUND")) or any(k in n for k in ("리츠", "투자회사", "부동산투자")):
        return "REIT_OR_FUND"
    if re.search(r"(우|우B|우C|우선주)$", n, re.I):
        return "PREFERRED_SHARE"
    return None


def write_csv(path, rows):
    if not rows:
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def fail_audit(asof, error, extra=None, exit_code=2):
    audit = {
        "pass": False,
        "status": "FAIL — DATA PIPELINE",
        "collector_revision": COLLECTOR_REVISION,
        "schema_version": SCHEMA_VERSION,
        "asof_date": asof,
        "error": error,
        "collected_at_kst": now_kst().isoformat(),
    }
    if extra:
        audit.update(extra)
    write_json(OUT / f"pipeline_audit_{asof}.json", audit)
    write_json(OUT / "latest_status.json", audit)
    return exit_code


# ---------------------------------------------------------------------------
# Diagnostic mode: safe to run at any time, NEVER prediction-eligible
# ---------------------------------------------------------------------------

def run_diagnostic(asof):
    result = {
        "diagnostic": True,
        "prediction_eligible": False,
        "collector_revision": COLLECTOR_REVISION,
        "schema_version": SCHEMA_VERSION,
        "asof_date": asof,
        "timestamp_kst": now_kst().isoformat(),
        "checks": {},
    }
    probes = [
        ("naver_benchmark", lambda: index_history("KOSPI", asof, 5)),
        ("naver_investor_flow", lambda: naver_investor_flow_map(asof)),
        ("naver_etn", lambda: etn_codes(1)),
        ("naver_management", lambda: codes_from_market_ranking("statusTag", 1)),
        ("naver_halt", lambda: codes_from_market_ranking("tradeStopYn", 1)),
        ("naver_etf", lambda: etf_snapshot(asof)[:5]),
        ("daum_universe", lambda: daum_market_universe("KOSPI")[:5]),
    ]
    for name, fn in probes:
        try:
            val = fn()
            result["checks"][name] = {"ok": True, "sample_count": len(val[0]) if isinstance(val, tuple) and hasattr(val[0], "__len__") else (len(val) if hasattr(val, "__len__") else None)}
        except Exception as e:
            result["checks"][name] = {"ok": False, "error": str(e)}
    result["warnings"] = []
    flow_check = result["checks"].get("naver_investor_flow", {})
    if flow_check.get("ok") and flow_check.get("sample_count") == 0:
        result["warnings"].append(
            "Investor-flow endpoint responded but produced zero stock-coded rows in diagnostic parsing. "
            "Do not treat this source as proven stock-level flow; scheduled feature coverage must decide activation."
        )
    result["all_source_probes_ok"] = all(x.get("ok") for x in result["checks"].values())
    write_json(OUT / f"diagnostic_{asof}_{now_kst().strftime('%H%M%S')}.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["all_source_probes_ok"] else 2


# ---------------------------------------------------------------------------
# Scheduled controlled run
# ---------------------------------------------------------------------------

def run_scheduled(asof):
    if valid_prior_run(asof):
        write_json(OUT / f"skip_status_{asof}_{now_kst().strftime('%H%M%S')}.json", {
            "status": "SKIP — VALID CONTROLLED RUN ALREADY EXISTS",
            "asof_date": asof,
            "collector_revision": COLLECTOR_REVISION,
            "timestamp_kst": now_kst().isoformat(),
        })
        return 0

        # Wait until the KRX regular-session capture window opens.
    # Do not collect or freeze any market data before 15:30:30 KST.
    while now_kst().time() < SAFE_CAPTURE_START:
        remaining = (
            datetime.combine(now_kst().date(), SAFE_CAPTURE_START, tzinfo=TZ)
            - now_kst()
        ).total_seconds()
        time.sleep(min(10.0, max(0.1, remaining)))

    start = now_kst()
    if not (SAFE_CAPTURE_START <= start.time() <= SAFE_CAPTURE_END):
        return fail_audit(asof, "UNSAFE_CAPTURE_TIME", {
            "capture_started_kst": start.isoformat(),
            "required_window_kst": "15:30:30–15:39:30",
            "after_market_contamination": "UNVERIFIED",
        })

    # Freeze all same-day values FIRST, before doing slower history work.
    try:
        # Time-sensitive KRX regular-session fields are frozen first.
        krx_current = naver_krx_regular_snapshot()
        flow_map = naver_investor_flow_map(asof)
        kospi_idx = index_history("KOSPI", asof, 100)
        kosdaq_idx = index_history("KOSDAQ", asof, 100)
    except Exception as e:
        return fail_audit(asof, "SAFE_CAPTURE_SOURCE_FAILED", {
            "capture_started_kst": start.isoformat(),
            "detail": str(e),
            "after_market_contamination": "UNVERIFIED",
        })

    end_capture = now_kst()
    if end_capture.time() >= dt_time(15, 40, 0):
        return fail_audit(asof, "SAFE_CAPTURE_FINISHED_TOO_LATE", {
            "capture_started_kst": start.isoformat(),
            "capture_finished_kst": end_capture.isoformat(),
            "after_market_contamination": "UNVERIFIED",
        })

    # Merge frozen KRX investor flow without guessing missing fields.
    for c, fr in flow_map.items():
        if c in krx_current:
            if fr.get("foreign_net_buy") is not None:
                krx_current[c]["foreign_net_buy"] = fr["foreign_net_buy"]
            if fr.get("institution_net_buy") is not None:
                krx_current[c]["institution_net_buy"] = fr["institution_net_buy"]

    # Less time-sensitive context/exclusion sources may be collected after the
    # time-sensitive regular-session snapshot has been frozen.
    try:
        etf_rows = etf_snapshot(asof)
        management, management_ok = codes_from_market_ranking("statusTag")
        halt, halt_ok = codes_from_market_ranking("tradeStopYn")
        etn, etn_ok = etn_codes()
    except Exception as e:
        return fail_audit(asof, "PROTECTION_OR_ETF_SOURCE_FAILED", {
            "capture_started_kst": start.isoformat(),
            "capture_finished_kst": end_capture.isoformat(),
            "detail": str(e),
        })

    # Trading-day gate
    if not kospi_idx or not kosdaq_idx or kospi_idx[-1]["date"] != asof or kosdaq_idx[-1]["date"] != asof:
        audit = {
            "pass": False,
            "status": "SKIP — NON_TRADING_DAY_OR_STALE_SOURCE",
            "collector_revision": COLLECTOR_REVISION,
            "schema_version": SCHEMA_VERSION,
            "asof_date": asof,
            "capture_started_kst": start.isoformat(),
            "capture_finished_kst": end_capture.isoformat(),
            "kospi_latest": kospi_idx[-1]["date"] if kospi_idx else None,
            "kosdaq_latest": kosdaq_idx[-1]["date"] if kosdaq_idx else None,
        }
        write_json(OUT / f"pipeline_audit_{asof}.json", audit)
        write_json(OUT / "latest_status.json", audit)
        return 0

    etf_codes_set = {r["ticker"] for r in etf_rows if r.get("ticker")}
    protection_gate = (
        management_ok and halt_ok and etn_ok
        and len(etf_codes_set) >= 100
        and len(etn) > 0
        and (len(management) + len(halt)) > 0
    )
    if not protection_gate:
        return fail_audit(asof, "PROTECTION_LIST_INTEGRITY_FAIL", {
            "capture_started_kst": start.isoformat(),
            "capture_finished_kst": end_capture.isoformat(),
            "krx_current_rows": len(krx_current),
            "management_codes": len(management),
            "halt_codes": len(halt),
            "etf_codes": len(etf_codes_set),
            "etn_codes": len(etn),
            "management_source_ok": management_ok,
            "halt_source_ok": halt_ok,
            "etn_source_ok": etn_ok,
        })

    # Persist frozen regular-session capture immediately.
    capture_rows = []
    for c, r in sorted(krx_current.items()):
        capture_rows.append({
            "asof_date": asof,
            "capture_timestamp_kst": end_capture.isoformat(),
            **r,
            "source": "NAVER_KRX_MARKET_LIST_SAFE_CAPTURE",
            "trade_type": "KRX",
        })
    write_csv(OUT / f"regular_close_capture_{asof}.csv", capture_rows)

    # Universe + sector can be enriched after the freeze because no same-day
    # price/volume/value from these sources is used.
    try:
        universe = daum_market_universe("KOSPI") + daum_market_universe("KOSDAQ")
        universe = list({x["ticker"]: x for x in universe}.values())
        sectors = {}
        sectors.update(daum_sector_map("KOSPI"))
        sectors.update(daum_sector_map("KOSDAQ"))
    except Exception as e:
        return fail_audit(asof, "UNIVERSE_OR_SECTOR_FAILED", {
            "capture_started_kst": start.isoformat(),
            "capture_finished_kst": end_capture.isoformat(),
            "detail": str(e),
        })

    if len(universe) < MIN_SNAPSHOT_ROWS:
        return fail_audit(asof, f"UNIVERSE_TOO_SMALL:{len(universe)}")

    # Slow history work occurs AFTER current-day values have been frozen.
    history, errors = {}, {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fs = {ex.submit(fetch_one_history, x, asof): x["ticker"] for x in universe}
        done = 0
        for fut in as_completed(fs):
            t, bars, err = fut.result()
            history[t] = bars
            if err:
                errors[t] = err
            done += 1
            if done % 250 == 0:
                print(f"history progress: {done}/{len(universe)}", flush=True)

    rows = []
    for item in universe:
        t = item["ticker"]
        cur = krx_current.get(t, {})
        prior = history.get(t, [])
        bars = list(prior)

        # Merge ONLY frozen KRX current day.
        if cur.get("close") is not None:
            bars.append({
                "date": asof,
                "open": cur.get("open"),
                "high": cur.get("high"),
                "low": cur.get("low"),
                "close": cur.get("close"),
                "volume": cur.get("volume"),
                "trading_value": cur.get("trading_value"),
            })

        company = item.get("company") or cur.get("company") or ""
        sector = sectors.get(t)
        reasons = []

        if t in etf_codes_set:
            reasons.append("ETF")
        if t in etn:
            reasons.append("ETN")
        name_ex = excluded_name(company)
        if name_ex:
            reasons.append(name_ex)
        st = (cur.get("security_type") or "").upper()
        if st in {"EF", "ETF", "ETN", "PREFERRED", "PREF"}:
            reasons.append("NON_COMMON_SECURITY_TYPE")
        if t in management:
            reasons.append("MANAGEMENT")
        if t in halt:
            reasons.append("TRADING_HALT")

        price = cur.get("close")
        med20 = median20_value(bars)
        normal20 = sum(
            1 for b in bars[-20:]
            if (b.get("close") or 0) > 0 and (b.get("volume") or 0) > 0
        )

        if price is None or price < MIN_PRICE:
            reasons.append("PRICE_LT_1000_OR_MISSING")
        if len(bars) < MIN_HISTORY_BARS:
            reasons.append("LISTING_OR_HISTORY_LT_120_TRADING_DAYS")
        if med20 is None or med20 < MIN_MEDIAN_VALUE20:
            reasons.append("MEDIAN_ACTUAL_VALUE20_LT_3B_OR_MISSING")
        if normal20 < MIN_NORMAL20:
            reasons.append("NORMAL20_LT_18")
        if not sector:
            reasons.append("SECTOR_MISSING")
        if cur.get("volume") is None or cur.get("trading_value") is None:
            reasons.append("CURRENT_KRX_VOLUME_VALUE_MISSING")

        row = {
            "asof_date": asof,
            "ticker": t,
            "company": company or "DATA NOT AVAILABLE",
            "market": item.get("market") or cur.get("market") or "DATA NOT AVAILABLE",
            "sector": sector or "DATA NOT AVAILABLE",
            "reference_price": price if price is not None else "DATA NOT AVAILABLE",
            "open": cur.get("open") if cur.get("open") is not None else "DATA NOT AVAILABLE",
            "high": cur.get("high") if cur.get("high") is not None else "DATA NOT AVAILABLE",
            "low": cur.get("low") if cur.get("low") is not None else "DATA NOT AVAILABLE",
            "volume": cur.get("volume") if cur.get("volume") is not None else "DATA NOT AVAILABLE",
            "actual_trading_value": cur.get("trading_value") if cur.get("trading_value") is not None else "DATA NOT AVAILABLE",
            "history_bars": len(bars),
            "median_actual_trading_value_20d": med20 if med20 is not None else "DATA NOT AVAILABLE",
            "normal_trading_days_20d": normal20,
            "return_1d": pct_return(bars, 1) if len(bars) >= 2 else "DATA NOT AVAILABLE",
            "return_5d": pct_return(bars, 5) if len(bars) >= 6 else "DATA NOT AVAILABLE",
            "return_20d": pct_return(bars, 20) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "return_60d": pct_return(bars, 60) if len(bars) >= 61 else "DATA NOT AVAILABLE",
            "ma_gap_5d": ma_gap(bars, 5) if len(bars) >= 5 else "DATA NOT AVAILABLE",
            "ma_gap_20d": ma_gap(bars, 20) if len(bars) >= 20 else "DATA NOT AVAILABLE",
            "ma_gap_60d": ma_gap(bars, 60) if len(bars) >= 60 else "DATA NOT AVAILABLE",
            "volume_ratio_20d": volume_ratio20(bars) if len(bars) >= 20 else "DATA NOT AVAILABLE",
            "realized_vol_20d": rv20(bars) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "mdd_60d": mdd60(bars) if len(bars) >= 60 else "DATA NOT AVAILABLE",
            "max_abs_gap_20d": max_gap20(bars) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "foreign_hold_ratio": cur.get("foreign_hold_ratio") if cur.get("foreign_hold_ratio") is not None else "DATA NOT AVAILABLE",
            "foreign_net_buy": cur.get("foreign_net_buy") if cur.get("foreign_net_buy") is not None else "DATA NOT AVAILABLE",
            "institution_net_buy": cur.get("institution_net_buy") if cur.get("institution_net_buy") is not None else "DATA NOT AVAILABLE",
            "per": cur.get("per") if cur.get("per") is not None else "DATA NOT AVAILABLE",
            "pbr": cur.get("pbr") if cur.get("pbr") is not None else "DATA NOT AVAILABLE",
            "eps": cur.get("eps") if cur.get("eps") is not None else "DATA NOT AVAILABLE",
            "roe": cur.get("roe") if cur.get("roe") is not None else "DATA NOT AVAILABLE",
            "management_flag": "YES" if t in management else "NO",
            "trading_halt_flag": "YES" if t in halt else "NO",
            "etf_flag": "YES" if t in etf_codes_set else "NO",
            "etn_flag": "YES" if t in etn else "NO",
            "security_type_raw": st or "DATA NOT AVAILABLE",
            "core_eligible": "YES" if not reasons else "NO",
            "eligible": "YES" if not reasons else "NO",
            "exclusion_reason": "|".join(dict.fromkeys(reasons)),
            "current_day_source": "NAVER_KRX_SAFE_CAPTURE",
            "history_source": "DAUM_PRIOR_DATES_ONLY",
            "trading_value_method": "ACTUAL_SOURCE_FIELD_ONLY",
        }
        rows.append(row)

    # Coverage is measured on core-eligible rows before factor activation.
    core = [r for r in rows if r["core_eligible"] == "YES"]
    feature_cov = {
        "price_momentum": coverage(core, lambda r: isinstance(r["return_5d"], (int, float)) and isinstance(r["return_20d"], (int, float)) and isinstance(r["return_60d"], (int, float))),
        "volume_liquidity": coverage(core, lambda r: isinstance(r["median_actual_trading_value_20d"], (int, float)) and isinstance(r["volume_ratio_20d"], (int, float))),
        "volatility_trend": coverage(core, lambda r: isinstance(r["realized_vol_20d"], (int, float)) and isinstance(r["ma_gap_20d"], (int, float))),
        "sector": coverage(core, lambda r: r["sector"] != "DATA NOT AVAILABLE"),
        "flow": coverage(core, lambda r: isinstance(r["foreign_net_buy"], (int, float)) or isinstance(r["institution_net_buy"], (int, float))),
        "fundamental_quality": coverage(core, lambda r: any(isinstance(r[k], (int, float)) for k in ("per", "pbr", "eps", "roe"))),
        "earnings_growth": 0.0,
        "short_credit": 0.0,
    }
    feature_cov = {k: round(v, 6) for k, v in feature_cov.items()}
    active_features = [k for k, v in feature_cov.items() if v >= MIN_FEATURE_COVERAGE]
    non_price_stock_groups = [k for k in ("flow", "fundamental_quality", "earnings_growth") if feature_cov[k] >= MIN_FEATURE_COVERAGE]
    non_price_group_ok = bool(non_price_stock_groups)

    # Benchmark summary
    benchmark_rows = []
    benchmark_gate = True
    for code, series in (("KOSPI", kospi_idx), ("KOSDAQ", kosdaq_idx)):
        if not series or series[-1]["date"] != asof or series[-1].get("close") is None:
            benchmark_gate = False
            continue
        benchmark_rows.append({
            "asof_date": asof,
            "benchmark": code,
            "close": series[-1]["close"],
            "return_5d": pct_return(series, 5) if len(series) >= 6 else "DATA NOT AVAILABLE",
            "return_20d": pct_return(series, 20) if len(series) >= 21 else "DATA NOT AVAILABLE",
            "return_60d": pct_return(series, 60) if len(series) >= 61 else "DATA NOT AVAILABLE",
            "vol_20d": rv20(series) if len(series) >= 21 else "DATA NOT AVAILABLE",
            "source": "NAVER_INDEX_REGULAR_SESSION",
        })

    # ETF summary
    etf_summary = {
        "asof_date": asof,
        "etf_count": len(etf_rows),
        "usage": "REGIME_SECTOR_THEME_CONTEXT_ONLY",
        "included_in_v1_stock_ranking": False,
    }
    vals = [r["return_3m_pct"] for r in etf_rows if isinstance(r.get("return_3m_pct"), (int, float))]
    etf_summary["avg_3m_return_pct"] = round(statistics.mean(vals), 6) if vals else None

    # Write data files before final audit.
    universe_path = OUT / f"universe_snapshot_{asof}.csv"
    etf_path = OUT / f"etf_snapshot_{asof}.csv"
    bench_path = OUT / f"benchmark_snapshot_{asof}.csv"
    feat_path = OUT / f"feature_coverage_{asof}.json"
    etf_sum_path = OUT / f"etf_regime_summary_{asof}.json"

    write_csv(universe_path, rows)
    write_csv(etf_path, etf_rows)
    write_csv(bench_path, benchmark_rows)
    write_json(feat_path, {
        "asof_date": asof,
        "collector_revision": COLLECTOR_REVISION,
        "coverage": feature_cov,
        "active_feature_groups_at_70pct": active_features,
        "active_stock_specific_non_price_groups": non_price_stock_groups,
        "non_price_group_ok": non_price_group_ok,
        "coverage_threshold": MIN_FEATURE_COVERAGE,
    })
    write_json(etf_sum_path, etf_summary)

    duplicate = len(rows) - len({r["ticker"] for r in rows})
    sector_cov = coverage(rows, lambda r: r["sector"] != "DATA NOT AVAILABLE")
    history_cov = coverage(rows, lambda r: int(r["history_bars"]) >= MIN_HISTORY_BARS)
    value_cov = coverage(rows, lambda r: isinstance(r["median_actual_trading_value_20d"], (int, float)))
    current_close_cov = coverage(rows, lambda r: isinstance(r["reference_price"], (int, float)))
    max_errors = max(MAX_HISTORY_ERRORS_ABS, int(len(rows) * MAX_HISTORY_ERROR_RATIO))

    after_market_contamination = False  # proven by capture timing + KRX-only source
    pass_all = (
        len(rows) >= MIN_SNAPSHOT_ROWS
        and duplicate == 0
        and sector_cov >= MIN_SECTOR_COVERAGE
        and history_cov >= 0.70
        and value_cov >= 0.70
        and current_close_cov >= 0.70
        and len(errors) <= max_errors
        and protection_gate
        and benchmark_gate
        and non_price_group_ok
        and after_market_contamination is False
    )

    audit = {
        "pass": bool(pass_all),
        "status": "PASS — CONTROLLED SNAPSHOT READY" if pass_all else "FAIL — DATA PIPELINE",
        "collector_revision": COLLECTOR_REVISION,
        "schema_version": SCHEMA_VERSION,
        "asof_date": asof,
        "data_cutoff_kst": "18:00",
        "capture_started_kst": start.isoformat(),
        "capture_finished_kst": end_capture.isoformat(),
        "safe_capture_window_kst": "15:30:30–15:39:30",
        "current_day_trade_type": "KRX",
        "after_market_contamination": after_market_contamination,
        "snapshot_rows": len(rows),
        "core_eligible_rows": len(core),
        "duplicate_ticker": duplicate,
        "sector_coverage": round(sector_cov, 6),
        "history_120bar_coverage": round(history_cov, 6),
        "actual_trading_value_20d_coverage": round(value_cov, 6),
        "current_krx_close_coverage": round(current_close_cov, 6),
        "history_fetch_errors": len(errors),
        "history_error_limit": max_errors,
        "management_codes": len(management),
        "halt_codes": len(halt),
        "etf_codes": len(etf_codes_set),
        "etn_codes": len(etn),
        "protection_gate": protection_gate,
        "benchmark_gate": benchmark_gate,
        "benchmark_rows": len(benchmark_rows),
        "investor_flow_capture_rows": len(flow_map),
        "feature_coverage": feature_cov,
        "active_feature_groups_at_70pct": active_features,
        "active_stock_specific_non_price_groups": non_price_stock_groups,
        "non_price_group_ok": non_price_group_ok,
        "no_imputation": True,
        "trading_value_estimated_close_x_volume": False,
        "same_day_generic_integrated_price_used": False,
        "etf_in_stock_ranking": False,
        "collected_at_kst": now_kst().isoformat(),
    }

    if not non_price_group_ok:
        audit["blocking_reason"] = "INSUFFICIENT NON-PRICE FACTOR COVERAGE"
    elif not benchmark_gate:
        audit["blocking_reason"] = "BENCHMARK DATA INCOMPLETE"
    elif not protection_gate:
        audit["blocking_reason"] = "PROTECTION LIST INTEGRITY FAIL"

    audit_path = OUT / f"pipeline_audit_{asof}.json"
    write_json(audit_path, audit)

    # Manifest hashes immutable payload files (latest_status intentionally excluded).
    manifest_targets = [
        OUT / f"regular_close_capture_{asof}.csv",
        universe_path, etf_path, bench_path, feat_path, etf_sum_path, audit_path,
    ]
    manifest = {
        "asof_date": asof,
        "collector_revision": COLLECTOR_REVISION,
        "schema_version": SCHEMA_VERSION,
        "files": {
            p.name: {"sha256": sha256_file(p), "bytes": p.stat().st_size}
            for p in manifest_targets if p.exists()
        },
    }
    manifest_path = OUT / f"manifest_{asof}.json"
    write_json(manifest_path, manifest)

    latest = dict(audit)
    latest["manifest_file"] = manifest_path.name
    latest["manifest_file_count"] = len(manifest["files"])
    latest["manifest_present"] = True
    write_json(OUT / "latest_status.json", latest)

    print(json.dumps(latest, ensure_ascii=False, indent=2))
    return 0 if pass_all else 2


def self_test():
    bars = []
    for i in range(1, 131):
        bars.append({
            "date": f"202601{i:02d}" if i <= 31 else str(20260000 + i),
            "open": 99.0 + i,
            "high": 101.0 + i,
            "low": 98.0 + i,
            "close": 100.0 + i,
            "volume": 1_000_000 + i * 100,
            "trading_value": 5_000_000_000 + i * 1_000_000,
        })
    assert pct_return(bars, 5) is not None
    assert ma_gap(bars, 20) is not None
    assert rv20(bars) is not None
    assert median20_value(bars) >= MIN_MEDIAN_VALUE20
    assert volume_ratio20(bars) is not None
    assert mdd60(bars) is not None
    assert max_gap20(bars) is not None
    assert normalize_code("A005930") == "005930"
    assert normalize_code("0193W0") == "0193W0"
    print("SELF_TEST PASS")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["scheduled", "diagnostic"], default="diagnostic")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        return self_test()

    asof = now_kst().strftime("%Y%m%d")
    if args.mode == "diagnostic":
        return run_diagnostic(asof)
    return run_scheduled(asof)


if __name__ == "__main__":
    raise SystemExit(main())
