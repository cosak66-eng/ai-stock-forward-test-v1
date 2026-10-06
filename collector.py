#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Stock Prediction Forward Test V1.0
Cloud market-data collector for GitHub Actions.

Purpose
-------
- Paper-trading / forward-test data collection only.
- No real order placement.
- Strict no-look-ahead:
  * Data cut-off = 18:00 KST.
  * Same-day KRX EOD data is NOT used for the prediction snapshot.
  * Current-day regular-session quote/volume/value comes from Naver's regular
    KRX-session fields only; NXT/after-hours fields are ignored.
  * KRX Open API history is limited to trading dates strictly BEFORE as-of date.

Outputs
-------
output/universe_snapshot_YYYYMMDD.csv
output/pipeline_audit_YYYYMMDD.json

Required environment variable
-----------------------------
KRX_AUTH_KEY
"""

from __future__ import annotations

import csv
import html
import json
import math
import os
import re
import statistics
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Seoul")
OUT = Path("output")
OUT.mkdir(parents=True, exist_ok=True)

# V1.0 frozen screening thresholds
MIN_PRICE = 1_000
MIN_MEDIAN_VALUE20 = 3_000_000_000
MIN_NORMAL20 = 18
MIN_HISTORY_BARS = 61
MIN_SNAPSHOT_ROWS = 1_000
MIN_SECTOR_COVERAGE = 0.70

KRX = {
    "KOSPI_DAILY": "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd",
    "KOSDAQ_DAILY": "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd",
    "KOSPI_BASE": "https://data-dbg.krx.co.kr/svc/apis/sto/stk_isu_base_info",
    "KOSDAQ_BASE": "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info",
}

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)

def now_kst() -> datetime:
    return datetime.now(TZ)

def nnum(v):
    if v is None:
        return None
    s = str(v).replace(",", "").replace("%", "").strip()
    if s in {"", "-", "null", "None"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

def first(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def http_bytes(url: str, headers: dict | None = None, timeout: int = 25, tries: int = 3) -> tuple[bytes, str]:
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        h.update(headers)
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
                ctype = r.headers.get_content_charset() or ""
                return data, ctype
        except Exception as e:
            last = e
            if i + 1 < tries:
                time.sleep(1.0 + i * 1.5)
    raise RuntimeError(f"HTTP failed: {url} :: {last}")

def http_text(url: str, headers: dict | None = None, timeout: int = 25) -> str:
    b, charset = http_bytes(url, headers, timeout)
    candidates = [charset, "utf-8", "euc-kr", "cp949"]
    for enc in candidates:
        if not enc:
            continue
        try:
            return b.decode(enc)
        except Exception:
            pass
    return b.decode("utf-8", errors="replace")

def http_json(url: str, headers: dict | None = None, timeout: int = 25):
    return json.loads(http_text(url, headers, timeout))

def krx_get(url: str, bas_dd: str, auth_key: str) -> list[dict]:
    q = urllib.parse.urlencode({"basDd": bas_dd})
    obj = http_json(f"{url}?{q}", headers={"AUTH_KEY": auth_key})
    rows = obj.get("OutBlock_1", [])
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected KRX response: {type(rows)}")
    return rows

def naver_index_dates(code: str = "KOSPI", pages: int = 5, page_size: int = 60) -> list[str]:
    dates = []
    for page in range(1, pages + 1):
        url = (
            f"https://m.stock.naver.com/api/index/{code}/price"
            f"?pageSize={page_size}&page={page}"
        )
        obj = http_json(url)
        if not isinstance(obj, list):
            break
        for x in obj:
            d = str(x.get("localTradedAt", ""))[:10].replace("-", "")
            if re.fullmatch(r"\d{8}", d):
                dates.append(d)
        if len(obj) < page_size:
            break
        time.sleep(0.15)
    return sorted(set(dates), reverse=True)

def naver_quotes(tickers: list[str], batch: int = 35) -> dict[str, dict]:
    out = {}
    for i in range(0, len(tickers), batch):
        part = tickers[i:i+batch]
        query = "|".join(f"SERVICE_ITEM:{x}" for x in part)
        url = (
            "https://polling.finance.naver.com/api/realtime?query="
            + urllib.parse.quote(query, safe=":|")
        )
        obj = http_json(url)
        result = obj.get("result", {}) if isinstance(obj, dict) else {}
        for area in result.get("areas", []) or []:
            for d in area.get("datas", []) or []:
                code = str(d.get("cd", "")).strip()
                if re.fullmatch(r"\d{6}", code):
                    out[code] = {
                        "ticker": code,
                        "name": d.get("nm"),
                        "close": nnum(d.get("nv")),
                        "open": nnum(d.get("ov")),
                        "high": nnum(d.get("hv")),
                        "low": nnum(d.get("lv")),
                        "volume": nnum(d.get("aq")),
                        "trading_value": nnum(d.get("aa")),
                        "market_status": d.get("ms"),
                    }
        time.sleep(0.10)
    return out

def recursive_codes(obj) -> set[str]:
    found = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in {"itemCode", "code", "cd"}:
                s = str(v)
                if re.fullmatch(r"\d{6}", s):
                    found.add(s)
            found |= recursive_codes(v)
    elif isinstance(obj, list):
        for v in obj:
            found |= recursive_codes(v)
    return found

def naver_management_codes() -> set[str]:
    out = set()
    for market in ("KOSPI", "KOSDAQ"):
        try:
            url = (
                "https://m.stock.naver.com/front-api/stock/domestic/stockList"
                f"?sortType=management&category={market}&page=1&pageSize=500"
            )
            out |= recursive_codes(http_json(url))
        except Exception:
            pass
    return out

def naver_halt_codes(max_pages: int = 10) -> set[str]:
    # Public Naver Finance trading-halt page. Read-only.
    out = set()
    for page in range(1, max_pages + 1):
        try:
            txt = http_text(f"https://finance.naver.com/sise/trading_halt.naver?page={page}")
        except Exception:
            break
        codes = set(re.findall(r"(?:code=|code%3D)(\d{6})", txt))
        new = codes - out
        out |= codes
        if page > 1 and not new:
            break
        time.sleep(0.10)
    return out

def strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def naver_sector_map() -> dict[str, str]:
    """
    Naver PC 업종 pages -> ticker: sector.
    This is a secondary public source because KRX Open API base-info endpoint
    does not expose a stable sector taxonomy suitable for the V1.0 cross-section.
    """
    base = "https://finance.naver.com"
    txt = http_text(base + "/sise/sise_group.naver?type=upjong")

    # Capture sector number and anchor label.
    pairs = []
    pat = re.compile(
        r'href=["\'](?:/)?sise/sise_group_detail\.naver\?type=upjong&amp;no=(\d+)["\'][^>]*>(.*?)</a>',
        re.I | re.S,
    )
    for no, label in pat.findall(txt):
        name = strip_tags(label)
        if name:
            pairs.append((no, name))

    # Fallback for unescaped & in HTML.
    if not pairs:
        pat2 = re.compile(
            r'href=["\'](?:/)?sise/sise_group_detail\.naver\?type=upjong&no=(\d+)["\'][^>]*>(.*?)</a>',
            re.I | re.S,
        )
        for no, label in pat2.findall(txt):
            name = strip_tags(label)
            if name:
                pairs.append((no, name))

    # Deduplicate while preserving order.
    seen = set()
    uniq = []
    for no, name in pairs:
        if no not in seen:
            uniq.append((no, name))
            seen.add(no)

    out = {}
    for no, sector in uniq:
        try:
            detail = http_text(
                base + f"/sise/sise_group_detail.naver?type=upjong&no={no}"
            )
            codes = re.findall(r"/item/main\.naver\?code=(\d{6})", detail)
            for code in codes:
                out.setdefault(code, sector)
        except Exception:
            continue
        time.sleep(0.08)
    return out

def parse_base(rows: list[dict], market: str) -> list[dict]:
    out = []
    for x in rows:
        ticker = str(first(x, "ISU_SRT_CD", "ISU_CD", default="")).strip()
        if len(ticker) > 6 and ticker[-6:].isdigit():
            ticker = ticker[-6:]
        if not re.fullmatch(r"\d{6}", ticker):
            continue
        out.append({
            "ticker": ticker,
            "company": str(first(x, "ISU_ABBRV", "ISU_NM", "ISU_ENG_NM", default="")).strip(),
            "market": market,
            "listing_date": str(first(x, "LIST_DD", "LIST_DATE", default="")).replace("-", ""),
            "security_group": str(first(x, "SECUGRP_NM", "SECURITY_GROUP", default="")).strip(),
            "stock_kind": str(first(x, "KIND_STKCERT_TP_NM", "STKCERT_TP_NM", default="")).strip(),
        })
    return out

def parse_daily(rows: list[dict], market: str, date: str) -> dict[str, dict]:
    out = {}
    for x in rows:
        ticker = str(first(x, "ISU_SRT_CD", "ISU_CD", default="")).strip()
        if len(ticker) > 6 and ticker[-6:].isdigit():
            ticker = ticker[-6:]
        if not re.fullmatch(r"\d{6}", ticker):
            continue
        out[ticker] = {
            "date": date,
            "market": market,
            "close": nnum(first(x, "TDD_CLSPRC", "CLSPRC")),
            "open": nnum(first(x, "TDD_OPNPRC", "OPNPRC")),
            "high": nnum(first(x, "TDD_HGPRC", "HGPRC")),
            "low": nnum(first(x, "TDD_LWPRC", "LWPRC")),
            "volume": nnum(first(x, "ACC_TRDVOL", "TRDVOL")),
            "trading_value": nnum(first(x, "ACC_TRDVAL", "TRDVAL")),
        }
    return out

def pct_return(bars: list[dict], h: int):
    if len(bars) <= h:
        return None
    a = bars[-1-h].get("close")
    b = bars[-1].get("close")
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0

def rv20(bars: list[dict]):
    if len(bars) < 21:
        return None
    rs = []
    for i in range(len(bars)-20, len(bars)):
        p0 = bars[i-1].get("close")
        p1 = bars[i].get("close")
        if p0 and p1 and p0 > 0 and p1 > 0:
            rs.append(math.log(p1 / p0))
    if len(rs) < 15:
        return None
    return statistics.stdev(rs) if len(rs) >= 2 else 0.0

def median20_value(bars: list[dict]):
    vals = [
        b.get("trading_value") for b in bars[-20:]
        if b.get("trading_value") is not None and b.get("trading_value") >= 0
    ]
    if len(vals) < 18:
        return None
    return statistics.median(vals)

def listing_trading_days(listing_date: str, market_dates_desc: list[str], asof: str):
    if not re.fullmatch(r"\d{8}", listing_date or ""):
        return None, False
    dates = [d for d in market_dates_desc if d <= asof]
    if not dates:
        return None, False
    oldest = min(dates)
    if listing_date < oldest and len(dates) >= 120:
        # Exact count is unnecessary once we know it exceeds the threshold.
        return 120, True
    c = sum(1 for d in dates if listing_date <= d <= asof)
    return c, c >= 120

def excluded_security(base: dict) -> tuple[bool, str]:
    name = base.get("company", "")
    sg = base.get("security_group", "")
    kind = base.get("stock_kind", "")

    # Common stock rule: if official kind is available, it must indicate common.
    if kind and ("보통" not in kind and "COMMON" not in kind.upper()):
        return True, f"NON_COMMON:{kind}"

    text = f"{name} {sg}".upper()
    banned = ["ETF", "ETN", "SPAC", "스팩", "리츠", "REIT", "펀드", "FUND", "투자회사"]
    if any(k.upper() in text for k in banned):
        return True, "EXCLUDED_SECURITY_TYPE"
    return False, ""

def write_audit(asof: str, payload: dict):
    p = OUT / f"pipeline_audit_{asof}.json"
    payload = dict(payload)
    payload.setdefault("asof_date", asof)
    payload.setdefault("collected_at_kst", now_kst().isoformat())
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p

def main():
    auth_key = os.environ.get("KRX_AUTH_KEY", "").strip()
    asof = now_kst().strftime("%Y%m%d")

    if not auth_key:
        write_audit(asof, {
            "pass": False,
            "status": "FAIL — DATA PIPELINE",
            "error": "KRX_AUTH_KEY_NOT_SET",
        })
        print("ERROR: KRX_AUTH_KEY is not set.", file=sys.stderr)
        return 2

    # Trading calendar from public Naver index history.
    try:
        market_dates = naver_index_dates("KOSPI", pages=5, page_size=60)
    except Exception as e:
        write_audit(asof, {
            "pass": False,
            "status": "FAIL — DATA PIPELINE",
            "error": f"NAVER_INDEX_DATES_FAILED: {e}",
        })
        return 2

    latest_market_date = market_dates[0] if market_dates else None
    if latest_market_date != asof:
        # Weekday holiday or non-trading day. This is not a collector failure.
        write_audit(asof, {
            "pass": False,
            "status": "SKIP — NON_TRADING_DAY",
            "latest_market_date": latest_market_date,
        })
        print(f"SKIP: {asof} is not the latest KRX trading date.")
        return 0

    prior_dates = [d for d in market_dates if d < asof]
    if len(prior_dates) < 60:
        write_audit(asof, {
            "pass": False,
            "status": "FAIL — DATA PIPELINE",
            "error": f"TRADING_CALENDAR_LT_60:{len(prior_dates)}",
        })
        return 2

    # IMPORTANT: base/history date is prior trading day. Same-day official KRX
    # EOD files are deliberately not consumed because V1.0 cutoff is 18:00 KST.
    base_date = prior_dates[0]

    try:
        kospi_base = krx_get(KRX["KOSPI_BASE"], base_date, auth_key)
        kosdaq_base = krx_get(KRX["KOSDAQ_BASE"], base_date, auth_key)
        universe = parse_base(kospi_base, "KOSPI") + parse_base(kosdaq_base, "KOSDAQ")
    except Exception as e:
        write_audit(asof, {
            "pass": False,
            "status": "FAIL — DATA PIPELINE",
            "error": f"KRX_BASE_FAILED:{e}",
            "base_date": base_date,
        })
        return 2

    # Frozen universe from prior official base-info plus today's regular-session quotes.
    tickers = sorted(set(x["ticker"] for x in universe))

    try:
        quotes = naver_quotes(tickers)
    except Exception as e:
        write_audit(asof, {
            "pass": False,
            "status": "FAIL — DATA PIPELINE",
            "error": f"NAVER_QUOTES_FAILED:{e}",
            "base_date": base_date,
        })
        return 2

    # History: latest 65 completed KRX trading days STRICTLY BEFORE as-of date.
    # 65 is enough for 60D momentum plus stability margin.
    hist_dates = sorted(prior_dates[:65])
    hist = {t: [] for t in tickers}

    for idx, d in enumerate(hist_dates, 1):
        try:
            k1 = parse_daily(krx_get(KRX["KOSPI_DAILY"], d, auth_key), "KOSPI", d)
            k2 = parse_daily(krx_get(KRX["KOSDAQ_DAILY"], d, auth_key), "KOSDAQ", d)
        except Exception as e:
            write_audit(asof, {
                "pass": False,
                "status": "FAIL — DATA PIPELINE",
                "error": f"KRX_HISTORY_FAILED:{d}:{e}",
                "base_date": base_date,
                "history_dates_completed": idx - 1,
            })
            return 2
        merged = {}
        merged.update(k1)
        merged.update(k2)
        for t, bar in merged.items():
            if t in hist:
                hist[t].append(bar)
        time.sleep(0.05)

    # Secondary exclusion/status/sector sources.
    management = naver_management_codes()
    halt = naver_halt_codes()
    try:
        sectors = naver_sector_map()
    except Exception:
        sectors = {}

    by_ticker = {}
    for b in universe:
        by_ticker[b["ticker"]] = b

    rows = []
    for t in tickers:
        b = by_ticker[t]
        q = quotes.get(t)

        bars = sorted(hist.get(t, []), key=lambda x: x["date"])
        if q and q.get("close") is not None:
            bars.append({
                "date": asof,
                "market": b["market"],
                "close": q.get("close"),
                "open": q.get("open"),
                "high": q.get("high"),
                "low": q.get("low"),
                "volume": q.get("volume"),
                "trading_value": q.get("trading_value"),
            })

        med20 = median20_value(bars)
        normal20 = sum(
            1 for x in bars[-20:]
            if (x.get("close") or 0) > 0 and (x.get("volume") or 0) > 0
        )
        ltd, listing_ok = listing_trading_days(
            b.get("listing_date", ""), market_dates, asof
        )
        type_excl, type_reason = excluded_security(b)

        price = q.get("close") if q else None
        sector = sectors.get(t)
        quote_ok = (
            q is not None
            and q.get("close") is not None
            and q.get("volume") is not None
            and q.get("trading_value") is not None
        )

        reasons = []
        if type_excl:
            reasons.append(type_reason)
        if t in management:
            reasons.append("MANAGEMENT")
        if t in halt:
            reasons.append("TRADING_HALT")
        if not listing_ok:
            reasons.append("LISTING_LT_120_TRADING_DAYS_OR_UNKNOWN")
        if price is None or price < MIN_PRICE:
            reasons.append("PRICE_LT_1000_OR_MISSING")
        if med20 is None or med20 < MIN_MEDIAN_VALUE20:
            reasons.append("MEDIAN_VALUE20_LT_3B_OR_MISSING")
        if normal20 < MIN_NORMAL20:
            reasons.append("NORMAL20_LT_18")
        if len(bars) < MIN_HISTORY_BARS:
            reasons.append("HISTORY_LT_61")
        if not quote_ok:
            reasons.append("CURRENT_QUOTE_REQUIRED_FIELDS_MISSING")
        if not sector:
            reasons.append("SECTOR_MISSING")

        eligible = len(reasons) == 0

        rows.append({
            "asof_date": asof,
            "ticker": t,
            "company": b.get("company"),
            "market": b.get("market"),
            "sector": sector or "DATA NOT AVAILABLE",
            "reference_price": price if price is not None else "DATA NOT AVAILABLE",
            "open": q.get("open") if q and q.get("open") is not None else "DATA NOT AVAILABLE",
            "high": q.get("high") if q and q.get("high") is not None else "DATA NOT AVAILABLE",
            "low": q.get("low") if q and q.get("low") is not None else "DATA NOT AVAILABLE",
            "volume": q.get("volume") if q and q.get("volume") is not None else "DATA NOT AVAILABLE",
            "trading_value": q.get("trading_value") if q and q.get("trading_value") is not None else "DATA NOT AVAILABLE",
            "market_status": q.get("market_status") if q else "DATA NOT AVAILABLE",
            "listing_date": b.get("listing_date") or "DATA NOT AVAILABLE",
            "listing_trading_days_threshold": ltd if ltd is not None else "DATA NOT AVAILABLE",
            "security_group": b.get("security_group") or "DATA NOT AVAILABLE",
            "stock_kind": b.get("stock_kind") or "DATA NOT AVAILABLE",
            "history_bars": len(bars),
            "median_trading_value_20d": med20 if med20 is not None else "DATA NOT AVAILABLE",
            "normal_trading_days_20d": normal20,
            "return_5d": pct_return(bars, 5) if len(bars) >= 6 else "DATA NOT AVAILABLE",
            "return_20d": pct_return(bars, 20) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "return_60d": pct_return(bars, 60) if len(bars) >= 61 else "DATA NOT AVAILABLE",
            "realized_vol_20d": rv20(bars) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "management_flag": "YES" if t in management else "NO",
            "trading_halt_flag": "YES" if t in halt else "NO",
            "eligible": "YES" if eligible else "NO",
            "exclusion_reason": "|".join(reasons),
            "price_source": "NAVER_REGULAR_KRX_SESSION",
            "history_source": "KRX_OPEN_API_PRIOR_TRADING_DAYS_ONLY",
        })

    # Snapshot write
    csv_path = OUT / f"universe_snapshot_{asof}.csv"
    fields = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    dup = len(rows) - len(set(r["ticker"] for r in rows))
    sector_cov = (
        sum(1 for r in rows if r["sector"] != "DATA NOT AVAILABLE") / len(rows)
        if rows else 0.0
    )
    eligible_count = sum(1 for r in rows if r["eligible"] == "YES")
    quote_count = sum(
        1 for r in rows if r["reference_price"] != "DATA NOT AVAILABLE"
    )

    passed = (
        len(rows) >= MIN_SNAPSHOT_ROWS
        and dup == 0
        and len(hist_dates) >= 60
        and quote_count >= MIN_SNAPSHOT_ROWS
        and sector_cov >= MIN_SECTOR_COVERAGE
    )

    audit = {
        "pass": bool(passed),
        "status": "PASS — CLOUD SNAPSHOT READY" if passed else "FAIL — DATA PIPELINE",
        "asof_date": asof,
        "data_cutoff_kst": "18:00",
        "collector_schedule_target_kst": "18:20",
        "base_info_date": base_date,
        "history_max_date": max(hist_dates) if hist_dates else None,
        "same_day_krx_eod_used": False,
        "snapshot_rows": len(rows),
        "eligible_rows": eligible_count,
        "current_quote_rows": quote_count,
        "duplicate_ticker": dup,
        "history_trading_dates": len(hist_dates) + 1,  # + current regular-session bar
        "sector_coverage": round(sector_cov, 6),
        "management_codes_detected": len(management),
        "halt_codes_detected": len(halt),
        "sources": {
            "official_history": "KRX Open API",
            "current_regular_session": "Naver Finance public quote endpoint",
            "sector_management_halt": "Naver Finance public pages/endpoints",
        },
        "integrity_note": (
            "Same-day KRX EOD was excluded because the V1.0 cut-off is 18:00 KST. "
            "NXT/after-hours quote fields are not consumed."
        ),
    }
    write_audit(asof, audit)

    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0 if passed else 2

if __name__ == "__main__":
    raise SystemExit(main())
