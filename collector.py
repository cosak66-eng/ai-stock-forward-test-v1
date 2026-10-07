#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Stock Prediction Forward Test V1.0
FREE WEB COLLECTOR — no KRX Open API key required.

Sources:
- Daum Finance public web JSON:
  universe, sector, OHLCV, actual daily trading value(accTradePrice)
- Naver Finance public pages:
  management, trading halt, ETF, ETN exclusions

No close*volume trading-value estimation is used.
"""

from __future__ import annotations

import csv, json, math, random, re, statistics, sys, time
import urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Seoul")
OUT = Path("output")
OUT.mkdir(parents=True, exist_ok=True)

MIN_PRICE = 1_000
MIN_MEDIAN_VALUE20 = 3_000_000_000
MIN_NORMAL20 = 18
MIN_HISTORY_BARS = 120
MIN_SNAPSHOT_ROWS = 1_000
MIN_SECTOR_COVERAGE = 0.70
MAX_WORKERS = 6

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/140 Safari/537.36"
DAUM_HEADERS = {"User-Agent": UA, "Referer": "https://finance.daum.net/", "Accept": "application/json, text/plain, */*"}
NAVER_HEADERS = {"User-Agent": UA, "Referer": "https://finance.naver.com/"}

def now_kst():
    return datetime.now(TZ)

def nnum(v):
    if v is None:
        return None
    s = str(v).replace(",", "").replace("%", "").strip()
    if s in {"", "-", "null", "None", "N/A"}:
        return None
    try:
        return float(s)
    except Exception:
        return None

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
                time.sleep(1.0 + i * 1.5 + random.random() * 0.7)
    raise RuntimeError(f"HTTP failed: {url} :: {last}")

def http_text(url, headers=None, timeout=30):
    data, charset = http_bytes(url, headers, timeout)
    for enc in [charset, "utf-8", "euc-kr", "cp949"]:
        if not enc:
            continue
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("utf-8", errors="replace")

def http_json(url, headers=None, timeout=30):
    return json.loads(http_text(url, headers, timeout))

def daum_market_universe(market):
    rows, page, total_pages = [], 1, 1
    per_page = 100
    while page <= total_pages:
        q = urllib.parse.urlencode({
            "page": page, "perPage": per_page, "fieldName": "marketCap",
            "order": "desc", "market": market, "pagination": "true"
        })
        obj = http_json("https://finance.daum.net/api/trend/market_capitalization?" + q, DAUM_HEADERS)
        data = obj.get("data") or []
        total_pages = int(obj.get("totalPages") or 1)
        for x in data:
            sym = str(x.get("symbolCode") or "")
            ticker = sym[1:] if sym.startswith("A") else sym
            if re.fullmatch(r"\d{6}", ticker):
                rows.append({
                    "ticker": ticker,
                    "symbol": "A" + ticker,
                    "company": str(x.get("name") or "").strip(),
                    "market": market,
                    "current_price_list": nnum(x.get("tradePrice")),
                    "market_cap": nnum(x.get("marketCap")),
                    "listed_share_count": nnum(x.get("listedShareCount")),
                    "foreign_ratio": nnum(x.get("foreignRatio")),
                })
        if not data:
            break
        page += 1
        time.sleep(0.08)
    return rows

def daum_daily(ticker, count=130):
    sym = "A" + ticker
    q = urllib.parse.urlencode({
        "symbolCode": sym, "page": 1, "perPage": count, "pagination": "true"
    })
    headers = dict(DAUM_HEADERS)
    headers["Referer"] = f"https://finance.daum.net/quotes/{sym}"
    obj = http_json(f"https://finance.daum.net/api/quote/{sym}/days?{q}", headers)
    out = []
    for x in obj.get("data") or []:
        ds = str(x.get("date") or "")[:10].replace("-", "")
        if not re.fullmatch(r"\d{8}", ds):
            continue
        out.append({
            "date": ds,
            "open": nnum(x.get("openingPrice")),
            "high": nnum(x.get("highPrice")),
            "low": nnum(x.get("lowPrice")),
            "close": nnum(x.get("tradePrice")),
            "volume": nnum(x.get("accTradeVolume")),
            "trading_value": nnum(x.get("accTradePrice")),
            "trade_time": str(x.get("tradeTime") or ""),
        })
    out.sort(key=lambda z: z["date"])
    return out

def daum_sector_map(market):
    q = urllib.parse.urlencode({
        "fieldName": "", "order": "", "perPage": 100, "market": market, "page": 1,
        "changes": "UPPER_LIMIT,RISE,EVEN,FALL,LOWER_LIMIT"
    })
    obj = http_json("https://finance.daum.net/api/quotes/sectors?" + q, DAUM_HEADERS)
    out = {}
    for cat in obj.get("data") or []:
        sector = str(cat.get("name") or cat.get("sectorName") or "").strip()
        for x in cat.get("includedStocks") or []:
            sym = str(x.get("symbolCode") or "")
            ticker = sym[1:] if sym.startswith("A") else sym
            if sector and re.fullmatch(r"\d{6}", ticker):
                out.setdefault(ticker, sector)
    return out

def naver_codes(url_template, max_pages=20):
    codes, success = set(), False
    for page in range(1, max_pages + 1):
        try:
            txt = http_text(url_template.format(page=page), NAVER_HEADERS)
            success = True
        except Exception:
            if page == 1:
                return set(), False
            break
        found = set(re.findall(r"(?:code=|code%3D)(\d{6})", txt))
        new = found - codes
        codes |= found
        if page > 1 and not new:
            break
        time.sleep(0.08)
    return codes, success

def protection_lists():
    management, ok_m = naver_codes("https://finance.naver.com/sise/management.naver?page={page}", 20)
    halt, ok_h = naver_codes("https://finance.naver.com/sise/trading_halt.naver?page={page}", 20)
    etf, ok_e = naver_codes("https://finance.naver.com/sise/etf.naver?page={page}", 40)
    etn, ok_n = naver_codes("https://finance.naver.com/sise/etn.naver?page={page}", 40)
    return {
        "management": management, "halt": halt, "etf": etf, "etn": etn,
        "ok": bool(ok_m and ok_h), "etf_ok": ok_e, "etn_ok": ok_n
    }

def looks_non_common(company):
    n = (company or "").strip()
    u = n.upper()
    if "스팩" in n or "SPAC" in u:
        return True, "SPAC"
    if any(k in u for k in ["REIT", "FUND"]) or any(k in n for k in ["리츠", "투자회사", "부동산투자"]):
        return True, "REIT_OR_FUND"
    if re.search(r"(우|우B|우C|우선주)$", n, re.I):
        return True, "PREFERRED_SHARE"
    return False, ""

def pct_return(bars, h):
    if len(bars) <= h:
        return None
    a, b = bars[-1-h].get("close"), bars[-1].get("close")
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0

def rv20(bars):
    if len(bars) < 21:
        return None
    rs = []
    for i in range(len(bars)-20, len(bars)):
        p0, p1 = bars[i-1].get("close"), bars[i].get("close")
        if p0 and p1 and p0 > 0 and p1 > 0:
            rs.append(math.log(p1 / p0))
    if len(rs) < 15:
        return None
    return statistics.stdev(rs) if len(rs) >= 2 else 0.0

def median20_actual_value(bars):
    vals = [b["trading_value"] for b in bars[-20:] if b.get("trading_value") is not None and b.get("trading_value") >= 0]
    if len(vals) < 18:
        return None
    return statistics.median(vals)


ETF_CATEGORY = {
    1: "국내 시장지수",
    2: "국내 업종/테마",
    3: "국내 파생",
    4: "해외 주식",
    5: "원자재",
    6: "채권",
    7: "기타",
}

def naver_etf_snapshot(asof):
    """
    Collect ALL Korean-listed ETFs from Naver Finance public ETF list endpoint.
    ETFs remain excluded from the V1.0 common-stock ranking universe.
    This dataset is for market-regime / sector-theme context only.
    """
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    obj = http_json(url, NAVER_HEADERS)
    items = ((obj or {}).get("result") or {}).get("etfItemList") or []

    rows = []
    for x in items:
        ticker = str(x.get("itemcode") or "").strip()
        if not re.fullmatch(r"\d{6}", ticker):
            continue

        price = nnum(x.get("nowVal"))
        nav = nnum(x.get("nav"))
        premium_discount = None
        if price is not None and nav not in (None, 0):
            premium_discount = (price / nav - 1.0) * 100.0

        cat = x.get("etfTabCode")
        try:
            cat_i = int(cat)
        except Exception:
            cat_i = None

        rows.append({
            "asof_date": asof,
            "ticker": ticker,
            "etf_name": str(x.get("itemname") or "").strip(),
            "category_code": cat_i if cat_i is not None else "DATA NOT AVAILABLE",
            "category": ETF_CATEGORY.get(cat_i, "DATA NOT AVAILABLE"),
            "price": price if price is not None else "DATA NOT AVAILABLE",
            "nav": nav if nav is not None else "DATA NOT AVAILABLE",
            "premium_discount_pct": round(premium_discount, 6) if premium_discount is not None else "DATA NOT AVAILABLE",
            "change_value": nnum(x.get("changeVal")) if nnum(x.get("changeVal")) is not None else "DATA NOT AVAILABLE",
            "change_rate_pct": nnum(x.get("changeRate")) if nnum(x.get("changeRate")) is not None else "DATA NOT AVAILABLE",
            "three_month_return_pct": nnum(x.get("threeMonthEarnRate")) if nnum(x.get("threeMonthEarnRate")) is not None else "DATA NOT AVAILABLE",
            "volume": nnum(x.get("quant")) if nnum(x.get("quant")) is not None else "DATA NOT AVAILABLE",
            # Preserve the original field without guessing its unit.
            "amount_raw_source": nnum(x.get("amonut")) if nnum(x.get("amonut")) is not None else "DATA NOT AVAILABLE",
            "market_cap_100m_krw": nnum(x.get("marketSum")) if nnum(x.get("marketSum")) is not None else "DATA NOT AVAILABLE",
            "rise_fall_code": str(x.get("risefall") or ""),
            "source": "NAVER_FINANCE_ETF_ITEM_LIST",
            "ranking_universe": "EXCLUDED_FROM_V1_0_STOCK_RANKING",
        })

    out_csv = OUT / f"etf_snapshot_{asof}.csv"
    if rows:
        fields = list(rows[0].keys())
        with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    # Simple regime/context summary. No model weight changes.
    by_cat = {}
    for r in rows:
        c = r["category"]
        d = by_cat.setdefault(c, {"count": 0, "up": 0, "down": 0, "avg_change_rate_pct": [], "avg_3m_return_pct": []})
        d["count"] += 1
        cr = r["change_rate_pct"]
        r3 = r["three_month_return_pct"]
        if isinstance(cr, (int, float)):
            if cr > 0:
                d["up"] += 1
            elif cr < 0:
                d["down"] += 1
            d["avg_change_rate_pct"].append(cr)
        if isinstance(r3, (int, float)):
            d["avg_3m_return_pct"].append(r3)

    category_summary = {}
    for c, d in by_cat.items():
        category_summary[c] = {
            "count": d["count"],
            "up": d["up"],
            "down": d["down"],
            "breadth_up_ratio": round(d["up"] / d["count"], 6) if d["count"] else None,
            "avg_change_rate_pct": round(sum(d["avg_change_rate_pct"]) / len(d["avg_change_rate_pct"]), 6) if d["avg_change_rate_pct"] else None,
            "avg_3m_return_pct": round(sum(d["avg_3m_return_pct"]) / len(d["avg_3m_return_pct"]), 6) if d["avg_3m_return_pct"] else None,
        }

    valid_3m = [r for r in rows if isinstance(r["three_month_return_pct"], (int, float))]
    top_3m = sorted(valid_3m, key=lambda r: r["three_month_return_pct"], reverse=True)[:20]
    bottom_3m = sorted(valid_3m, key=lambda r: r["three_month_return_pct"])[:20]

    summary = {
        "asof_date": asof,
        "etf_count": len(rows),
        "category_summary": category_summary,
        "top_20_by_3m_return": [
            {"ticker": r["ticker"], "name": r["etf_name"], "category": r["category"], "return_3m_pct": r["three_month_return_pct"]}
            for r in top_3m
        ],
        "bottom_20_by_3m_return": [
            {"ticker": r["ticker"], "name": r["etf_name"], "category": r["category"], "return_3m_pct": r["three_month_return_pct"]}
            for r in bottom_3m
        ],
        "usage": "MARKET_REGIME_AND_THEME_CONTEXT_ONLY",
        "v1_0_stock_ranking_inclusion": False,
        "source": "NAVER_FINANCE_PUBLIC_ETF_LIST",
    }
    (OUT / f"etf_regime_summary_{asof}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return rows, summary

def write_audit(asof, payload):
    p = OUT / f"pipeline_audit_{asof}.json"
    x = dict(payload)
    x.setdefault("asof_date", asof)
    x.setdefault("collected_at_kst", now_kst().isoformat())
    p.write_text(json.dumps(x, ensure_ascii=False, indent=2), encoding="utf-8")
    return p

def fetch_one(item):
    t = item["ticker"]
    try:
        return t, daum_daily(t, 130), None
    except Exception as e:
        return t, [], str(e)

def main():
    asof = now_kst().strftime("%Y%m%d")

    try:
        universe = daum_market_universe("KOSPI") + daum_market_universe("KOSDAQ")
    except Exception as e:
        write_audit(asof, {"pass": False, "status": "FAIL — DATA PIPELINE", "error": f"DAUM_UNIVERSE_FAILED:{e}"})
        return 2

    universe = list({x["ticker"]: x for x in universe}.values())
    if len(universe) < MIN_SNAPSHOT_ROWS:
        write_audit(asof, {"pass": False, "status": "FAIL — DATA PIPELINE", "error": f"UNIVERSE_TOO_SMALL:{len(universe)}"})
        return 2

    prot = protection_lists()
    if not prot["ok"]:
        write_audit(asof, {"pass": False, "status": "FAIL — DATA PIPELINE", "error": "NAVER_MANAGEMENT_OR_HALT_SOURCE_UNAVAILABLE"})
        return 2

    try:
        sectors = {}
        sectors.update(daum_sector_map("KOSPI"))
        sectors.update(daum_sector_map("KOSDAQ"))
        sector_error = None
    except Exception as e:
        sectors, sector_error = {}, str(e)

    history, errors = {}, {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fs = {ex.submit(fetch_one, x): x["ticker"] for x in universe}
        done = 0
        for fut in as_completed(fs):
            t, bars, err = fut.result()
            history[t] = bars
            if err:
                errors[t] = err
            done += 1
            if done % 250 == 0:
                print(f"history progress: {done}/{len(universe)}", flush=True)

    latest_dates = [bars[-1]["date"] for bars in history.values() if bars]
    latest_mode = statistics.mode(latest_dates) if latest_dates else None

    if latest_mode != asof:
        write_audit(asof, {
            "pass": False,
            "status": "SKIP — NON_TRADING_DAY_OR_STALE_SOURCE",
            "latest_source_date": latest_mode,
            "snapshot_candidate_rows": len(universe),
        })
        return 0


    # ETF dataset is collected separately and does NOT enter V1.0 stock ranking.
    try:
        etf_rows, etf_summary = naver_etf_snapshot(asof)
        etf_error = None
    except Exception as e:
        etf_rows, etf_summary = [], {}
        etf_error = str(e)

    rows = []
    for item in universe:
        t = item["ticker"]
        bars = [b for b in history.get(t, []) if b["date"] <= asof]
        last = bars[-1] if bars else {}
        price = last.get("close") if bars else item.get("current_price_list")
        med20 = median20_actual_value(bars)
        normal20 = sum(1 for b in bars[-20:] if (b.get("close") or 0) > 0 and (b.get("volume") or 0) > 0)

        non_common, nc_reason = looks_non_common(item.get("company", ""))
        reasons = []
        if t in prot["etf"]: reasons.append("ETF")
        if t in prot["etn"]: reasons.append("ETN")
        if non_common: reasons.append(nc_reason)
        if t in prot["management"]: reasons.append("MANAGEMENT")
        if t in prot["halt"]: reasons.append("TRADING_HALT")
        if price is None or price < MIN_PRICE: reasons.append("PRICE_LT_1000_OR_MISSING")
        if len(bars) < MIN_HISTORY_BARS: reasons.append("LISTING_OR_HISTORY_LT_120_TRADING_DAYS")
        if med20 is None or med20 < MIN_MEDIAN_VALUE20: reasons.append("MEDIAN_ACTUAL_VALUE20_LT_3B_OR_MISSING")
        if normal20 < MIN_NORMAL20: reasons.append("NORMAL20_LT_18")

        sector = sectors.get(t)
        if not sector: reasons.append("SECTOR_MISSING")

        rows.append({
            "asof_date": asof,
            "ticker": t,
            "company": item.get("company") or "DATA NOT AVAILABLE",
            "market": item.get("market") or "DATA NOT AVAILABLE",
            "sector": sector or "DATA NOT AVAILABLE",
            "reference_price": price if price is not None else "DATA NOT AVAILABLE",
            "open": last.get("open") if last.get("open") is not None else "DATA NOT AVAILABLE",
            "high": last.get("high") if last.get("high") is not None else "DATA NOT AVAILABLE",
            "low": last.get("low") if last.get("low") is not None else "DATA NOT AVAILABLE",
            "volume": last.get("volume") if last.get("volume") is not None else "DATA NOT AVAILABLE",
            "actual_trading_value": last.get("trading_value") if last.get("trading_value") is not None else "DATA NOT AVAILABLE",
            "market_cap": item.get("market_cap") if item.get("market_cap") is not None else "DATA NOT AVAILABLE",
            "foreign_ratio": item.get("foreign_ratio") if item.get("foreign_ratio") is not None else "DATA NOT AVAILABLE",
            "history_bars": len(bars),
            "median_actual_trading_value_20d": med20 if med20 is not None else "DATA NOT AVAILABLE",
            "normal_trading_days_20d": normal20,
            "return_5d": pct_return(bars, 5) if len(bars) >= 6 else "DATA NOT AVAILABLE",
            "return_20d": pct_return(bars, 20) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "return_60d": pct_return(bars, 60) if len(bars) >= 61 else "DATA NOT AVAILABLE",
            "realized_vol_20d": rv20(bars) if len(bars) >= 21 else "DATA NOT AVAILABLE",
            "management_flag": "YES" if t in prot["management"] else "NO",
            "trading_halt_flag": "YES" if t in prot["halt"] else "NO",
            "etf_flag": "YES" if t in prot["etf"] else "NO",
            "etn_flag": "YES" if t in prot["etn"] else "NO",
            "eligible": "YES" if not reasons else "NO",
            "exclusion_reason": "|".join(dict.fromkeys(reasons)),
            "price_history_source": "DAUM_FINANCE_PUBLIC_WEB",
            "trading_value_method": "ACTUAL_ACC_TRADE_PRICE",
        })

    csv_path = OUT / f"universe_snapshot_{asof}.csv"
    fields = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    duplicate = len(rows) - len(set(r["ticker"] for r in rows))
    sector_cov = sum(1 for r in rows if r["sector"] != "DATA NOT AVAILABLE") / len(rows) if rows else 0.0
    history120_cov = sum(1 for r in rows if int(r["history_bars"]) >= 120) / len(rows) if rows else 0.0
    value20_cov = sum(1 for r in rows if r["median_actual_trading_value_20d"] != "DATA NOT AVAILABLE") / len(rows) if rows else 0.0
    eligible_count = sum(1 for r in rows if r["eligible"] == "YES")

    passed = (
        len(rows) >= MIN_SNAPSHOT_ROWS
        and duplicate == 0
        and sector_cov >= MIN_SECTOR_COVERAGE
        and value20_cov >= 0.70
        and history120_cov >= 0.70
        and len(errors) <= max(50, int(len(rows) * 0.05))
        and prot["ok"]
    )

    audit = {
        "pass": bool(passed),
        "status": "PASS — FREE WEB SNAPSHOT READY" if passed else "FAIL — DATA PIPELINE",
        "asof_date": asof,
        "data_cutoff_rule": "18:00 KST; regular-session daily data only",
        "snapshot_rows": len(rows),
        "eligible_rows": eligible_count,
        "duplicate_ticker": duplicate,
        "sector_coverage": round(sector_cov, 6),
        "history_120bar_coverage": round(history120_cov, 6),
        "actual_trading_value_20d_coverage": round(value20_cov, 6),
        "history_fetch_errors": len(errors),
        "management_codes": len(prot["management"]),
        "halt_codes": len(prot["halt"]),
        "etf_codes": len(prot["etf"]),
        "etn_codes": len(prot["etn"]),
        "etf_snapshot_rows": len(etf_rows),
        "etf_snapshot_error": etf_error,
        "sector_error": sector_error,
        "sources": {
            "universe": "Daum Finance public market-cap web data",
            "history_ohlcv_actual_value": "Daum Finance public daily quote web data",
            "sector": "Daum Finance public sector web data",
            "management_halt_etf_etn": "Naver Finance public pages",
            "etf_full_snapshot": "Naver Finance public ETF item list",
        },
        "integrity": {
            "krx_open_api_used": False,
            "krx_auth_key_required": False,
            "trading_value_estimated_close_x_volume": False,
            "actual_acc_trade_price_used": True,
            "missing_value_imputation": False,
            "etf_in_stock_ranking": False,
        },
        "note": "Website-facing interfaces can change; source failure must remain DATA PIPELINE failure, never silent imputation.",
    }
    write_audit(asof, audit)
    (OUT / "latest_status.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0 if passed else 2

if __name__ == "__main__":
    raise SystemExit(main())
