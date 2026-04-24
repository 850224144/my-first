# -*- coding: utf-8 -*-
"""
core/weekly.py

方案 G+：周线过滤 V1

作用：
1. 只读本地 stock_daily 缓存，不远程拉历史 K；
2. 将日线聚合为周线；
3. 过滤掉周线趋势不健康的股票；
4. 作为板块过滤之后、日线二买评分之前的第二道过滤层。

周线 V1 条件：
- 至少 30 根周线；
- 周线 ma5 > ma10 > ma20；
- 最新周收盘价 > ma10；
- ma10 相比 3 周前不下降；
- 最近 3 周不明显创新低（允许 2% 噪音）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List, Tuple

import polars as pl

from core.data import get_db_connection
from core.logger import get_logger, log_reject, log_exception


def _normalize_code(code: str) -> str:
    s = str(code).strip()
    if not s:
        return ""
    s = s.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    s = s.replace("sh", "").replace("sz", "").replace("bj", "")
    return s.zfill(6) if s.isdigit() else s


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    s = str(value)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _load_daily_for_codes(codes: List[str]) -> pl.DataFrame:
    codes = sorted(set(_normalize_code(c) for c in codes if _normalize_code(c)))
    if not codes:
        return pl.DataFrame()

    in_list = ",".join([f"'{c}'" for c in codes])
    sql = f"""
        SELECT code, date, open, high, low, close, volume, amount
        FROM stock_daily
        WHERE adj_type = 'qfq' AND code IN ({in_list})
        ORDER BY code, date
    """
    con = get_db_connection()
    try:
        cur = con.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        con.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, schema=cols, orient="row")


def _daily_to_weekly_for_code(code: str, df: pl.DataFrame) -> List[Dict[str, Any]]:
    """将单只股票日线转周线。"""
    rows: List[Dict[str, Any]] = []
    if df is None or df.is_empty():
        return rows

    weeks: Dict[tuple, Dict[str, Any]] = {}
    for r in df.sort("date").iter_rows(named=True):
        d = _parse_date(r.get("date"))
        if d is None:
            continue
        iso_year, iso_week, _ = d.isocalendar()
        key = (iso_year, iso_week)
        close = float(r.get("close") or 0)
        open_ = float(r.get("open") or close)
        high = float(r.get("high") or close)
        low = float(r.get("low") or close)
        volume = float(r.get("volume") or 0)
        amount = r.get("amount")
        amount = float(amount) if amount is not None else close * volume * 100

        if key not in weeks:
            weeks[key] = {
                "code": code,
                "week": f"{iso_year}-{iso_week:02d}",
                "date": d.isoformat(),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
            }
        else:
            w = weeks[key]
            w["date"] = d.isoformat()
            w["high"] = max(float(w["high"]), high)
            w["low"] = min(float(w["low"]), low)
            w["close"] = close
            w["volume"] = float(w.get("volume") or 0) + volume
            w["amount"] = float(w.get("amount") or 0) + amount

    return [weeks[k] for k in sorted(weeks.keys())]


def evaluate_weekly_trend(code: str, daily: pl.DataFrame) -> Dict[str, Any]:
    """评估单只股票是否通过周线过滤。"""
    code = _normalize_code(code)
    weekly = _daily_to_weekly_for_code(code, daily)
    if len(weekly) < 30:
        return {"pass": False, "reason": f"weekly_not_enough:{len(weekly)}", "weekly_bars": len(weekly)}

    closes = [float(x["close"]) for x in weekly]
    lows = [float(x["low"]) for x in weekly]

    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10
    ma20 = sum(closes[-20:]) / 20
    ma10_prev3 = sum(closes[-13:-3]) / 10 if len(closes) >= 33 else ma10
    last_close = closes[-1]

    trend_ok = ma5 > ma10 > ma20
    price_ok = last_close > ma10
    slope_ok = ma10 >= ma10_prev3 * 0.995

    # 最近3周不明显创新低：允许2%噪音，避免过严。
    recent3_low = min(lows[-3:])
    prev3_low = min(lows[-6:-3]) if len(lows) >= 6 else recent3_low
    low_ok = recent3_low >= prev3_low * 0.98

    passed = bool(trend_ok and price_ok and slope_ok and low_ok)
    reasons = []
    if not trend_ok:
        reasons.append("weekly_ma_not_bull")
    if not price_ok:
        reasons.append("weekly_close_below_ma10")
    if not slope_ok:
        reasons.append("weekly_ma10_down")
    if not low_ok:
        reasons.append("weekly_recent_low_break")

    return {
        "pass": passed,
        "reason": ",".join(reasons) if reasons else "weekly_ok",
        "weekly_bars": len(weekly),
        "weekly_close": round(last_close, 3),
        "weekly_ma5": round(ma5, 3),
        "weekly_ma10": round(ma10, 3),
        "weekly_ma20": round(ma20, 3),
    }


def filter_universe_by_weekly_trend(universe: pl.DataFrame, strict_weekly: bool = False) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """周线过滤。返回过滤后 universe 和报告。"""
    report = {
        "input": 0,
        "passed": 0,
        "rejected": 0,
        "no_daily": 0,
        "not_enough": 0,
        "skipped_not_enough": 0,
        "strict_weekly": bool(strict_weekly),
    }
    if universe is None or universe.is_empty() or "code" not in universe.columns:
        return pl.DataFrame(), report

    rows = universe.to_dicts()
    report["input"] = len(rows)
    codes = [r.get("code", "") for r in rows]

    try:
        daily_all = _load_daily_for_codes(codes)
    except Exception as e:
        log_exception("读取周线过滤所需日线失败", e)
        return pl.DataFrame(), report

    if daily_all is None or daily_all.is_empty():
        for r in rows:
            log_reject(r.get("code", ""), "weekly", "no_daily_cache", "周线过滤无日线缓存", name=r.get("name", ""))
        report["no_daily"] = len(rows)
        report["rejected"] = len(rows)
        return pl.DataFrame(), report

    daily_by_code = {str(k[0] if isinstance(k, tuple) else k): g for k, g in daily_all.group_by("code", maintain_order=True)}
    passed_rows: List[Dict[str, Any]] = []

    for r in rows:
        code = _normalize_code(r.get("code", ""))
        g = daily_by_code.get(code)
        if g is None or g.is_empty():
            report["no_daily"] += 1
            log_reject(code, "weekly", "no_daily_cache", "周线过滤无日线缓存", name=r.get("name", ""))
            continue

        res = evaluate_weekly_trend(code, g)
        if not res.get("pass"):
            reason = str(res.get("reason", ""))
            if reason.startswith("weekly_not_enough"):
                report["not_enough"] += 1
                if not strict_weekly:
                    # 调试默认：周线数据不足不剔除，先放行，避免缓存覆盖不足时全杀。
                    out = dict(r)
                    out.update({
                        "weekly_bars": res.get("weekly_bars"),
                        "weekly_status": "skipped_not_enough",
                    })
                    passed_rows.append(out)
                    report["skipped_not_enough"] += 1
                    continue
            report["rejected"] += 1
            log_reject(code, "weekly", "weekly_filter_fail", reason, name=r.get("name", ""))
            continue

        out = dict(r)
        out.update({
            "weekly_bars": res.get("weekly_bars"),
            "weekly_close": res.get("weekly_close"),
            "weekly_ma5": res.get("weekly_ma5"),
            "weekly_ma10": res.get("weekly_ma10"),
            "weekly_ma20": res.get("weekly_ma20"),
            "weekly_status": "passed",
        })
        passed_rows.append(out)

    report["passed"] = len(passed_rows)
    report["rejected"] = report["input"] - report["passed"]

    get_logger().info(
        "周线过滤完成：input=%s passed=%s rejected=%s no_daily=%s not_enough=%s",
        report["input"], report["passed"], report["rejected"], report["no_daily"], report["not_enough"]
    )
    return (pl.DataFrame(passed_rows) if passed_rows else pl.DataFrame(), report)
