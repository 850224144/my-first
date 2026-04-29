# core/forward_stats.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
import os

import polars as pl


FORWARD_STATS_PATH = "data/signal_forward_stats.parquet"


def _ensure_dirs():
    os.makedirs("data", exist_ok=True)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _nested_get(item: Dict[str, Any], key: str, default=None):
    if not isinstance(item, dict):
        return default
    v = item.get(key)
    if v not in [None, ""]:
        return v
    for parent in ["score_detail", "plan"]:
        obj = item.get(parent)
        if isinstance(obj, dict):
            v = obj.get(key)
            if v not in [None, ""]:
                return v
    return default


def _read() -> pl.DataFrame:
    if not os.path.exists(FORWARD_STATS_PATH):
        return pl.DataFrame()
    try:
        return pl.read_parquet(FORWARD_STATS_PATH)
    except Exception:
        return pl.DataFrame()


def _write(df: pl.DataFrame):
    _ensure_dirs()
    df.write_parquet(FORWARD_STATS_PATH)


def record_signal_results(results: List[Dict[str, Any]], mode: str) -> pl.DataFrame:
    """记录每次候选出现，用于后续1/3/5/10日表现统计。"""
    _ensure_dirs()
    old = _read()
    today = _today_str()
    rows = []

    for item in results:
        code = str(item.get("code", "")).zfill(6)
        if not code:
            continue
        entry = _safe_float(_nested_get(item, "entry_price", None), None)
        trigger = _safe_float(_nested_get(item, "trigger_price", None), None)
        stop = _safe_float(_nested_get(item, "stop_loss", None), None)
        target1 = _safe_float(_nested_get(item, "take_profit_1", None), None)
        target2 = _safe_float(_nested_get(item, "take_profit_2", None), None)
        rows.append({
            "signal_id": f"{today}_{mode}_{code}",
            "signal_date": today,
            "mode": mode,
            "code": code,
            "name": str(item.get("name") or ""),
            "signal": str(_nested_get(item, "signal", "")),
            "action": str(_nested_get(item, "action", "")),
            "total_score": _safe_float(_nested_get(item, "total_score", _nested_get(item, "score", None)), None),
            "risk_pct": _safe_float(_nested_get(item, "risk_pct", None), None),
            "entry_price": entry,
            "trigger_price": trigger,
            "stop_loss": stop,
            "take_profit_1": target1,
            "take_profit_2": target2,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ret_1d": None,
            "ret_3d": None,
            "ret_5d": None,
            "ret_10d": None,
            "hit_trigger": None,
            "hit_stop": None,
            "hit_target1": None,
            "hit_target2": None,
            "max_profit_pct": None,
            "max_drawdown_pct": None,
            "last_eval_date": "",
        })

    if not rows:
        return old

    new = pl.DataFrame(rows)
    if old.is_empty():
        out = new
    else:
        old_ids = set(new["signal_id"].to_list())
        keep = old.filter(~pl.col("signal_id").is_in(list(old_ids))) if "signal_id" in old.columns else old
        out = pl.concat([keep, new], how="diagonal_relaxed")

    _write(out)
    return out


def _get_db_connection():
    from core.data import get_db_connection
    return get_db_connection()


def _load_daily(code: str) -> pl.DataFrame:
    con = _get_db_connection()
    try:
        rows = con.execute("""
            SELECT date, open, high, low, close
            FROM stock_daily
            WHERE code = ?
            ORDER BY date ASC
        """, [str(code).zfill(6)]).fetchall()
        cols = [x[0] for x in con.description]
    finally:
        con.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, schema=cols, orient="row").with_columns([
        pl.col("date").cast(pl.Date, strict=False),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
    ])


def update_forward_stats() -> pl.DataFrame:
    """每日收盘后更新候选后续表现。"""
    df = _read()
    if df.is_empty():
        return df

    updated = []
    for row in df.to_dicts():
        code = str(row.get("code", "")).zfill(6)
        signal_date = str(row.get("signal_date", ""))[:10]
        entry = _safe_float(row.get("entry_price"), None)
        trigger = _safe_float(row.get("trigger_price"), None)
        stop = _safe_float(row.get("stop_loss"), None)
        target1 = _safe_float(row.get("take_profit_1"), None)
        target2 = _safe_float(row.get("take_profit_2"), None)

        daily = _load_daily(code)
        if daily.is_empty() or not signal_date or entry is None:
            updated.append(row)
            continue

        after = daily.filter(pl.col("date").cast(pl.Utf8) >= signal_date)
        if after.is_empty():
            updated.append(row)
            continue

        closes = after["close"].to_list()
        highs = after["high"].to_list()
        lows = after["low"].to_list()
        dates = after["date"].cast(pl.Utf8).to_list()

        def ret_n(n: int):
            if len(closes) > n:
                return round((float(closes[n]) / entry - 1) * 100, 2)
            return row.get(f"ret_{n}d")

        row["ret_1d"] = ret_n(1)
        row["ret_3d"] = ret_n(3)
        row["ret_5d"] = ret_n(5)
        row["ret_10d"] = ret_n(10)

        max_high = max([float(x) for x in highs]) if highs else None
        min_low = min([float(x) for x in lows]) if lows else None
        if max_high:
            row["max_profit_pct"] = round((max_high / entry - 1) * 100, 2)
        if min_low:
            row["max_drawdown_pct"] = round((min_low / entry - 1) * 100, 2)
        if trigger:
            row["hit_trigger"] = any(float(h) >= trigger for h in highs)
        if stop:
            row["hit_stop"] = any(float(l) <= stop for l in lows)
        if target1:
            row["hit_target1"] = any(float(h) >= target1 for h in highs)
        if target2:
            row["hit_target2"] = any(float(h) >= target2 for h in highs)
        row["last_eval_date"] = dates[-1] if dates else ""
        updated.append(row)

    out = pl.DataFrame(updated)
    _write(out)
    return out


__all__ = ["record_signal_results", "update_forward_stats"]
