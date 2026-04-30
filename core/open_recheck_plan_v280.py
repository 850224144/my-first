"""
v2.8.0 明日开盘复核计划构建。
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import datetime as dt
import json


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def next_trade_date_simple_v280(trade_date: str) -> str:
    """
    简单下一个交易日估算：跳过周末。
    真实交易日历可后续替换。
    """
    d = dt.date.fromisoformat(trade_date)
    d += dt.timedelta(days=1)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d.isoformat()


def build_open_recheck_plan_v280(row: Dict[str, Any], *, trade_date: str) -> Dict[str, Any]:
    existing = _parse_jsonish(row.get("open_recheck_plan"))
    if isinstance(existing, dict):
        plan = dict(existing)
    else:
        plan = {}

    symbol = row.get("symbol") or row.get("code")
    stock_name = row.get("stock_name") or row.get("name")

    plan.setdefault("trade_date", trade_date)
    plan.setdefault("plan_date", next_trade_date_simple_v280(trade_date))
    plan.setdefault("symbol", symbol)
    plan.setdefault("stock_name", stock_name)
    plan.setdefault("plan_type", "OPEN_RECHECK")
    plan.setdefault("source_signal_status", row.get("signal_status"))
    plan.setdefault("planned_buy_price", _f(row.get("current_price") or row.get("trigger_price")))
    plan.setdefault("trigger_price", _f(row.get("trigger_price")))
    plan.setdefault("stop_loss", _f(row.get("stop_loss") or row.get("yj_stop_loss")))
    plan.setdefault("risk_pct", _f(row.get("risk_pct")))
    plan.setdefault("daily_2buy_score", _f(row.get("daily_2buy_score") or row.get("total_score")))
    plan.setdefault("sector_score", _f(row.get("sector_score")))
    plan.setdefault("leader_score", _f(row.get("leader_score")))
    plan.setdefault("yuanjun_score", _f(row.get("yuanjun_score")))
    plan.setdefault("weekly_score", _f(row.get("weekly_score")))
    plan.setdefault("theme_name", row.get("theme_name"))
    plan.setdefault("status", "PENDING_OPEN_RECHECK")
    plan.setdefault("created_at", dt.datetime.now().isoformat(timespec="seconds"))

    return plan


def build_paper_trade_candidate_v280(row: Dict[str, Any], *, trade_date: str) -> Dict[str, Any]:
    existing = _parse_jsonish(row.get("paper_trade_record"))
    if isinstance(existing, dict):
        rec = dict(existing)
    else:
        rec = {}

    symbol = row.get("symbol") or row.get("code")
    stock_name = row.get("stock_name") or row.get("name")

    rec.setdefault("trade_date", trade_date)
    rec.setdefault("symbol", symbol)
    rec.setdefault("stock_name", stock_name)
    rec.setdefault("signal_status", row.get("signal_status"))
    rec.setdefault("paper_status", "PENDING_BUY_CONFIRM")
    rec.setdefault("buy_price", _f(row.get("current_price") or row.get("trigger_price")))
    rec.setdefault("planned_buy_price", _f(row.get("current_price") or row.get("trigger_price")))
    rec.setdefault("stop_loss", _f(row.get("stop_loss") or row.get("yj_stop_loss")))
    rec.setdefault("take_profit_1", _f(row.get("target_1") or row.get("take_profit_1")))
    rec.setdefault("take_profit_2", _f(row.get("target_2") or row.get("take_profit_2")))
    rec.setdefault("risk_pct", _f(row.get("risk_pct")))
    rec.setdefault("daily_2buy_score", _f(row.get("daily_2buy_score") or row.get("total_score")))
    rec.setdefault("source", "tail_confirm_v280")
    rec.setdefault("created_at", dt.datetime.now().isoformat(timespec="seconds"))

    return rec
