"""
v2.5.0 paper_trader 增强工具。

目标：
- 统一生成纸面交易记录字段
- 区分 close_tail / next_open
- 正确生成 sellable_date
- 开盘买入当天跌破撤军线时，只发 T+1 风险提醒，不模拟当天止损
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import datetime as dt
import json
import math


BUY_STATUSES = {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED", "OPEN_BUY_TRIGGERED"}


def _date(value: Any) -> dt.date:
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    return dt.date.fromisoformat(str(value)[:10])


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        v = float(str(value).replace(",", ""))
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def next_trading_date(trade_date: Any, trading_days: Optional[List[str]] = None) -> str:
    """
    返回下一个交易日。
    如果传入 trading_days，严格按交易日历。
    如果没有交易日历，按自然日跳过周末粗略处理。
    """
    d = _date(trade_date)

    if trading_days:
        days = sorted(_date(x) for x in trading_days)
        for x in days:
            if x > d:
                return x.isoformat()

    x = d + dt.timedelta(days=1)
    while x.weekday() >= 5:
        x += dt.timedelta(days=1)
    return x.isoformat()


def is_sellable(position: Dict[str, Any], trade_date: Any) -> bool:
    d = _date(trade_date)
    sellable_date = position.get("sellable_date")
    if not sellable_date:
        buy_date = position.get("buy_date") or position.get("trade_date")
        if not buy_date:
            return False
        sellable_date = next_trading_date(buy_date)
    return d >= _date(sellable_date)


def build_paper_trade_record(
    *,
    candidate: Dict[str, Any],
    signal: Dict[str, Any],
    trade_date: Any,
    entry_type: str = "close_tail",
    trading_days: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    生成可写入 paper_trader 的统一记录。
    不直接入库，由你的原 paper_trader 调用。
    """
    status = signal.get("signal_status") or signal.get("open_status")
    if status not in BUY_STATUSES:
        raise ValueError(f"not a buy status: {status}")

    buy_date = _date(trade_date).isoformat()
    sellable_date = next_trading_date(buy_date, trading_days=trading_days)

    symbol = candidate.get("symbol") or candidate.get("code")
    name = candidate.get("stock_name") or candidate.get("name")

    buy_price = (
        signal.get("planned_buy_price")
        or candidate.get("planned_buy_price")
        or candidate.get("current_price")
        or candidate.get("price")
    )
    stop_loss = signal.get("stop_loss") or candidate.get("yj_stop_loss") or candidate.get("stop_loss")

    record = {
        "symbol": symbol,
        "stock_name": name,
        "signal_status": status,
        "entry_type": entry_type,
        "buy_date": buy_date,
        "sellable_date": sellable_date,
        "buy_price": _f(buy_price),
        "planned_buy_price": _f(buy_price),
        "stop_loss": _f(stop_loss),
        "target_1": _f(signal.get("target_1") or candidate.get("target_1")),
        "target_2": _f(signal.get("target_2") or candidate.get("target_2")),
        "time_stop_days": int(signal.get("time_stop_days") or candidate.get("time_stop_days") or 5),
        "risk_pct": _f(candidate.get("risk_pct")),
        "daily_2buy_score": _f(candidate.get("daily_2buy_score", candidate.get("total_score"))),
        "sector_score": _f(candidate.get("sector_score")),
        "leader_score": _f(candidate.get("leader_score")),
        "weekly_score": _f(candidate.get("weekly_score")),
        "yuanjun_score": _f(candidate.get("yuanjun_score")),
        "theme_name": candidate.get("theme_name"),
        "sector_state": candidate.get("sector_state"),
        "leader_type": candidate.get("leader_type"),
        "weekly_state": candidate.get("weekly_state"),
        "yuanjun_state": candidate.get("yuanjun_state"),
        "divergence_count": candidate.get("divergence_count"),
        "rescue_candle_score": _f(candidate.get("rescue_candle_score")),
        "yj_candle_low": _f(candidate.get("yj_candle_low")),
        "yj_candle_mid": _f(candidate.get("yj_candle_mid")),
        "yj_candle_high": _f(candidate.get("yj_candle_high")),
        "signal_reasons": json.dumps(signal.get("signal_reasons") or signal.get("reasons") or [], ensure_ascii=False),
        "risk_flags": json.dumps(signal.get("risk_flags") or [], ensure_ascii=False),
        "raw_json": json.dumps({"candidate": candidate, "signal": signal}, ensure_ascii=False),
    }
    return record


def check_t1_stop_risk(position: Dict[str, Any], *, current_price: float, trade_date: Any) -> Dict[str, Any]:
    """
    检查跌破撤军线。
    如果不可卖，只返回 T+1 风险提醒。
    """
    stop_loss = _f(position.get("stop_loss"))
    if not stop_loss:
        return {"has_stop_risk": False, "can_sell": False, "message": ""}

    if current_price >= stop_loss:
        return {"has_stop_risk": False, "can_sell": is_sellable(position, trade_date), "message": ""}

    can_sell = is_sellable(position, trade_date)
    if can_sell:
        return {
            "has_stop_risk": True,
            "can_sell": True,
            "action": "SELL_STOP_LOSS",
            "message": "已跌破撤军线，且已过 T+1，可执行止损。",
        }

    return {
        "has_stop_risk": True,
        "can_sell": False,
        "action": "T1_RISK_ALERT",
        "message": "今日新买入，已跌破撤军线，但 T+1 不可卖出，次交易日优先处理。",
    }
