"""
v2.5.0 次日开盘复核模块。

调度建议：
- 09:26 open_recheck：拿集合竞价后的开盘价做初筛
- 09:30 open_confirm：连续竞价开始后再确认一次

只检查昨晚 trade_plan 里的 OPEN_RECHECK 标的，不扫全市场。
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import datetime as dt
import math


DEFAULT_OPEN_CONFIG: Dict[str, Any] = {
    "max_open_chase_pct": 3,
    "open_confirm_drop_tolerance_pct": 0.5,
    "require_open_above_yj_mid": True,
    "require_fresh_quote": True,
    "write_paper_trade_on_confirm": True,
}


@dataclass
class OpenCheckResult:
    open_status: str
    should_notify: bool
    should_write_paper_trade: bool
    entry_type: str
    reasons: List[str]
    risk_flags: List[str]
    planned_buy_price: Optional[float]
    stop_loss: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _cfg(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(DEFAULT_OPEN_CONFIG)
    if config:
        if "open_recheck" in config and isinstance(config["open_recheck"], dict):
            out.update(config["open_recheck"])
        else:
            out.update(config)
    return out


def _f(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value is None:
            return default
        v = float(str(value).replace(",", ""))
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _b(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return bool(value)


def open_recheck(
    trade_plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    market_state: str = "",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    09:26 使用。

    通过条件：
    - open_price 有效
    - open_price >= 援军阳线中位
    - open_price 不高于计划买入价太多
    - 大盘不是 risk_off
    - 行情 fresh
    """
    cfg = _cfg(config)
    reasons: List[str] = []
    flags: List[str] = []

    open_price = _f(quote.get("open") or quote.get("open_price"))
    current_price = _f(quote.get("price") or quote.get("current_price"), open_price)
    fresh = _b(quote.get("fresh_quote", quote.get("is_fresh")), True)

    planned_price = _f(
        trade_plan.get("planned_buy_price")
        or trade_plan.get("signal_price")
        or trade_plan.get("trigger_price")
    )
    yj_mid = _f(trade_plan.get("yj_candle_mid") or trade_plan.get("rescue_candle_mid"), None)
    stop_loss = _f(trade_plan.get("yj_stop_loss") or trade_plan.get("stop_loss") or trade_plan.get("yj_candle_low"), None)

    if str(market_state).lower() == "risk_off":
        flags.append("大盘 risk_off，开盘复核拒绝(market_risk_off)")
    if cfg["require_fresh_quote"] and not fresh:
        flags.append("行情不新鲜(quote_not_fresh)")
    if not open_price or open_price <= 0:
        flags.append("开盘价无效(open_price_invalid)")
    if not planned_price or planned_price <= 0:
        flags.append("计划买入价无效(planned_buy_price_invalid)")

    if yj_mid and open_price and cfg["require_open_above_yj_mid"]:
        if open_price < yj_mid:
            flags.append("开盘跌破援军阳线二分位(open_below_yj_mid)")
        else:
            reasons.append("开盘价未跌破援军阳线二分位")

    if planned_price and open_price:
        max_open = planned_price * (1 + float(cfg["max_open_chase_pct"]) / 100.0)
        if open_price > max_open:
            flags.append("开盘价高于允许追价上限(open_chase_too_high)")
        else:
            reasons.append("开盘价未明显高开过度")

    if flags:
        status = "OPEN_RECHECK_REJECTED"
        should_notify = True
        should_write = False
    else:
        status = "OPEN_RECHECK_PASSED"
        should_notify = True
        should_write = False
        reasons.append("进入 09:30 二次确认，不立即建仓")

    return OpenCheckResult(
        open_status=status,
        should_notify=should_notify,
        should_write_paper_trade=should_write,
        entry_type="next_open",
        reasons=list(dict.fromkeys(reasons)),
        risk_flags=list(dict.fromkeys(flags)),
        planned_buy_price=current_price or open_price,
        stop_loss=stop_loss,
    ).to_dict()


def open_confirm(
    trade_plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    market_state: str = "",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    09:30 使用。

    通过才返回 OPEN_BUY_TRIGGERED。
    """
    cfg = _cfg(config)
    reasons: List[str] = []
    flags: List[str] = []

    open_price = _f(quote.get("open") or quote.get("open_price"))
    current_price = _f(quote.get("price") or quote.get("current_price"))
    fresh = _b(quote.get("fresh_quote", quote.get("is_fresh")), True)

    planned_price = _f(
        trade_plan.get("planned_buy_price")
        or trade_plan.get("signal_price")
        or trade_plan.get("trigger_price")
    )
    yj_mid = _f(trade_plan.get("yj_candle_mid") or trade_plan.get("rescue_candle_mid"), None)
    stop_loss = _f(trade_plan.get("yj_stop_loss") or trade_plan.get("stop_loss") or trade_plan.get("yj_candle_low"), None)

    if str(market_state).lower() == "risk_off":
        flags.append("大盘 risk_off，开盘确认拒绝(market_risk_off)")
    if cfg["require_fresh_quote"] and not fresh:
        flags.append("行情不新鲜(quote_not_fresh)")
    if not open_price or open_price <= 0:
        flags.append("开盘价无效(open_price_invalid)")
    if not current_price or current_price <= 0:
        flags.append("当前价无效(current_price_invalid)")
    if not planned_price or planned_price <= 0:
        flags.append("计划买入价无效(planned_buy_price_invalid)")

    if open_price and current_price:
        min_price = open_price * (1 - float(cfg["open_confirm_drop_tolerance_pct"]) / 100.0)
        if current_price < min_price:
            flags.append("9:30 后快速走弱(open_confirm_fast_drop)")
        else:
            reasons.append("9:30 后价格未快速走弱")

    if yj_mid and current_price and cfg["require_open_above_yj_mid"]:
        if current_price < yj_mid:
            flags.append("当前价跌破援军阳线二分位(current_below_yj_mid)")
        else:
            reasons.append("当前价仍在援军阳线二分位上方")

    if planned_price and current_price:
        max_price = planned_price * (1 + float(cfg["max_open_chase_pct"]) / 100.0)
        if current_price > max_price:
            flags.append("当前价超过允许追价上限(open_confirm_chase_too_high)")
        else:
            reasons.append("当前价仍在允许买入区")

    if flags:
        status = "OPEN_CONFIRM_REJECTED"
        should_write = False
    else:
        status = "OPEN_BUY_TRIGGERED"
        should_write = bool(cfg.get("write_paper_trade_on_confirm", True))
        reasons.append("开盘二次确认通过，可写入纸面交易")
        reasons.append("注意：今日买入后 T+1 不可卖出")

    return OpenCheckResult(
        open_status=status,
        should_notify=True,
        should_write_paper_trade=should_write,
        entry_type="next_open",
        reasons=list(dict.fromkeys(reasons)),
        risk_flags=list(dict.fromkeys(flags)),
        planned_buy_price=current_price,
        stop_loss=stop_loss,
    ).to_dict()


def filter_open_recheck_plans(trade_plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    只挑昨晚计划里需要次日开盘复核的标的。
    """
    out: List[Dict[str, Any]] = []
    for p in trade_plans or []:
        plan_type = str(p.get("plan_type") or p.get("signal_status") or "").upper()
        if plan_type in {"OPEN_RECHECK", "YJ_OPEN_RECHECK", "NEXT_OPEN_RECHECK"}:
            out.append(p)
    return out
