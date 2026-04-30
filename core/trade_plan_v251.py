"""
v2.5.1 明日计划构建工具。

目标：
- 从尾盘候选中生成完整 OPEN_RECHECK 计划
- 保留 sector_state / leader_type / weekly_state / yuanjun_state 等字段
- 避免 v2.5.0 demo 中 open paper_trade_record 字段为 null
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import datetime as dt


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def should_create_open_recheck_plan(candidate: Dict[str, Any], signal: Dict[str, Any]) -> bool:
    """
    保守规则：
    - 已经 BUY / STRONG_BUY 的，不再生成次日开盘计划
    - REJECTED 不生成
    - NEAR_TRIGGER 且无硬拒绝，才允许生成
    - 必须有 yj_candle_mid 或 yj_stop_loss
    """
    status = signal.get("signal_status")
    if status in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}:
        return False
    if status == "REJECTED":
        return False
    if signal.get("blocking_flags"):
        return False

    daily_score = _f(candidate.get("daily_2buy_score", candidate.get("total_score")), 0) or 0
    risk_pct = _f(candidate.get("risk_pct"), 999) or 999
    yuanjun_score = _f(candidate.get("yuanjun_score"), 0) or 0

    has_yj_level = candidate.get("yj_candle_mid") is not None or candidate.get("yj_stop_loss") is not None
    if status == "NEAR_TRIGGER" and daily_score >= 75 and risk_pct <= 8 and yuanjun_score >= 55 and has_yj_level:
        return True

    return False


def build_open_recheck_plan(
    candidate: Dict[str, Any],
    signal: Dict[str, Any],
    *,
    plan_date: Optional[str] = None,
    source: str = "tail_confirm",
) -> Dict[str, Any]:
    """
    生成完整 OPEN_RECHECK 计划。
    """
    if plan_date is None:
        plan_date = dt.date.today().isoformat()

    symbol = candidate.get("symbol") or candidate.get("code")
    stock_name = candidate.get("stock_name") or candidate.get("name")

    planned_buy_price = (
        signal.get("planned_buy_price")
        or candidate.get("planned_buy_price")
        or candidate.get("trigger_price")
        or candidate.get("current_price")
        or candidate.get("price")
    )

    yj_low = candidate.get("yj_candle_low") or candidate.get("rescue_candle_low") or candidate.get("yj_stop_loss") or candidate.get("stop_loss")
    yj_mid = candidate.get("yj_candle_mid") or candidate.get("rescue_candle_mid")
    yj_high = candidate.get("yj_candle_high") or candidate.get("rescue_candle_high")

    return {
        "plan_date": plan_date,
        "plan_type": "OPEN_RECHECK",
        "source": source,

        "symbol": symbol,
        "stock_name": stock_name,

        "signal_status": signal.get("signal_status"),
        "signal_level": signal.get("signal_level"),

        "planned_buy_price": _f(planned_buy_price),
        "trigger_price": _f(candidate.get("trigger_price")),
        "current_price": _f(candidate.get("current_price", candidate.get("price"))),

        "yj_candle_low": _f(yj_low),
        "yj_candle_mid": _f(yj_mid),
        "yj_candle_high": _f(yj_high),
        "yj_stop_loss": _f(candidate.get("yj_stop_loss") or yj_low),

        "target_1": _f(signal.get("target_1") or candidate.get("target_1")),
        "target_2": _f(signal.get("target_2") or candidate.get("target_2")),
        "time_stop_days": signal.get("time_stop_days") or candidate.get("time_stop_days") or 5,

        "risk_pct": _f(candidate.get("risk_pct")),
        "daily_2buy_score": _f(candidate.get("daily_2buy_score", candidate.get("total_score"))),

        "theme_name": candidate.get("theme_name"),
        "sector_score": _f(candidate.get("sector_score")),
        "sector_state": candidate.get("sector_state"),
        "leader_score": _f(candidate.get("leader_score")),
        "leader_type": candidate.get("leader_type"),
        "weekly_score": _f(candidate.get("weekly_score")),
        "weekly_state": candidate.get("weekly_state"),
        "yuanjun_score": _f(candidate.get("yuanjun_score")),
        "yuanjun_state": candidate.get("yuanjun_state"),
        "divergence_count": candidate.get("divergence_count"),
        "rescue_candle_score": _f(candidate.get("rescue_candle_score")),

        "signal_reasons": signal.get("signal_reasons") or [],
        "risk_flags": signal.get("risk_flags") or [],
        "blocking_flags": signal.get("blocking_flags") or [],
        "downgrade_flags": signal.get("downgrade_flags") or [],
        "upgrade_reasons": signal.get("upgrade_reasons") or [],
    }
