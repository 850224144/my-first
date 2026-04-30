"""
v2.5.0 正式信号合成模块。

职责：
- 把日线二买、板块、周线、援军、风控、价格触发统一合成最终状态
- 只有 BUY_TRIGGERED / STRONG_BUY_TRIGGERED 才允许写入尾盘纸面交易
- open_recheck/open_confirm 的开盘买点由 core.open_recheck 处理

核心原则：
- 日线二买 + 价格触发 + 风险可控 = 买入底线
- 周线 / 板块 / 援军 = 升级、降级、极弱过滤
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import math


DEFAULT_FINAL_SIGNAL_CONFIG: Dict[str, Any] = {
    "daily_watch_score": 70,
    "daily_near_score": 75,
    "daily_buy_score": 80,
    "daily_strong_score": 85,
    "max_risk_pct": 8,
    "strong_max_risk_pct": 6,
    "max_price_distance_from_trigger_pct": 3,
    "weekly_hard_reject_score": 45,
    "weekly_buy_score": 55,
    "weekly_strong_score": 70,
    "sector_hard_reject_score": 45,
    "sector_buy_score": 55,
    "sector_strong_score": 70,
    "leader_reject_score": 50,
    "leader_buy_score": 60,
    "leader_strong_score": 70,
    "yuanjun_reject_score": 50,
    "yuanjun_buy_score": 55,
    "yuanjun_strong_score": 80,
    "rescue_candle_strong_score": 75,
}


@dataclass
class FinalSignal:
    signal_status: str
    signal_level: str
    should_write_paper_trade: bool
    entry_type: Optional[str]
    signal_reasons: List[str]
    risk_flags: List[str]
    blocking_flags: List[str]
    downgrade_flags: List[str]
    upgrade_reasons: List[str]
    planned_buy_price: Optional[float]
    stop_loss: Optional[float]
    target_1: Optional[float]
    target_2: Optional[float]
    time_stop_days: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _cfg(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(DEFAULT_FINAL_SIGNAL_CONFIG)
    if config:
        # 支持直接传 final_signal 配置，也支持传完整配置 dict
        if "final_signal" in config and isinstance(config["final_signal"], dict):
            out.update(config["final_signal"])
        else:
            out.update(config)
    return out


def _f(value: Any, default: float = 0.0) -> float:
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
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    return bool(value)


def _listify(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if str(x)]
    if isinstance(value, tuple):
        return [str(x) for x in value if str(x)]
    if isinstance(value, str):
        return [value] if value else []
    return [str(value)]


def _contains_flag(flags: List[str], keyword: str) -> bool:
    return any(keyword in str(f) for f in flags)


def _merge_flags(candidate: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key in [
        "risk_flags",
        "sector_flags",
        "weekly_flags",
        "yuanjun_flags",
        "flags",
        "signal_flags",
    ]:
        out.extend(_listify(candidate.get(key)))
    return list(dict.fromkeys(out))


def _price_distance_pct(current_price: float, trigger_price: float) -> float:
    if trigger_price <= 0:
        return 999.0
    return (current_price / trigger_price - 1.0) * 100.0


def build_final_signal(candidate: Dict[str, Any], *, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = _cfg(config)

    reasons: List[str] = []
    risk_flags: List[str] = _merge_flags(candidate)
    blocking_flags: List[str] = []
    downgrade_flags: List[str] = []
    upgrade_reasons: List[str] = []

    daily_score = _f(candidate.get("daily_2buy_score", candidate.get("total_score")), 0)
    risk_pct = _f(candidate.get("risk_pct"), 999)
    current_price = _f(candidate.get("current_price", candidate.get("price")), 0)
    trigger_price = _f(candidate.get("trigger_price"), 0)
    fresh_quote = _b(candidate.get("fresh_quote", candidate.get("is_fresh")), True)
    market_state = str(candidate.get("market_state", "") or "").lower()

    weekly_score = _f(candidate.get("weekly_score"), 50)
    weekly_state = str(candidate.get("weekly_state", "") or "")
    sector_score = _f(candidate.get("sector_score"), 50)
    leader_score = _f(candidate.get("leader_score"), 50)
    leader_type = str(candidate.get("leader_type", "") or "")
    yuanjun_score = _f(candidate.get("yuanjun_score"), 50)
    rescue_score = _f(candidate.get("rescue_candle_score"), 50)

    already_holding = _b(candidate.get("already_holding"), False)
    already_paper_holding = _b(candidate.get("already_paper_holding"), False)
    in_cooldown = _b(candidate.get("in_cooldown"), False)

    no_breakout = _b(candidate.get("no_breakout"), False) or _contains_flag(risk_flags, "no_breakout")
    too_hot_today = _b(candidate.get("too_hot_today"), False) or _contains_flag(risk_flags, "too_hot_today")

    # 硬拒绝：市场、数据、持仓、风控
    if market_state == "risk_off":
        blocking_flags.append("大盘风险关闭(market_risk_off)")
    if not fresh_quote:
        blocking_flags.append("行情不新鲜(quote_not_fresh)")
    if already_holding:
        blocking_flags.append("已有真实持仓(already_holding)")
    if already_paper_holding:
        blocking_flags.append("已有纸面持仓(already_paper_holding)")
    if in_cooldown:
        blocking_flags.append("冷却期内(in_cooldown)")
    if no_breakout:
        blocking_flags.append("未突破(no_breakout)")
    if too_hot_today:
        blocking_flags.append("当日过热(too_hot_today)")

    if daily_score < cfg["daily_watch_score"]:
        blocking_flags.append("日线二买结构不足(daily_2buy_broken)")
    if risk_pct > cfg["max_risk_pct"]:
        blocking_flags.append("风险比例过高(risk_pct_too_high)")

    if _contains_flag(risk_flags, "pullback_new_low"):
        blocking_flags.append("回调创新低(pullback_new_low)")
    if leader_type == "follower" or _contains_flag(risk_flags, "leader_follower"):
        blocking_flags.append("后排跟风，援军不救杂毛(leader_follower)")
    if _contains_flag(risk_flags, "no_sector_follow"):
        blocking_flags.append("无板块效应(no_sector_follow)")
    if _contains_flag(risk_flags, "divergence_count_too_many"):
        blocking_flags.append("主线第二次及以上分歧(divergence_count_too_many)")

    if weekly_score < cfg["weekly_hard_reject_score"] or weekly_state in {"weekly_downtrend", "weekly_too_hot"}:
        blocking_flags.append("周线极弱/过热(weekly_downtrend_hard)")
    if sector_score < cfg["sector_hard_reject_score"]:
        blocking_flags.append("板块极弱(sector_hard_reject)")
    if leader_score < cfg["leader_reject_score"]:
        blocking_flags.append("龙头分过低(leader_score_too_low)")
    if yuanjun_score < cfg["yuanjun_reject_score"]:
        blocking_flags.append("援军分过低(yuanjun_score_too_low)")

    if blocking_flags:
        status = "REJECTED"
        level = "reject"
        should_write = False
    else:
        price_triggered = trigger_price > 0 and current_price >= trigger_price
        price_far = False
        if price_triggered:
            dist = _price_distance_pct(current_price, trigger_price)
            if dist > cfg["max_price_distance_from_trigger_pct"]:
                price_far = True
                downgrade_flags.append(f"当前价远离触发价过多(price_too_far_from_trigger:{dist:.2f}%)")

        if trigger_price <= 0:
            downgrade_flags.append("缺少触发价(trigger_price_missing)")
        elif not price_triggered:
            downgrade_flags.append("当前价未触发买入价(price_not_triggered)")

        if weekly_score < cfg["weekly_buy_score"]:
            downgrade_flags.append("周线一般，降级(weekly_neutral)")
        if sector_score < cfg["sector_buy_score"]:
            downgrade_flags.append("板块一般，降级(sector_neutral)")
        if leader_score < cfg["leader_buy_score"]:
            downgrade_flags.append("龙头分一般，降级(leader_neutral)")
        if yuanjun_score < cfg["yuanjun_buy_score"]:
            downgrade_flags.append("援军确认度一般，降级(yuanjun_neutral)")
        if _contains_flag(risk_flags, "volume_not_confirm"):
            downgrade_flags.append("量能未确认(volume_not_confirm)")
        if _contains_flag(risk_flags, "high_position_yuanjun"):
            downgrade_flags.append("高位援军风险，禁止强买(high_position_yuanjun)")

        strong_ok = (
            daily_score >= cfg["daily_strong_score"]
            and risk_pct <= cfg["strong_max_risk_pct"]
            and price_triggered
            and not price_far
            and weekly_score >= cfg["weekly_strong_score"]
            and sector_score >= cfg["sector_strong_score"]
            and leader_score >= cfg["leader_strong_score"]
            and yuanjun_score >= cfg["yuanjun_strong_score"]
            and rescue_score >= cfg["rescue_candle_strong_score"]
            and not _contains_flag(risk_flags, "high_position_yuanjun")
            and not _contains_flag(risk_flags, "volume_not_confirm")
        )

        buy_ok = (
            daily_score >= cfg["daily_buy_score"]
            and risk_pct <= cfg["max_risk_pct"]
            and price_triggered
            and not price_far
            and weekly_score >= cfg["weekly_buy_score"]
            and sector_score >= cfg["sector_buy_score"]
            and leader_score >= cfg["leader_buy_score"]
            and yuanjun_score >= cfg["yuanjun_buy_score"]
        )

        if strong_ok:
            status = "STRONG_BUY_TRIGGERED"
            level = "strong_buy"
            should_write = True
            upgrade_reasons.append("日线二买、周线、板块、龙头、援军、风控均达到强信号")
        elif buy_ok:
            status = "BUY_TRIGGERED"
            level = "buy"
            should_write = True
            reasons.append("满足普通买入触发：日线二买、价格触发、风险可控、评分达标")
        elif daily_score >= cfg["daily_near_score"]:
            status = "NEAR_TRIGGER"
            level = "near"
            should_write = False
            reasons.append("接近触发，但价格/周线/板块/援军/量能仍有未确认项")
        elif daily_score >= cfg["daily_watch_score"]:
            status = "WATCH_ONLY"
            level = "watch"
            should_write = False
            reasons.append("结构可观察，但未达到买入触发")
        else:
            status = "REJECTED"
            level = "reject"
            should_write = False
            blocking_flags.append("日线二买分过低(daily_2buy_score_too_low)")

    if weekly_score >= cfg["weekly_strong_score"]:
        upgrade_reasons.append("周线较强")
    if sector_score >= cfg["sector_strong_score"]:
        upgrade_reasons.append("板块较强")
    if leader_score >= cfg["leader_strong_score"]:
        upgrade_reasons.append("龙头/前排地位较强")
    if yuanjun_score >= cfg["yuanjun_strong_score"]:
        upgrade_reasons.append("援军确认度较高")

    planned_buy_price = current_price if current_price > 0 else None
    stop_loss = (
        candidate.get("yj_stop_loss")
        or candidate.get("stop_loss")
        or candidate.get("rescue_candle_low")
    )
    stop_loss = _f(stop_loss, None) if stop_loss is not None else None

    target_1 = candidate.get("target_1")
    target_2 = candidate.get("target_2")
    target_1 = _f(target_1, None) if target_1 is not None else None
    target_2 = _f(target_2, None) if target_2 is not None else None

    time_stop_days = candidate.get("time_stop_days", candidate.get("time_stop_trading_days", 5))
    try:
        time_stop_days = int(time_stop_days)
    except Exception:
        time_stop_days = 5

    return FinalSignal(
        signal_status=status,
        signal_level=level,
        should_write_paper_trade=should_write,
        entry_type="close_tail" if should_write else None,
        signal_reasons=list(dict.fromkeys(reasons)),
        risk_flags=list(dict.fromkeys(risk_flags)),
        blocking_flags=list(dict.fromkeys(blocking_flags)),
        downgrade_flags=list(dict.fromkeys(downgrade_flags)),
        upgrade_reasons=list(dict.fromkeys(upgrade_reasons)),
        planned_buy_price=planned_buy_price,
        stop_loss=stop_loss,
        target_1=target_1,
        target_2=target_2,
        time_stop_days=time_stop_days,
    ).to_dict()


def is_buy_signal(signal: Dict[str, Any]) -> bool:
    return signal.get("signal_status") in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"} and bool(signal.get("should_write_paper_trade"))
