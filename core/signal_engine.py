"""
v2.4.0 信号合成模块。

注意：
本版本默认 enable_final_buy_change=false。
也就是说：先生成观察状态和原因，不强行改变现有 paper_trader 买入行为。

v2.5.0 再正式让该模块接管 BUY / STRONG_BUY / NEAR_TRIGGER / WATCH_ONLY。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class ObservationSignal:
    signal_status: str
    signal_level: str
    signal_reasons: List[str]
    risk_flags: List[str]
    should_write_paper_trade: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def build_observation_signal(candidate: Dict[str, Any], *, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    v2.4.0 观察态信号合成。

    输入 candidate 可包含：
    - daily_2buy_score / total_score
    - risk_pct
    - current_price / price
    - trigger_price
    - fresh_quote / is_fresh
    - weekly_score
    - sector_score
    - leader_score
    - yuanjun_score
    - flags
    """
    cfg = config or {}
    reasons: List[str] = []
    flags: List[str] = []

    daily_score = _f(candidate.get("daily_2buy_score", candidate.get("total_score")), 0)
    risk_pct = _f(candidate.get("risk_pct"), 999)
    current_price = _f(candidate.get("current_price", candidate.get("price")), 0)
    trigger_price = _f(candidate.get("trigger_price"), 0)
    fresh = bool(candidate.get("fresh_quote", candidate.get("is_fresh", True)))

    weekly_score = _f(candidate.get("weekly_score"), 50)
    sector_score = _f(candidate.get("sector_score"), 50)
    leader_score = _f(candidate.get("leader_score"), 50)
    yuanjun_score = _f(candidate.get("yuanjun_score"), 50)

    all_flags: List[str] = []
    for k in ["risk_flags", "sector_flags", "weekly_flags", "yuanjun_flags", "flags"]:
        v = candidate.get(k)
        if isinstance(v, list):
            all_flags.extend([str(x) for x in v])
        elif isinstance(v, str) and v:
            all_flags.append(v)

    if not fresh:
        flags.append("行情不新鲜(quote_not_fresh)")
    if risk_pct > _f(cfg.get("risk_pct_max"), 8):
        flags.append("风险比例过高(risk_pct_too_high)")
    if daily_score < _f(cfg.get("daily_2buy_buy_score"), 80):
        flags.append("日线二买分未达买入线(daily_2buy_score_low)")
    if trigger_price > 0 and current_price < trigger_price:
        flags.append("当前价未触发买入价(price_not_triggered)")

    # 极弱项才硬拒绝
    if weekly_score < _f(cfg.get("weekly_hard_reject_score"), 45):
        flags.append("周线极弱(weekly_hard_reject)")
    if sector_score < _f(cfg.get("sector_hard_reject_score"), 45):
        flags.append("板块极弱(sector_hard_reject)")
    if leader_score < _f(cfg.get("leader_reject_score"), 50):
        flags.append("非板块前排(leader_score_too_low)")
    if yuanjun_score < _f(cfg.get("yuanjun_reject_score"), 50):
        flags.append("援军分过低(yuanjun_score_too_low)")

    flags.extend(all_flags)
    flags = list(dict.fromkeys(flags))

    # v2.4.0 仅判断观察态
    hard_reject_keywords = [
        "risk_off",
        "quote_not_fresh",
        "risk_pct_too_high",
        "pullback_new_low",
        "leader_follower",
        "no_sector_follow",
        "divergence_count_too_many",
    ]

    hard_rejected = any(any(key in f for key in hard_reject_keywords) for f in flags)

    if hard_rejected:
        status = "REJECTED"
        level = "reject"
    elif daily_score >= 85 and risk_pct <= 6 and weekly_score >= 70 and sector_score >= 70 and leader_score >= 70 and yuanjun_score >= 75:
        status = "STRONG_WATCH"
        level = "strong_watch"
        reasons.append("多项评分较强，具备强观察价值")
    elif daily_score >= 80 and risk_pct <= 8 and current_price >= trigger_price > 0:
        status = "BUY_CANDIDATE_PREVIEW"
        level = "buy_preview"
        reasons.append("满足日线二买、价格触发、风险可控；v2.4.0 先作为买入候选展示")
    elif daily_score >= 75:
        status = "NEAR_TRIGGER"
        level = "near"
        reasons.append("接近触发，但仍有条件未确认")
    elif daily_score >= 70:
        status = "WATCH_ONLY"
        level = "watch"
        reasons.append("结构可观察，尚未达到买入候选")
    else:
        status = "REJECTED"
        level = "reject"
        flags.append("日线二买分过低(daily_2buy_score_too_low)")

    if weekly_score >= 70:
        reasons.append("周线较强")
    elif weekly_score < 55:
        flags.append("周线一般，降级提醒(weekly_neutral)")

    if sector_score >= 70:
        reasons.append("板块较强")
    elif sector_score < 55:
        flags.append("板块强度一般，降级提醒(sector_neutral)")

    if yuanjun_score >= 75:
        reasons.append("援军确认度较高")
    elif yuanjun_score < 55:
        flags.append("援军确认度一般，降级提醒(yuanjun_neutral)")

    return ObservationSignal(
        signal_status=status,
        signal_level=level,
        signal_reasons=list(dict.fromkeys(reasons)),
        risk_flags=list(dict.fromkeys(flags)),
        should_write_paper_trade=False,
    ).to_dict()
