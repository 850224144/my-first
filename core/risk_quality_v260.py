"""
v2.6.0 Observe 风险质量诊断。

核心目标：
- 不放松 tail_confirm
- 在 observe 阶段提前分层
- 找出噪音候选和真正接近交易的候选
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _flags(item: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for k in ["risk_flags", "blocking_flags", "downgrade_flags", "weekly_flags", "sector_flags", "yuanjun_flags"]:
        v = item.get(k)
        if isinstance(v, list):
            out.extend(str(x) for x in v if str(x))
        elif isinstance(v, str) and v:
            out.append(v)
    return list(dict.fromkeys(out))


def _has(flags: List[str], keyword: str) -> bool:
    return any(keyword in x for x in flags)


def risk_bucket(risk_pct: Any) -> str:
    r = _f(risk_pct, 999)
    if r <= 5:
        return "<=5"
    if r <= 8:
        return "5-8"
    if r <= 12:
        return "8-12"
    if r <= 20:
        return "12-20"
    if r <= 30:
        return "20-30"
    return ">30"


def diagnose_risk_reasons(item: Dict[str, Any]) -> List[str]:
    flags = _flags(item)
    reasons: List[str] = []

    risk = _f(item.get("risk_pct"), 999)
    price = _f(item.get("current_price", item.get("price")), 0)
    trigger = _f(item.get("trigger_price"), 0)
    stop = _f(item.get("stop_loss", item.get("yj_stop_loss")), 0)

    if risk > 8:
        reasons.append("风险比例超过8%")
    if risk > 20:
        reasons.append("止损距离严重过远")
    elif risk > 12:
        reasons.append("止损距离偏远")

    if price and stop and price > stop:
        stop_gap = (price / stop - 1) * 100
        if stop_gap > 20:
            reasons.append("当前价距离结构止损超过20%")
        elif stop_gap > 12:
            reasons.append("当前价距离结构止损超过12%")

    if trigger and price < trigger:
        reasons.append("价格未突破触发价")

    if _has(flags, "too_hot_today"):
        reasons.append("当日过热")
    if _has(flags, "trend_too_hot") or _has(flags, "weekly_downtrend_hard") or _has(flags, "weekly_too_hot"):
        reasons.append("周线/趋势过热")
    if _has(flags, "no_breakout"):
        reasons.append("未突破")
    if _has(flags, "volatility_not_contracting"):
        reasons.append("波动未收敛")
    if _has(flags, "volume_not_confirm"):
        reasons.append("量能未确认")
    if _has(flags, "pullback_days_not_ideal"):
        reasons.append("回调天数不理想")
    if _has(flags, "lows_not_rising"):
        reasons.append("低点抬高不充分")

    return list(dict.fromkeys(reasons))


def classify_observe_quality(item: Dict[str, Any]) -> Dict[str, Any]:
    flags = _flags(item)
    risk = _f(item.get("risk_pct"), 999)
    daily = _f(item.get("daily_2buy_score", item.get("total_score")), 0)
    price = _f(item.get("current_price", item.get("price")), 0)
    trigger = _f(item.get("trigger_price"), 0)
    fresh = bool(item.get("fresh_quote", item.get("is_fresh", False)))

    price_triggered = bool(trigger > 0 and price >= trigger)
    too_hot = _has(flags, "too_hot_today")
    no_breakout = _has(flags, "no_breakout") or (trigger > 0 and price < trigger)
    weekly_risk = _has(flags, "weekly_downtrend_hard") or _has(flags, "weekly_too_hot")

    reasons = diagnose_risk_reasons(item)

    if not fresh:
        quality = "reject_bad_data"
        priority = 0
    elif risk <= 8 and daily >= 80 and price_triggered and not too_hot and not no_breakout and not weekly_risk:
        quality = "tail_ready"
        priority = 100
    elif risk <= 8 and daily >= 70:
        quality = "observe_keep"
        priority = 80
    elif risk <= 12 and daily >= 75 and not too_hot:
        quality = "observe_light"
        priority = 60
    elif risk <= 20 and daily >= 75 and price_triggered:
        quality = "observe_light"
        priority = 45
    else:
        quality = "noise_high_risk"
        priority = 20

    # 过热、未突破、周线风险降低优先级
    if too_hot:
        priority -= 15
    if no_breakout:
        priority -= 10
    if weekly_risk:
        priority -= 10
    priority = max(0, min(100, priority))

    return {
        "symbol": item.get("symbol") or item.get("code"),
        "stock_name": item.get("stock_name") or item.get("name"),
        "observe_quality": quality,
        "observe_priority": priority,
        "risk_bucket": risk_bucket(risk),
        "risk_pct": risk,
        "daily_2buy_score": daily,
        "current_price": price,
        "trigger_price": trigger,
        "price_triggered": price_triggered,
        "fresh_quote": fresh,
        "theme_name": item.get("theme_name"),
        "xgb_pools": item.get("xgb_pools"),
        "risk_reasons": reasons,
    }


def summarize_observe_quality(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [classify_observe_quality(x) for x in items]
    quality_counter = Counter(x["observe_quality"] for x in rows)
    bucket_counter = Counter(x["risk_bucket"] for x in rows)
    triggered = sum(1 for x in rows if x["price_triggered"])
    fresh = sum(1 for x in rows if x["fresh_quote"])

    top = sorted(rows, key=lambda x: (x["observe_priority"], -x["risk_pct"]), reverse=True)

    return {
        "total": len(items),
        "fresh_count": fresh,
        "price_triggered_count": triggered,
        "quality_counter": dict(quality_counter),
        "risk_bucket_counter": dict(bucket_counter),
        "top_candidates": top[:20],
        "rows": rows,
    }


def format_observe_quality_report(items: List[Dict[str, Any]]) -> str:
    s = summarize_observe_quality(items)
    lines: List[str] = []
    lines.append("【Observe 风险质量诊断 v2.6.0】")
    lines.append("")
    lines.append(f"候选总数：{s['total']}")
    lines.append(f"行情新鲜：{s['fresh_count']}")
    lines.append(f"价格触发：{s['price_triggered_count']}")
    lines.append("")
    lines.append("观察质量分布：")
    for k, v in s["quality_counter"].items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("risk_pct 分布：")
    order = ["<=5", "5-8", "8-12", "12-20", "20-30", ">30"]
    for k in order:
        if s["risk_bucket_counter"].get(k):
            lines.append(f"- {k}: {s['risk_bucket_counter'][k]}")
    lines.append("")
    lines.append("优先级 Top10：")
    for x in s["top_candidates"][:10]:
        lines.append(
            f"- {x['symbol']} {x.get('stock_name') or ''} | "
            f"{x['observe_quality']} p={x['observe_priority']} "
            f"risk={x['risk_pct']} daily={x['daily_2buy_score']} "
            f"price={x['current_price']} trigger={x['trigger_price']} "
            f"theme={x.get('theme_name') or '-'} | "
            f"{','.join(x['risk_reasons'][:4])}"
        )
    return "\n".join(lines)
