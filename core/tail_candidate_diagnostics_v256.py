"""
v2.5.6 尾盘候选诊断。

目标：
- 不改策略
- 不放松风控
- 解释为什么没有 BUY_TRIGGERED
- 找出“最接近可交易”的候选
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


def _has(flags: List[str], key: str) -> bool:
    return any(key in str(x) for x in flags)


def collect_flags(item: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for k in ["blocking_flags", "downgrade_flags", "risk_flags", "sector_flags", "weekly_flags", "yuanjun_flags"]:
        v = item.get(k)
        if isinstance(v, list):
            out.extend(str(x) for x in v)
        elif isinstance(v, str) and v:
            out.append(v)
    return list(dict.fromkeys(out))


def diagnose_one_candidate(item: Dict[str, Any]) -> Dict[str, Any]:
    flags = collect_flags(item)
    risk_pct = _f(item.get("risk_pct"), 999)
    daily = _f(item.get("daily_2buy_score", item.get("total_score")), 0)
    current_price = _f(item.get("current_price", item.get("price")), 0)
    trigger_price = _f(item.get("trigger_price"), 0)
    fresh = bool(item.get("fresh_quote", item.get("is_fresh", False)))

    reasons: List[str] = []
    category = "unknown"

    if not fresh or _has(flags, "quote_not_fresh"):
        reasons.append("行情不新鲜")
        category = "data_not_ready"

    if risk_pct > 8 or _has(flags, "risk_pct_too_high"):
        reasons.append("风险比例过高")
        category = "risk_too_high"

    if trigger_price and current_price < trigger_price:
        reasons.append("价格未突破触发价")
        if category == "unknown":
            category = "price_not_triggered"

    if _has(flags, "no_breakout"):
        reasons.append("未突破")
        if category == "unknown":
            category = "price_not_triggered"

    if _has(flags, "too_hot_today"):
        reasons.append("当日过热")
        if category == "unknown":
            category = "too_hot"

    if _has(flags, "weekly_too_hot") or _has(flags, "weekly_downtrend_hard"):
        reasons.append("周线过热/极弱")
        if category == "unknown":
            category = "weekly_risk"

    if _has(flags, "sector_data_missing"):
        reasons.append("板块数据缺失")
        if category == "unknown":
            category = "sector_missing"

    if _has(flags, "yuanjun_data_missing"):
        reasons.append("援军数据缺失")
        if category == "unknown":
            category = "yuanjun_missing"

    if not reasons:
        reasons.append("未命中明确诊断规则")
        category = "other"

    # 接近度评分：越高越接近可交易
    closeness = 0.0
    closeness += min(40, daily / 100 * 40)
    if fresh:
        closeness += 10
    if risk_pct <= 8:
        closeness += 20
    elif risk_pct <= 12:
        closeness += 8
    if trigger_price and current_price >= trigger_price:
        closeness += 15
    elif trigger_price and current_price >= trigger_price * 0.98:
        closeness += 8
    if _f(item.get("weekly_score"), 50) >= 55:
        closeness += 5
    if _f(item.get("sector_score"), 50) >= 55:
        closeness += 5
    if _f(item.get("yuanjun_score"), 50) >= 55:
        closeness += 5

    return {
        "symbol": item.get("symbol") or item.get("code"),
        "stock_name": item.get("stock_name") or item.get("name"),
        "signal_status": item.get("signal_status"),
        "category": category,
        "reasons": reasons,
        "closeness_score": round(closeness, 2),
        "daily_2buy_score": daily,
        "risk_pct": risk_pct,
        "current_price": current_price,
        "trigger_price": trigger_price,
        "fresh_quote": fresh,
        "weekly_score": _f(item.get("weekly_score"), 50),
        "sector_score": _f(item.get("sector_score"), 50),
        "leader_score": _f(item.get("leader_score"), 50),
        "yuanjun_score": _f(item.get("yuanjun_score"), 50),
    }


def diagnose_tail_candidates(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    diagnostics = [diagnose_one_candidate(x) for x in items]
    category_counter = Counter(x["category"] for x in diagnostics)
    status_counter = Counter(x.get("signal_status") for x in items)

    top_near = sorted(
        diagnostics,
        key=lambda x: (
            x["closeness_score"],
            -x["risk_pct"] if x["risk_pct"] <= 8 else -999,
        ),
        reverse=True,
    )[:20]

    risk_ok = [x for x in diagnostics if x["risk_pct"] <= 8]
    price_triggered = [x for x in diagnostics if x["trigger_price"] and x["current_price"] >= x["trigger_price"]]
    fresh_ok = [x for x in diagnostics if x["fresh_quote"]]

    return {
        "total": len(items),
        "status_counter": dict(status_counter),
        "category_counter": dict(category_counter),
        "fresh_ok_count": len(fresh_ok),
        "risk_ok_count": len(risk_ok),
        "price_triggered_count": len(price_triggered),
        "top_near_candidates": top_near,
        "diagnostics": diagnostics,
    }


def format_tail_diagnosis_report(items: List[Dict[str, Any]]) -> str:
    d = diagnose_tail_candidates(items)
    lines: List[str] = []
    lines.append("【尾盘候选诊断 v2.5.6】")
    lines.append("")
    lines.append(f"候选总数：{d['total']}")
    lines.append(f"行情新鲜：{d['fresh_ok_count']}")
    lines.append(f"风险合格 risk_pct<=8：{d['risk_ok_count']}")
    lines.append(f"价格已触发：{d['price_triggered_count']}")
    lines.append("")
    lines.append("信号分布：")
    for k, v in d["status_counter"].items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("拒绝/降级主因：")
    for k, v in d["category_counter"].items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("最接近可交易 Top10：")
    for x in d["top_near_candidates"][:10]:
        lines.append(
            f"- {x['symbol']} {x.get('stock_name') or ''} | close={x['closeness_score']} | "
            f"daily={x['daily_2buy_score']} risk={x['risk_pct']} "
            f"price={x['current_price']} trigger={x['trigger_price']} | {','.join(x['reasons'])}"
        )
    return "\n".join(lines)
