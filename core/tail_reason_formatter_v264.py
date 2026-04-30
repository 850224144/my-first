"""
v2.6.4 尾盘原因解释器。

目标：
- REJECTED 不再优先展示“周线较强”这类正向原因
- WATCH_ONLY 展示“为什么只能观察”
- BUY 展示“为什么买”
"""

from __future__ import annotations

from typing import Any, Dict, List
import re

try:
    from .safe_fields_v262 import normalize_list_field
except Exception:
    def normalize_list_field(value):
        if value is None:
            return []
        if isinstance(value, list):
            return [str(x) for x in value if str(x)]
        return [str(value)]


POSITIVE_KEYWORDS = [
    "周线较强",
    "周线强",
    "板块较强",
    "援军确认",
    "题材匹配",
    "入选选股宝",
    "结构可观察",
]

BLOCKING_KEYWORDS = [
    "no_breakout",
    "未突破",
    "price_not_triggered",
    "价格未突破",
    "volume_not_confirm",
    "量能未确认",
    "volatility_not_contracting",
    "波动未收敛",
    "risk_pct_too_high",
    "风险比例过高",
    "too_hot_today",
    "当日过热",
    "lows_not_rising",
    "低点抬高不充分",
    "pullback_days_not_ideal",
    "回调天数不理想",
    "weekly_downtrend_hard",
    "weekly_too_hot",
    "周线/趋势过热",
    "fresh",
    "行情不新鲜",
]


def _flatten_reasons(*values: Any) -> List[str]:
    out: List[str] = []
    for v in values:
        out.extend(normalize_list_field(v))
    cleaned = []
    for x in out:
        s = str(x).strip()
        if s and s.lower() not in {"nan", "none", "null", "[]"}:
            cleaned.append(s)
    return list(dict.fromkeys(cleaned))


def _contains_any(text: str, keywords: List[str]) -> bool:
    return any(k in text for k in keywords)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    return list(dict.fromkeys(str(x) for x in items if str(x).strip()))


def infer_price_reason(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    try:
        price = float(row.get("current_price") or row.get("price") or 0)
        trigger = float(row.get("trigger_price") or 0)
        if trigger > 0 and price < trigger:
            reasons.append(f"价格未突破触发价：现价 {price} < 触发价 {trigger}")
    except Exception:
        pass
    return reasons


def infer_score_reason(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    try:
        score = float(row.get("daily_2buy_score") or row.get("total_score") or 0)
        if score < 80:
            reasons.append(f"二买总分不足：{score} < 80")
    except Exception:
        pass
    return reasons


def infer_risk_reason(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    try:
        risk = float(row.get("risk_pct") or 999)
        if risk > 8:
            reasons.append(f"风险比例过高：{risk}% > 8%")
    except Exception:
        pass
    return reasons


def pick_rejected_reasons(row: Dict[str, Any]) -> List[str]:
    raw = _flatten_reasons(
        row.get("blocking_flags"),
        row.get("downgrade_flags"),
        row.get("risk_flags"),
        row.get("risk_reasons"),
    )

    inferred = []
    inferred.extend(infer_risk_reason(row))
    inferred.extend(infer_price_reason(row))
    inferred.extend(infer_score_reason(row))

    blocking = [x for x in raw if _contains_any(x, BLOCKING_KEYWORDS)]
    neutral = [x for x in raw if not _contains_any(x, POSITIVE_KEYWORDS)]

    reasons = []
    reasons.extend(inferred)
    reasons.extend(blocking)
    reasons.extend(neutral)

    # 避免只有“周线较强”
    reasons = [x for x in reasons if not (len(reasons) > 1 and _contains_any(x, POSITIVE_KEYWORDS))]

    if not reasons:
        reasons = ["未满足尾盘买入触发条件"]

    return _dedupe_keep_order(reasons)[:8]


def pick_watch_only_reasons(row: Dict[str, Any]) -> List[str]:
    raw = _flatten_reasons(
        row.get("signal_reasons"),
        row.get("downgrade_flags"),
        row.get("risk_flags"),
        row.get("risk_reasons"),
    )
    reasons = []
    reasons.extend(infer_score_reason(row))
    reasons.extend(infer_price_reason(row))

    for x in raw:
        if _contains_any(x, BLOCKING_KEYWORDS) or "结构可观察" in x or "未达到买入触发" in x:
            reasons.append(x)

    if not reasons:
        reasons = ["结构可观察，但未达到买入触发"]

    return _dedupe_keep_order(reasons)[:8]


def pick_buy_reasons(row: Dict[str, Any]) -> List[str]:
    raw = _flatten_reasons(
        row.get("signal_reasons"),
        row.get("upgrade_reasons"),
        row.get("sector_reasons"),
        row.get("yuanjun_reasons"),
    )
    reasons = [x for x in raw if not _contains_any(x, ["缺失", "no_match", "未匹配"])]
    if not reasons:
        reasons = ["满足尾盘买入触发条件"]
    return _dedupe_keep_order(reasons)[:10]


def explain_tail_row_v264(row: Dict[str, Any]) -> Dict[str, Any]:
    status = str(row.get("signal_status") or "")
    if status in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED", "OPEN_BUY_TRIGGERED"}:
        reasons = pick_buy_reasons(row)
        action = "写入纸面交易" if row.get("should_write_paper_trade") else "买入信号但未写入纸面交易"
    elif status == "WATCH_ONLY":
        reasons = pick_watch_only_reasons(row)
        action = "继续观察，不建仓"
    elif status == "NEAR_TRIGGER":
        reasons = pick_watch_only_reasons(row)
        action = "接近触发，提醒但不建仓"
    else:
        reasons = pick_rejected_reasons(row)
        action = "拒绝买入"

    return {
        "symbol": row.get("symbol") or row.get("code"),
        "stock_name": row.get("stock_name") or row.get("name"),
        "signal_status": status,
        "explain_reasons": reasons,
        "suggested_action": action,
    }


def add_tail_explanations_v264(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        exp = explain_tail_row_v264(item)
        item["explain_reasons"] = exp["explain_reasons"]
        item["suggested_action"] = exp["suggested_action"]
        out.append(item)
    return out
