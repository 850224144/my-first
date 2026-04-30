"""
v2.6.5 尾盘原因压缩器。

目标：
- 去重
- 中英混合 key 中文化
- 拆分串联字段
- REJECTED/WATCH_ONLY/BUY 分别使用不同优先级
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


KEY_MAP = {
    "volume_not_confirm": "量能未确认(volume_not_confirm)",
    "volatility_not_contracting": "波动未收敛(volatility_not_contracting)",
    "no_breakout": "未突破(no_breakout)",
    "lows_not_rising": "低点抬高不充分(lows_not_rising)",
    "price_not_triggered": "价格未触发(price_not_triggered)",
    "weekly_downtrend_hard": "周线极弱/过热(weekly_downtrend_hard)",
    "weekly_too_hot": "周线过热(weekly_too_hot)",
    "risk_pct_too_high": "风险比例过高(risk_pct_too_high)",
    "too_hot_today": "当日过热(too_hot_today)",
    "pullback_days_not_ideal": "回调天数不理想(pullback_days_not_ideal)",
}

CANONICAL_PRIORITY = [
    "risk_high",
    "price_not_triggered",
    "score_not_enough",
    "no_breakout",
    "volume_not_confirm",
    "volatility_not_contracting",
    "lows_not_rising",
    "weekly_risk",
    "too_hot_today",
    "pullback_not_ideal",
    "watch_structure",
    "other",
]


def _flatten(*values: Any) -> List[str]:
    out: List[str] = []
    for v in values:
        out.extend(normalize_list_field(v))
    return [str(x).strip() for x in out if str(x).strip()]


def split_reason_text(text: str) -> List[str]:
    s = str(text).strip()
    if not s:
        return []

    # JSON/array 字符串已经由 normalize_list_field 尽力处理过；这里处理串联字段
    # 例如：操作建议：等待确认;入场类型：量能未确认;风险等级：偏高
    parts = [s]
    for sep in ["；", ";", "，", ",", "|"]:
        if sep in s:
            parts = [x.strip() for x in re.split(r"[；;，,\|]", s) if x.strip()]
            break
    return parts


def normalize_reason_text(text: str) -> str:
    s = str(text).strip()
    if not s:
        return ""

    # 先替换英文 key
    for key, label in KEY_MAP.items():
        if s == key:
            return label
        if key in s and label not in s:
            # 如果原始是“量能未确认(volume_not_confirm)”就不重复替换
            if "(" in s and ")" in s:
                return s
            return label

    # 中文同义归一
    if "量能未确认" in s:
        return KEY_MAP["volume_not_confirm"]
    if "波动未收敛" in s:
        return KEY_MAP["volatility_not_contracting"]
    if "低点抬高不充分" in s:
        return KEY_MAP["lows_not_rising"]
    if "未突破" in s and "触发价" not in s:
        return KEY_MAP["no_breakout"]
    if "当前价未触发买入价" in s:
        return KEY_MAP["price_not_triggered"]
    if "周线极弱" in s or "周线/趋势过热" in s:
        return KEY_MAP["weekly_downtrend_hard"]
    if "当日过热" in s:
        return KEY_MAP["too_hot_today"]

    # 清理无意义元信息
    if s.startswith("操作建议"):
        return ""
    if s.startswith("入场类型"):
        return ""
    if s.startswith("风险等级"):
        return ""
    if s in {"等待确认", "正常", "偏高"}:
        return ""

    return s


def canonical_key(reason: str) -> str:
    s = reason
    if "风险比例过高" in s or "risk_pct_too_high" in s:
        return "risk_high"
    if "价格未突破触发价" in s or "price_not_triggered" in s or "价格未触发" in s:
        return "price_not_triggered"
    if "二买总分不足" in s:
        return "score_not_enough"
    if "未突破(no_breakout)" in s or "no_breakout" in s:
        return "no_breakout"
    if "量能未确认" in s or "volume_not_confirm" in s:
        return "volume_not_confirm"
    if "波动未收敛" in s or "volatility_not_contracting" in s:
        return "volatility_not_contracting"
    if "低点抬高不充分" in s or "lows_not_rising" in s:
        return "lows_not_rising"
    if "周线" in s and ("过热" in s or "极弱" in s or "weekly" in s):
        return "weekly_risk"
    if "当日过热" in s or "too_hot_today" in s:
        return "too_hot_today"
    if "回调天数" in s or "pullback_days_not_ideal" in s:
        return "pullback_not_ideal"
    if "结构可观察" in s or "未达到买入触发" in s:
        return "watch_structure"
    return "other"


def compact_reasons(raw: List[str], *, max_items: int = 5) -> List[str]:
    normalized: List[str] = []
    for x in raw:
        for part in split_reason_text(str(x)):
            y = normalize_reason_text(part)
            if y:
                normalized.append(y)

    best_by_key: Dict[str, str] = {}
    for r in normalized:
        k = canonical_key(r)
        # 保留更具体的句子：价格/分数原因一般包含数值
        if k not in best_by_key or len(r) > len(best_by_key[k]):
            best_by_key[k] = r

    sorted_items = sorted(
        best_by_key.items(),
        key=lambda kv: CANONICAL_PRIORITY.index(kv[0]) if kv[0] in CANONICAL_PRIORITY else 999,
    )
    return [v for _, v in sorted_items[:max_items]]


def infer_basic_reasons(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []

    try:
        risk = float(row.get("risk_pct") or 0)
        if risk > 8:
            reasons.append(f"风险比例过高：{risk}% > 8%")
    except Exception:
        pass

    try:
        price = float(row.get("current_price") or row.get("price") or 0)
        trigger = float(row.get("trigger_price") or 0)
        if trigger > 0 and price < trigger:
            reasons.append(f"价格未突破触发价：现价 {price} < 触发价 {trigger}")
    except Exception:
        pass

    try:
        score = float(row.get("daily_2buy_score") or row.get("total_score") or 0)
        if score < 80:
            reasons.append(f"二买总分不足：{score} < 80")
    except Exception:
        pass

    return reasons


def explain_tail_row_v265(row: Dict[str, Any], *, max_items: int = 5) -> Dict[str, Any]:
    status = str(row.get("signal_status") or "")

    base = infer_basic_reasons(row)

    if status in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED", "OPEN_BUY_TRIGGERED"}:
        raw = _flatten(
            row.get("signal_reasons"),
            row.get("upgrade_reasons"),
            row.get("sector_reasons"),
            row.get("yuanjun_reasons"),
        )
        reasons = compact_reasons(raw or ["满足尾盘买入触发条件"], max_items=max_items)
        action = "写入纸面交易" if row.get("should_write_paper_trade") else "买入信号但未写入纸面交易"
    elif status == "WATCH_ONLY":
        raw = _flatten(
            row.get("signal_reasons"),
            row.get("downgrade_flags"),
            row.get("blocking_flags"),
            row.get("risk_flags"),
            row.get("risk_reasons"),
        )
        reasons = compact_reasons(base + raw, max_items=max_items)
        action = "继续观察，不建仓"
    elif status == "NEAR_TRIGGER":
        raw = _flatten(
            row.get("signal_reasons"),
            row.get("downgrade_flags"),
            row.get("risk_flags"),
            row.get("risk_reasons"),
        )
        reasons = compact_reasons(base + raw, max_items=max_items)
        action = "接近触发，提醒但不建仓"
    else:
        raw = _flatten(
            row.get("blocking_flags"),
            row.get("downgrade_flags"),
            row.get("risk_flags"),
            row.get("risk_reasons"),
        )
        reasons = compact_reasons(base + raw, max_items=max_items)
        action = "拒绝买入"

    if not reasons:
        reasons = ["未满足尾盘买入触发条件"]

    return {
        "symbol": row.get("symbol") or row.get("code"),
        "stock_name": row.get("stock_name") or row.get("name"),
        "signal_status": status,
        "explain_reasons": reasons,
        "suggested_action": action,
    }


def add_tail_explanations_v265(rows: List[Dict[str, Any]], *, max_items: int = 5) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        item = dict(r)
        exp = explain_tail_row_v265(item, max_items=max_items)
        item["explain_reasons"] = exp["explain_reasons"]
        item["suggested_action"] = exp["suggested_action"]
        return_symbol = exp.get("symbol")
        if return_symbol and not item.get("symbol"):
            item["symbol"] = return_symbol
        out.append(item)
    return out
