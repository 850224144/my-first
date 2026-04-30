"""
v2.5.1 企业微信消息格式化。

目标：
- 强买原因不为空
- 降级/拒绝原因可解释
- 开盘复核消息统一
"""

from __future__ import annotations

from typing import Any, Dict, List


def _fmt(value: Any, default: str = "-") -> str:
    if value is None or value == "":
        return default
    return str(value)


def normalize_signal_reasons(signal: Dict[str, Any]) -> Dict[str, Any]:
    """
    修复 v2.5.0 中 STRONG_BUY_TRIGGERED signal_reasons 为空的问题。
    将 upgrade_reasons 合并到 signal_reasons，保证通知可展示。
    """
    out = dict(signal)
    reasons: List[str] = []
    for key in ["signal_reasons", "upgrade_reasons"]:
        v = out.get(key)
        if isinstance(v, list):
            reasons.extend([str(x) for x in v if str(x)])
        elif isinstance(v, str) and v:
            reasons.append(v)
    if not reasons:
        status = out.get("signal_status")
        if status in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}:
            reasons.append("买入信号触发，但原因字段为空，请检查候选评分字段")
    out["signal_reasons"] = list(dict.fromkeys(reasons))
    return out


def format_final_signal_message(candidate: Dict[str, Any], signal: Dict[str, Any]) -> str:
    signal = normalize_signal_reasons(signal)
    lines: List[str] = []
    lines.append("【尾盘确认 v2.5.1】")
    lines.append("")
    lines.append(f"股票：{_fmt(candidate.get('stock_name') or candidate.get('name'))} {_fmt(candidate.get('symbol') or candidate.get('code'))}")
    lines.append(f"状态：{_fmt(signal.get('signal_status'))}")
    lines.append("")
    lines.append(f"日线二买分：{_fmt(candidate.get('daily_2buy_score', candidate.get('total_score')))}")
    lines.append(f"题材：{_fmt(candidate.get('theme_name'))}")
    lines.append(f"板块分：{_fmt(candidate.get('sector_score'))}")
    lines.append(f"龙头类型：{_fmt(candidate.get('leader_type'))}")
    lines.append(f"龙头分：{_fmt(candidate.get('leader_score'))}")
    lines.append(f"周线分：{_fmt(candidate.get('weekly_score'))}")
    lines.append(f"援军分：{_fmt(candidate.get('yuanjun_score'))}")
    lines.append(f"风险比例：{_fmt(candidate.get('risk_pct'))}")
    lines.append("")
    if signal.get("planned_buy_price"):
        lines.append(f"计划买入价：{signal.get('planned_buy_price')}")
    if signal.get("stop_loss"):
        lines.append(f"撤军线：{signal.get('stop_loss')}")
    if signal.get("target_1"):
        lines.append(f"目标1：{signal.get('target_1')}")
    if signal.get("target_2"):
        lines.append(f"目标2：{signal.get('target_2')}")

    if signal.get("signal_reasons"):
        lines.append("")
        lines.append("触发/加分原因：")
        for r in signal["signal_reasons"][:10]:
            lines.append(f"- {r}")

    if signal.get("downgrade_flags"):
        lines.append("")
        lines.append("降级原因：")
        for r in signal["downgrade_flags"][:10]:
            lines.append(f"- {r}")

    if signal.get("blocking_flags"):
        lines.append("")
        lines.append("拒绝原因：")
        for r in signal["blocking_flags"][:10]:
            lines.append(f"- {r}")

    action = "写入纸面交易" if signal.get("should_write_paper_trade") else "不建仓"
    lines.append("")
    lines.append(f"系统动作：{action}")
    return "\n".join(lines)


def format_open_check_message(plan: Dict[str, Any], result: Dict[str, Any]) -> str:
    lines: List[str] = []
    status = result.get("open_status", "-")
    title = "【开盘复核 v2.5.1】" if "RECHECK" in status else "【开盘确认 v2.5.1】"
    lines.append(title)
    lines.append("")
    lines.append(f"股票：{_fmt(plan.get('stock_name') or plan.get('name'))} {_fmt(plan.get('symbol') or plan.get('code'))}")
    lines.append(f"状态：{_fmt(status)}")
    lines.append("")
    lines.append(f"题材：{_fmt(plan.get('theme_name'))}")
    lines.append(f"日线二买分：{_fmt(plan.get('daily_2buy_score'))}")
    lines.append(f"板块分：{_fmt(plan.get('sector_score'))}")
    lines.append(f"龙头分：{_fmt(plan.get('leader_score'))}")
    lines.append(f"周线分：{_fmt(plan.get('weekly_score'))}")
    lines.append(f"援军分：{_fmt(plan.get('yuanjun_score'))}")
    lines.append("")
    lines.append(f"计划买入价：{_fmt(plan.get('planned_buy_price'))}")
    lines.append(f"援军阳线二分位：{_fmt(plan.get('yj_candle_mid'))}")
    lines.append(f"撤军线：{_fmt(result.get('stop_loss') or plan.get('yj_stop_loss'))}")
    lines.append(f"当前计划价：{_fmt(result.get('planned_buy_price'))}")

    if result.get("reasons"):
        lines.append("")
        lines.append("确认原因：")
        for r in result["reasons"][:10]:
            lines.append(f"- {r}")

    if result.get("risk_flags"):
        lines.append("")
        lines.append("风险/拒绝原因：")
        for r in result["risk_flags"][:10]:
            lines.append(f"- {r}")

    action = "写入纸面交易" if result.get("should_write_paper_trade") else "仅提醒，不建仓"
    lines.append("")
    lines.append(f"系统动作：{action}")
    return "\n".join(lines)
