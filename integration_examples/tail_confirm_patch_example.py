"""
tail_confirm 接入示例。

v2.4.0 原则：
- 只把板块/周线/援军评分展示出来
- 不强制改变原有 BUY_TRIGGERED 写入逻辑
- 等 v2.5.0 再让 build_final_signal 正式接管
"""

def format_tail_confirm_message(candidate):
    symbol = candidate.get("symbol") or candidate.get("code")
    name = candidate.get("stock_name") or candidate.get("name") or ""

    lines = []
    lines.append("【尾盘确认 v2.4.0 评分预览】")
    lines.append("")
    lines.append(f"股票：{name} {symbol}")
    lines.append(f"状态预览：{candidate.get('signal_status', '-')}")
    lines.append("")
    lines.append(f"日线二买分：{candidate.get('daily_2buy_score', candidate.get('total_score', '-'))}")
    lines.append(f"板块：{candidate.get('theme_name', '-')}")
    lines.append(f"板块分：{candidate.get('sector_score', '-')}")
    lines.append(f"龙头类型：{candidate.get('leader_type', '-')}")
    lines.append(f"龙头分：{candidate.get('leader_score', '-')}")
    lines.append(f"周线分：{candidate.get('weekly_score', '-')}")
    lines.append(f"周线状态：{candidate.get('weekly_state', '-')}")
    lines.append(f"援军分：{candidate.get('yuanjun_score', '-')}")
    lines.append(f"援军状态：{candidate.get('yuanjun_state', '-')}")
    lines.append(f"风险比例：{candidate.get('risk_pct', '-')}")
    lines.append("")
    reasons = candidate.get("signal_reasons") or []
    flags = candidate.get("risk_flags") or []
    if reasons:
        lines.append("触发/加分原因：")
        for r in reasons[:8]:
            lines.append(f"- {r}")
    if flags:
        lines.append("")
        lines.append("风险/未确认原因：")
        for f in flags[:8]:
            lines.append(f"- {f}")
    lines.append("")
    lines.append("系统动作：v2.4.0 仅评分展示，不改变原买入逻辑。")
    return "\n".join(lines)
