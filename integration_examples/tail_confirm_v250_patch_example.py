"""
tail_confirm v2.5.0 接入示例。

不要直接覆盖你的 tail_confirm 文件。
把下面逻辑合并到当前 tail_confirm 的最终判断位置。

核心变化：
- 原来 tail_confirm 自己判断 BUY/NEAR
- 现在调用 build_final_signal(candidate)
- 只有 BUY_TRIGGERED / STRONG_BUY_TRIGGERED 写入 paper_trader
"""

from core.final_signal_engine import build_final_signal, is_buy_signal
from core.paper_trade_ext import build_paper_trade_record


def handle_tail_candidate(candidate, *, trade_date, trading_days=None, strategy_config=None):
    signal = build_final_signal(candidate, config=strategy_config)

    enriched = dict(candidate)
    enriched.update(signal)

    if is_buy_signal(signal):
        paper_record = build_paper_trade_record(
            candidate=enriched,
            signal=signal,
            trade_date=trade_date,
            entry_type="close_tail",
            trading_days=trading_days,
        )
        # TODO: 调用你现有 paper_trader 写入函数
        # paper_trader.insert_trade(paper_record)
        enriched["paper_trade_record"] = paper_record

    return enriched


def format_tail_confirm_v250_message(candidate):
    lines = []
    lines.append("【尾盘确认 v2.5.0】")
    lines.append("")
    lines.append(f"股票：{candidate.get('stock_name', '')} {candidate.get('symbol') or candidate.get('code')}")
    lines.append(f"状态：{candidate.get('signal_status', '-')}")
    lines.append("")
    lines.append(f"日线二买分：{candidate.get('daily_2buy_score', candidate.get('total_score', '-'))}")
    lines.append(f"题材：{candidate.get('theme_name', '-')}")
    lines.append(f"板块分：{candidate.get('sector_score', '-')}")
    lines.append(f"龙头类型：{candidate.get('leader_type', '-')}")
    lines.append(f"龙头分：{candidate.get('leader_score', '-')}")
    lines.append(f"周线分：{candidate.get('weekly_score', '-')}")
    lines.append(f"援军分：{candidate.get('yuanjun_score', '-')}")
    lines.append(f"风险比例：{candidate.get('risk_pct', '-')}")
    lines.append("")
    if candidate.get("signal_reasons"):
        lines.append("触发原因：")
        for x in candidate["signal_reasons"][:8]:
            lines.append(f"- {x}")
    if candidate.get("upgrade_reasons"):
        lines.append("")
        lines.append("加分原因：")
        for x in candidate["upgrade_reasons"][:8]:
            lines.append(f"- {x}")
    if candidate.get("downgrade_flags"):
        lines.append("")
        lines.append("降级原因：")
        for x in candidate["downgrade_flags"][:8]:
            lines.append(f"- {x}")
    if candidate.get("blocking_flags"):
        lines.append("")
        lines.append("拒绝原因：")
        for x in candidate["blocking_flags"][:8]:
            lines.append(f"- {x}")

    action = "写入纸面交易" if candidate.get("should_write_paper_trade") else "不建仓"
    lines.append("")
    lines.append(f"系统动作：{action}")
    return "\n".join(lines)
