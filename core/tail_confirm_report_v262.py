"""
v2.6.2 尾盘确认报告。
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter

from .safe_fields_v262 import stringify_list, json_dumps_safe


def build_tail_confirm_summary_v262(
    *,
    trade_date: str,
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    status_counter = Counter(str(x.get("signal_status")) for x in results)
    quality_counter = Counter(str(x.get("observe_quality")) for x in results)
    action_counter = Counter("write_paper" if x.get("should_write_paper_trade") else "notify_only" for x in results)

    buy_rows = [x for x in results if x.get("should_write_paper_trade")]
    near_rows = [x for x in results if x.get("signal_status") == "NEAR_TRIGGER"]
    watch_rows = [x for x in results if x.get("signal_status") == "WATCH_ONLY"]

    return {
        "trade_date": trade_date,
        "total": len(results),
        "status_counter": dict(status_counter),
        "quality_counter": dict(quality_counter),
        "action_counter": dict(action_counter),
        "buy_count": len(buy_rows),
        "near_count": len(near_rows),
        "watch_count": len(watch_rows),
        "buy_symbols": [
            {
                "symbol": x.get("symbol") or x.get("code"),
                "stock_name": x.get("stock_name") or x.get("name"),
                "signal_status": x.get("signal_status"),
                "risk_pct": x.get("risk_pct"),
                "current_price": x.get("current_price"),
                "trigger_price": x.get("trigger_price"),
                "stop_loss": x.get("stop_loss"),
                "reasons": x.get("signal_reasons") or x.get("upgrade_reasons") or [],
            }
            for x in buy_rows
        ],
    }


def format_tail_confirm_summary_md_v262(summary: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append(f"# Tail Confirm Summary {summary.get('trade_date')}")
    lines.append("")
    lines.append("## 总览")
    lines.append("")
    lines.append(f"- 输入候选：{summary.get('total')}")
    lines.append(f"- 买入信号：{summary.get('buy_count')}")
    lines.append(f"- NEAR_TRIGGER：{summary.get('near_count')}")
    lines.append(f"- WATCH_ONLY：{summary.get('watch_count')}")
    lines.append("")
    lines.append("## 信号分布")
    lines.append("")
    for k, v in (summary.get("status_counter") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## 观察质量分布")
    lines.append("")
    for k, v in (summary.get("quality_counter") or {}).items():
        lines.append(f"- {k}: {v}")

    lines.append("")
    lines.append("## 尾盘确认明细")
    lines.append("")

    if not results:
        lines.append("- 无")
    else:
        for x in results:
            reasons = (
                x.get("signal_reasons")
                or x.get("blocking_flags")
                or x.get("downgrade_flags")
                or x.get("risk_reasons")
                or []
            )
            lines.append(
                f"- {x.get('symbol') or x.get('code')} {x.get('stock_name') or x.get('name') or ''} | "
                f"{x.get('signal_status')} | "
                f"quality={x.get('observe_quality')} "
                f"risk={x.get('risk_pct')} "
                f"price={x.get('current_price')} "
                f"trigger={x.get('trigger_price')} | "
                f"{stringify_list(reasons)}"
            )

    return "\n".join(lines)
