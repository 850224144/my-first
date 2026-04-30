"""
v2.6.4 尾盘确认报告。
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter

from .safe_fields_v262 import stringify_list, json_dumps_safe
from .tail_reason_formatter_v264 import add_tail_explanations_v264


def build_tail_confirm_summary_v264(
    *,
    trade_date: str,
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    enriched = add_tail_explanations_v264(results)

    status_counter = Counter(str(x.get("signal_status")) for x in enriched)
    quality_counter = Counter(str(x.get("observe_quality")) for x in enriched)
    action_counter = Counter(str(x.get("suggested_action")) for x in enriched)

    buy_rows = [x for x in enriched if x.get("should_write_paper_trade")]
    watch_rows = [x for x in enriched if x.get("signal_status") == "WATCH_ONLY"]
    rejected_rows = [x for x in enriched if x.get("signal_status") == "REJECTED"]

    near_candidates = sorted(
        enriched,
        key=lambda x: (
            1 if x.get("signal_status") == "WATCH_ONLY" else 0,
            float(x.get("observe_priority") or 0),
            -float(x.get("risk_pct") or 999),
            float(x.get("daily_2buy_score") or 0),
        ),
        reverse=True,
    )[:10]

    return {
        "trade_date": trade_date,
        "total": len(enriched),
        "status_counter": dict(status_counter),
        "quality_counter": dict(quality_counter),
        "action_counter": dict(action_counter),
        "buy_count": len(buy_rows),
        "watch_count": len(watch_rows),
        "rejected_count": len(rejected_rows),
        "buy_symbols": [
            {
                "symbol": x.get("symbol") or x.get("code"),
                "stock_name": x.get("stock_name") or x.get("name"),
                "signal_status": x.get("signal_status"),
                "risk_pct": x.get("risk_pct"),
                "current_price": x.get("current_price"),
                "trigger_price": x.get("trigger_price"),
                "stop_loss": x.get("stop_loss"),
                "reasons": x.get("explain_reasons") or [],
            }
            for x in buy_rows
        ],
        "near_candidates": [
            {
                "symbol": x.get("symbol") or x.get("code"),
                "stock_name": x.get("stock_name") or x.get("name"),
                "signal_status": x.get("signal_status"),
                "observe_quality": x.get("observe_quality"),
                "risk_pct": x.get("risk_pct"),
                "daily_2buy_score": x.get("daily_2buy_score"),
                "current_price": x.get("current_price"),
                "trigger_price": x.get("trigger_price"),
                "reasons": x.get("explain_reasons") or [],
            }
            for x in near_candidates
        ],
    }


def format_tail_confirm_summary_md_v264(summary: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
    rows = add_tail_explanations_v264(results)

    lines: List[str] = []
    lines.append(f"# Tail Confirm Summary v2.6.4 {summary.get('trade_date')}")
    lines.append("")
    lines.append("## 总览")
    lines.append("")
    lines.append(f"- 输入候选：{summary.get('total')}")
    lines.append(f"- 买入信号：{summary.get('buy_count')}")
    lines.append(f"- WATCH_ONLY：{summary.get('watch_count')}")
    lines.append(f"- REJECTED：{summary.get('rejected_count')}")
    lines.append("")
    lines.append("## 信号分布")
    lines.append("")
    for k, v in (summary.get("status_counter") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## 最接近买点")
    lines.append("")
    near = summary.get("near_candidates") or []
    if not near:
        lines.append("- 无")
    else:
        for x in near[:5]:
            lines.append(
                f"- {x.get('symbol')} {x.get('stock_name') or ''} | "
                f"{x.get('signal_status')} | "
                f"risk={x.get('risk_pct')} daily={x.get('daily_2buy_score')} "
                f"price={x.get('current_price')} trigger={x.get('trigger_price')} | "
                f"{stringify_list(x.get('reasons'))}"
            )

    lines.append("")
    lines.append("## 尾盘确认明细")
    lines.append("")
    if not rows:
        lines.append("- 无")
    else:
        for x in rows:
            lines.append(
                f"- {x.get('symbol') or x.get('code')} {x.get('stock_name') or x.get('name') or ''} | "
                f"{x.get('signal_status')} | "
                f"quality={x.get('observe_quality')} "
                f"risk={x.get('risk_pct')} "
                f"daily={x.get('daily_2buy_score')} "
                f"price={x.get('current_price')} "
                f"trigger={x.get('trigger_price')} | "
                f"{stringify_list(x.get('explain_reasons'))}"
            )

    return "\n".join(lines)


def build_tail_daily_section_v264(summary: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
    rows = add_tail_explanations_v264(results)

    lines: List[str] = []
    lines.append("## 尾盘确认")
    lines.append("")
    lines.append(f"- 尾盘重点候选：{summary.get('total')}")
    lines.append(f"- 买入信号：{summary.get('buy_count')}")
    lines.append(f"- 观察：{summary.get('watch_count')}")
    lines.append(f"- 拒绝：{summary.get('rejected_count')}")
    lines.append("")

    buy = [x for x in rows if x.get("should_write_paper_trade")]
    watch = [x for x in rows if x.get("signal_status") == "WATCH_ONLY"]

    if buy:
        lines.append("### 今日买入信号")
        for x in buy:
            lines.append(
                f"- {x.get('symbol')} {x.get('stock_name') or ''} | "
                f"price={x.get('current_price')} stop={x.get('stop_loss')} | "
                f"{stringify_list(x.get('explain_reasons'))}"
            )
        lines.append("")
    else:
        lines.append("### 今日买入信号")
        lines.append("- 无")
        lines.append("")

    lines.append("### 最接近买点")
    near = watch if watch else rows[:3]
    if not near:
        lines.append("- 无")
    else:
        for x in near[:5]:
            lines.append(
                f"- {x.get('symbol')} {x.get('stock_name') or ''} | "
                f"{x.get('signal_status')} | risk={x.get('risk_pct')} "
                f"daily={x.get('daily_2buy_score')} price={x.get('current_price')} "
                f"trigger={x.get('trigger_price')} | "
                f"{stringify_list(x.get('explain_reasons'))}"
            )
    return "\n".join(lines)
