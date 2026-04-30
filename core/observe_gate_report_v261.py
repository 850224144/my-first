"""
v2.6.1 Observe Gate 报告生成。
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter
import json


def _counter(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    return dict(Counter(str(r.get(key)) for r in rows))


def build_observe_gate_summary_v261(
    *,
    trade_date: str,
    quality_rows: List[Dict[str, Any]],
    tail_focus_rows: List[Dict[str, Any]],
    low_priority_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "trade_date": trade_date,
        "total": len(quality_rows),
        "tail_focus_count": len(tail_focus_rows),
        "low_priority_count": len(low_priority_rows),
        "quality_counter": _counter(quality_rows, "observe_quality"),
        "risk_bucket_counter": _counter(quality_rows, "risk_bucket"),
        "status_counter": _counter(quality_rows, "signal_status"),
        "tail_focus_symbols": [
            {
                "symbol": r.get("symbol") or r.get("code"),
                "stock_name": r.get("stock_name") or r.get("name"),
                "observe_quality": r.get("observe_quality"),
                "observe_priority": r.get("observe_priority"),
                "risk_pct": r.get("risk_pct"),
                "daily_2buy_score": r.get("daily_2buy_score"),
                "current_price": r.get("current_price"),
                "trigger_price": r.get("trigger_price"),
                "signal_status": r.get("signal_status"),
                "risk_reasons": r.get("risk_reasons"),
            }
            for r in tail_focus_rows[:50]
        ],
    }


def format_observe_gate_summary_md_v261(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Observe Gate Summary {summary.get('trade_date')}")
    lines.append("")
    lines.append("## 总览")
    lines.append("")
    lines.append(f"- 候选总数：{summary.get('total')}")
    lines.append(f"- 尾盘重点候选：{summary.get('tail_focus_count')}")
    lines.append(f"- 低优先级/噪音候选：{summary.get('low_priority_count')}")
    lines.append("")
    lines.append("## 观察质量分布")
    lines.append("")
    for k, v in (summary.get("quality_counter") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## 风险分布")
    lines.append("")
    for k, v in (summary.get("risk_bucket_counter") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## 信号分布")
    lines.append("")
    for k, v in (summary.get("status_counter") or {}).items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## 尾盘重点候选")
    lines.append("")
    rows = summary.get("tail_focus_symbols") or []
    if not rows:
        lines.append("- 无")
    else:
        for r in rows:
            reasons = r.get("risk_reasons") or []
            lines.append(
                f"- {r.get('symbol')} {r.get('stock_name') or ''} | "
                f"{r.get('observe_quality')} p={r.get('observe_priority')} "
                f"risk={r.get('risk_pct')} daily={r.get('daily_2buy_score')} "
                f"price={r.get('current_price')} trigger={r.get('trigger_price')} "
                f"status={r.get('signal_status')} | "
                f"{'，'.join(str(x) for x in reasons[:4])}"
            )
    return "\n".join(lines)


def dumps_json_v261(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)
