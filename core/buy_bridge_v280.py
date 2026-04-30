"""
v2.8.0 BUY -> paper trade / open recheck bridge.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import Counter
import datetime as dt
import json

from .parquet_safe_writer_v263 import read_parquet_safe_v263, write_parquet_safe_v263
from .trade_plan_store_v280 import append_parquet_dedup_v280
from .open_recheck_plan_v280 import (
    build_open_recheck_plan_v280,
    build_paper_trade_candidate_v280,
)


BUY_STATUSES = {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED", "OPEN_BUY_TRIGGERED"}


def is_buy_row_v280(row: Dict[str, Any]) -> bool:
    status = str(row.get("signal_status") or "")
    should = row.get("should_write_paper_trade")
    if isinstance(should, str):
        should = should.lower() in {"1", "true", "yes"}
    return status in BUY_STATUSES and bool(should)


def build_buy_bridge_v280(
    *,
    trade_date: Optional[str] = None,
    tail_results_path: str | Path = "data/tail_confirm_results_v265.parquet",
    paper_candidates_path: str | Path = "data/paper_trade_candidates.parquet",
    open_recheck_path: str | Path = "data/trade_plan_open_recheck.parquet",
    report_dir: str | Path = "data/reports",
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()

    rows = read_parquet_safe_v263(tail_results_path)
    buy_rows = [r for r in rows if is_buy_row_v280(r)]

    paper_rows = [build_paper_trade_candidate_v280(r, trade_date=trade_date) for r in buy_rows]
    plan_rows = [build_open_recheck_plan_v280(r, trade_date=trade_date) for r in buy_rows]

    final_paper = append_parquet_dedup_v280(
        path=paper_candidates_path,
        new_rows=paper_rows,
        dedupe_keys=["trade_date", "symbol", "signal_status"],
    )
    final_plans = append_parquet_dedup_v280(
        path=open_recheck_path,
        new_rows=plan_rows,
        dedupe_keys=["trade_date", "plan_date", "symbol", "plan_type"],
    )

    summary = {
        "trade_date": trade_date,
        "tail_results_path": str(tail_results_path),
        "tail_result_rows": len(rows),
        "buy_rows": len(buy_rows),
        "paper_candidates_added": len(paper_rows),
        "open_recheck_plans_added": len(plan_rows),
        "paper_candidates_total": len(final_paper),
        "open_recheck_plans_total": len(final_plans),
        "buy_symbols": [
            {
                "symbol": r.get("symbol") or r.get("code"),
                "stock_name": r.get("stock_name") or r.get("name"),
                "signal_status": r.get("signal_status"),
                "risk_pct": r.get("risk_pct"),
                "buy_price": r.get("current_price") or r.get("trigger_price"),
            }
            for r in buy_rows
        ],
    }

    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"buy_bridge_summary_{trade_date}.json"
    md_path = report_dir / f"buy_bridge_summary_{trade_date}.md"

    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_path.write_text(format_buy_bridge_summary_v280(summary), encoding="utf-8")

    return {
        "summary": summary,
        "summary_json_path": str(json_path),
        "summary_md_path": str(md_path),
        "paper_candidates_path": str(paper_candidates_path),
        "open_recheck_path": str(open_recheck_path),
    }


def format_buy_bridge_summary_v280(summary: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"# BUY Bridge Summary {summary.get('trade_date')}")
    lines.append("")
    lines.append(f"- 尾盘结果行数：{summary.get('tail_result_rows')}")
    lines.append(f"- BUY 行数：{summary.get('buy_rows')}")
    lines.append(f"- 新增纸面交易候选：{summary.get('paper_candidates_added')}")
    lines.append(f"- 新增开盘复核计划：{summary.get('open_recheck_plans_added')}")
    lines.append("")
    lines.append("## BUY 明细")
    rows = summary.get("buy_symbols") or []
    if not rows:
        lines.append("- 无")
    else:
        for r in rows:
            lines.append(
                f"- {r.get('symbol')} {r.get('stock_name') or ''} | "
                f"{r.get('signal_status')} | price={r.get('buy_price')} risk={r.get('risk_pct')}"
            )
    return "\n".join(lines)
