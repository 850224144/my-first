"""
v2.6.4 Tail Focus 尾盘确认 Runner。

基于 v2.6.3：
- 使用安全 Parquet 写入
- 使用 v2.6.4 原因解释与报告
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt

from .tail_focus_loader_v261 import load_tail_focus_v261
from .pipeline_v252 import process_tail_candidate_v252
from .safe_fields_v262 import clean_record_fields, json_dumps_safe
from .parquet_safe_writer_v263 import write_parquet_safe_v263
from .tail_reason_formatter_v264 import add_tail_explanations_v264
from .tail_confirm_report_v264 import (
    build_tail_confirm_summary_v264,
    format_tail_confirm_summary_md_v264,
    build_tail_daily_section_v264,
)


def _write_text(text: str, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def run_tail_confirm_from_tail_focus_v264(
    *,
    trade_date: Optional[str] = None,
    tail_focus_path: str | Path = "data/watchlist_tail_focus.parquet",
    output_results_path: str | Path = "data/tail_confirm_results_v264.parquet",
    report_dir: str | Path = "data/reports",
    db_path: str = "data/trading_state.db",
    persist: bool = False,
    trading_days: Optional[List[str]] = None,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()

    rows = load_tail_focus_v261(tail_focus_path)
    cleaned_rows = [clean_record_fields(x) for x in rows]

    results: List[Dict[str, Any]] = []
    for c in cleaned_rows:
        out = process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            trading_days=trading_days,
            db_path=db_path,
            persist=persist,
            allow_demo=False,
        )
        for k in [
            "observe_quality",
            "observe_priority",
            "risk_bucket",
            "risk_reasons",
            "can_enter_tail_focus",
            "should_deprioritize",
            "xgb_pools",
            "theme_name",
        ]:
            if k in c:
                out[k] = c[k]
        results.append(clean_record_fields(out))

    results = add_tail_explanations_v264(results)
    write_parquet_safe_v263(results, output_results_path)

    summary = build_tail_confirm_summary_v264(
        trade_date=trade_date,
        results=results,
    )

    report_dir = Path(report_dir)
    json_path = report_dir / f"tail_confirm_summary_v264_{trade_date}.json"
    md_path = report_dir / f"tail_confirm_summary_v264_{trade_date}.md"
    daily_path = report_dir / f"tail_daily_section_{trade_date}.md"

    _write_text(json_dumps_safe(summary), json_path)
    _write_text(format_tail_confirm_summary_md_v264(summary, results), md_path)
    _write_text(build_tail_daily_section_v264(summary, results), daily_path)

    return {
        "trade_date": trade_date,
        "input_rows": len(rows),
        "result_rows": len(results),
        "buy_count": summary["buy_count"],
        "watch_count": summary["watch_count"],
        "rejected_count": summary["rejected_count"],
        "output_results_path": str(output_results_path),
        "summary_json_path": str(json_path),
        "summary_md_path": str(md_path),
        "daily_section_path": str(daily_path),
        "summary": summary,
        "results": results,
    }
