"""
v2.6.1 Observe Gate 输出落盘。

默认读取 v2.5.9 的全链路预览结果，然后应用 v2.6.0 observe gate。
不会覆盖 data/watchlist.parquet。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt

from .watchlist_pipeline_v259 import preview_watchlist_with_xgb_clean_v259
from .observe_gate_v260 import apply_observe_gate_v260
from .observe_gate_report_v261 import (
    build_observe_gate_summary_v261,
    format_observe_gate_summary_md_v261,
    dumps_json_v261,
)


def _pd():
    import pandas as pd
    return pd


def _write_parquet(rows: List[Dict[str, Any]], path: str | Path) -> None:
    pd = _pd()
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(p, index=False)


def _write_text(text: str, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def build_observe_gate_outputs_v261(
    *,
    trade_date: Optional[str] = None,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    xgb_cache_root: str | Path = "data/xgb",
    output_quality_path: str | Path = "data/watchlist_quality.parquet",
    output_tail_focus_path: str | Path = "data/watchlist_tail_focus.parquet",
    output_low_priority_path: str | Path = "data/watchlist_low_priority.parquet",
    report_dir: str | Path = "data/reports",
    limit: int = 500,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()

    report = preview_watchlist_with_xgb_clean_v259(
        watchlist_path=watchlist_path,
        duckdb_path=duckdb_path,
        xgb_cache_root=xgb_cache_root,
        trade_date=trade_date,
        limit=limit,
        fetch_xgb_if_empty=fetch_xgb_if_empty,
    )

    quality_rows = apply_observe_gate_v260(report["results"])

    tail_focus_rows = [
        r for r in quality_rows
        if r.get("can_enter_tail_focus")
    ]

    low_priority_rows = [
        r for r in quality_rows
        if r.get("should_deprioritize")
    ]

    # 排序：尾盘重点候选按优先级、风险、分数
    tail_focus_rows = sorted(
        tail_focus_rows,
        key=lambda x: (
            float(x.get("observe_priority") or 0),
            -float(x.get("risk_pct") or 999),
            float(x.get("daily_2buy_score") or 0),
        ),
        reverse=True,
    )

    quality_rows = sorted(
        quality_rows,
        key=lambda x: (
            float(x.get("observe_priority") or 0),
            -float(x.get("risk_pct") or 999),
            float(x.get("daily_2buy_score") or 0),
        ),
        reverse=True,
    )

    _write_parquet(quality_rows, output_quality_path)
    _write_parquet(tail_focus_rows, output_tail_focus_path)
    _write_parquet(low_priority_rows, output_low_priority_path)

    summary = build_observe_gate_summary_v261(
        trade_date=trade_date,
        quality_rows=quality_rows,
        tail_focus_rows=tail_focus_rows,
        low_priority_rows=low_priority_rows,
    )

    report_dir = Path(report_dir)
    json_path = report_dir / f"observe_gate_summary_{trade_date}.json"
    md_path = report_dir / f"observe_gate_summary_{trade_date}.md"

    _write_text(dumps_json_v261(summary), json_path)
    _write_text(format_observe_gate_summary_md_v261(summary), md_path)

    return {
        "trade_date": trade_date,
        "input_rows": len(report["results"]),
        "quality_rows": len(quality_rows),
        "tail_focus_rows": len(tail_focus_rows),
        "low_priority_rows": len(low_priority_rows),
        "output_quality_path": str(output_quality_path),
        "output_tail_focus_path": str(output_tail_focus_path),
        "output_low_priority_path": str(output_low_priority_path),
        "summary_json_path": str(json_path),
        "summary_md_path": str(md_path),
        "summary": summary,
    }
