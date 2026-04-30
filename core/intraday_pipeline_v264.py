"""
v2.6.4 盘中尾盘一键流程。

流程：
1. build_observe_gate_outputs_v261
2. run_tail_confirm_from_tail_focus_v264
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt

from .observe_gate_store_v261 import build_observe_gate_outputs_v261
from .tail_confirm_runner_v264 import run_tail_confirm_from_tail_focus_v264


def run_intraday_tail_pipeline_v264(
    *,
    trade_date: Optional[str] = None,
    root: str | Path = ".",
    persist_tail: bool = False,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    root = Path(root)

    observe = build_observe_gate_outputs_v261(
        trade_date=trade_date,
        watchlist_path=root / "data" / "watchlist.parquet",
        duckdb_path=root / "data" / "stock_data.duckdb",
        xgb_cache_root=root / "data" / "xgb",
        output_quality_path=root / "data" / "watchlist_quality.parquet",
        output_tail_focus_path=root / "data" / "watchlist_tail_focus.parquet",
        output_low_priority_path=root / "data" / "watchlist_low_priority.parquet",
        report_dir=root / "data" / "reports",
        limit=500,
        fetch_xgb_if_empty=fetch_xgb_if_empty,
    )

    tail = run_tail_confirm_from_tail_focus_v264(
        trade_date=trade_date,
        tail_focus_path=root / "data" / "watchlist_tail_focus.parquet",
        output_results_path=root / "data" / "tail_confirm_results_v264.parquet",
        report_dir=root / "data" / "reports",
        db_path=str(root / "data" / "trading_state.db"),
        persist=persist_tail,
    )

    return {
        "trade_date": trade_date,
        "observe": observe,
        "tail_confirm": tail,
    }
