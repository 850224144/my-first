"""
v2.5.7 watchlist pipeline with sector_hot fallback.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt

from .watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255
from .sector_hot_fallback_v257 import apply_sector_hot_fallback
from .pipeline_v252 import process_tail_candidate_v252


def preview_watchlist_with_sector_fallback_v257(
    *,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    xgb_cache_root: str | Path = "data/xgb",
    sector_hot_path: str | Path = "data/sector_hot.parquet",
    trade_date: Optional[str] = None,
    limit: int = 80,
    allow_xgb_fetch: bool = False,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    base_report = preview_final_signals_from_watchlist_v255(
        watchlist_path=watchlist_path,
        duckdb_path=duckdb_path,
        xgb_cache_root=xgb_cache_root,
        trade_date=trade_date,
        limit=limit,
        allow_xgb_fetch=allow_xgb_fetch,
    )

    adjusted = []
    fallback_matched = 0
    for item in base_report["results"]:
        c = apply_sector_hot_fallback(item, path=sector_hot_path)
        if "sector_hot_fallback" in str(c.get("risk_flags")):
            fallback_matched += 1
        # 重新跑 final_signal，persist=False
        adjusted.append(process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            persist=False,
            allow_demo=False,
        ))

    out = dict(base_report)
    out["results_before_sector_fallback"] = base_report["results"]
    out["results"] = adjusted
    out["sector_hot_fallback_matched"] = fallback_matched
    return out
