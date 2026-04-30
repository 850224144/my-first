"""
v2.5.8 watchlist + XGB live pools pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt

from .watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255
from .xgb_live_pools_v258 import load_xgb_live_pools_v258, live_pools_report
from .xgb_pool_enricher_v258 import enrich_candidate_with_xgb_pools_v258, xgb_enrich_report
from .pipeline_v252 import process_tail_candidate_v252


def preview_watchlist_with_xgb_live_v258(
    *,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    xgb_cache_root: str | Path = "data/xgb",
    trade_date: Optional[str] = None,
    limit: int = 80,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()

    base = preview_final_signals_from_watchlist_v255(
        watchlist_path=watchlist_path,
        duckdb_path=duckdb_path,
        xgb_cache_root=xgb_cache_root,
        trade_date=trade_date,
        limit=limit,
        allow_xgb_fetch=False,
    )

    pools = load_xgb_live_pools_v258(
        trade_date=trade_date,
        cache_root=xgb_cache_root,
        fetch_if_empty=fetch_xgb_if_empty,
    )

    enriched_candidates = []
    for item in base["results"]:
        enriched = enrich_candidate_with_xgb_pools_v258(item, pools)
        enriched_candidates.append(enriched)

    rerun = []
    for c in enriched_candidates:
        rerun.append(process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            persist=False,
            allow_demo=False,
        ))

    out = dict(base)
    out["xgb_live_pools_report"] = live_pools_report(pools)
    out["xgb_enrich_report"] = xgb_enrich_report(enriched_candidates)
    out["results_before_xgb_live"] = base["results"]
    out["results"] = rerun
    return out
