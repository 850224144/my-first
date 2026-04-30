"""
v2.5.5 真实 watchlist -> tail_confirm 接入示例。
"""

from core.watchlist_pipeline_v255 import (
    load_watchlist_rows,
    build_realtime_quote_map_for_watchlist_v255,
    build_candidates_from_watchlist_v255,
    enrich_candidates_from_project_data_v255,
)
from core.xgb_core_pools_v255 import load_xgb_core_pools_v255
from core.pipeline_v252 import process_tail_candidate_v252


def run_tail_confirm_from_real_watchlist_v255(
    *,
    trade_date,
    trading_days=None,
    send_wecom_func=None,
    allow_xgb_fetch=False,
):
    rows = load_watchlist_rows("data/watchlist.parquet")
    quote_map = build_realtime_quote_map_for_watchlist_v255(
        rows,
        duckdb_path="data/stock_data.duckdb",
        trade_date=trade_date,
    )
    core_pools = load_xgb_core_pools_v255(
        trade_date=trade_date,
        cache_root="data/xgb",
        allow_fetch=allow_xgb_fetch,
    )

    candidates = build_candidates_from_watchlist_v255(rows, quote_map=quote_map)
    candidates = enrich_candidates_from_project_data_v255(
        candidates,
        duckdb_path="data/stock_data.duckdb",
        core_pools=core_pools,
        max_daily_load=None,
    )

    results = []
    for c in candidates:
        out = process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            trading_days=trading_days,
            db_path="data/trading_state.db",
            persist=True,
            allow_demo=False,
        )
        results.append(out)
        if send_wecom_func and out.get("wecom_message"):
            send_wecom_func(out["wecom_message"])

    return results
