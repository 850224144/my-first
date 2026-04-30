"""
v2.5.8 tail_confirm 接入示例。
"""

from core.watchlist_pipeline_v258 import preview_watchlist_with_xgb_live_v258

def run_tail_confirm_with_xgb_live_v258(trade_date):
    return preview_watchlist_with_xgb_live_v258(
        watchlist_path="data/watchlist.parquet",
        duckdb_path="data/stock_data.duckdb",
        xgb_cache_root="data/xgb",
        trade_date=trade_date,
        limit=200,
        fetch_xgb_if_empty=True,
    )
