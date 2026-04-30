"""
v2.5.9 tail_confirm 接入示例。
"""

from core.watchlist_pipeline_v259 import preview_watchlist_with_xgb_clean_v259

def run_tail_confirm_v259(trade_date):
    return preview_watchlist_with_xgb_clean_v259(
        watchlist_path="data/watchlist.parquet",
        duckdb_path="data/stock_data.duckdb",
        xgb_cache_root="data/xgb",
        trade_date=trade_date,
        limit=200,
        fetch_xgb_if_empty=True,
    )
