"""
v2.5.7 tail_confirm 接入示例。

逻辑：
1. 正常使用实时行情 + stock_daily + XGB core_pools
2. 如果 XGB core_pools 没数据，则使用 sector_hot.parquet 兜底 sector_score/theme_name
3. 不伪造 leader_score/yuanjun_score
"""

from core.watchlist_pipeline_v257 import preview_watchlist_with_sector_fallback_v257

def run_tail_confirm_with_sector_fallback_v257(trade_date):
    return preview_watchlist_with_sector_fallback_v257(
        watchlist_path="data/watchlist.parquet",
        duckdb_path="data/stock_data.duckdb",
        xgb_cache_root="data/xgb",
        sector_hot_path="data/sector_hot.parquet",
        trade_date=trade_date,
        limit=200,
        allow_xgb_fetch=False,
    )
