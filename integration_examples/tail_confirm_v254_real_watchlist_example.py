"""
v2.5.4 真实 watchlist -> tail_confirm 接入示例。

特点：
- 自动从 DuckDB realtime_quote 获取实时价格
- 自动从 DuckDB stock_daily 读日 K 算周线
- 没有选股宝 core_pools 时，板块/援军只降级，不伪造强信号
"""

from core.watchlist_pipeline_v254 import (
    load_watchlist_rows,
    build_realtime_quote_map_for_watchlist_v254,
    build_candidates_from_watchlist_v254,
    enrich_candidates_from_project_data_v254,
)
from core.pipeline_v252 import process_tail_candidate_v252


def run_tail_confirm_from_real_watchlist_v254(
    *,
    trade_date,
    core_pools=None,
    trading_days=None,
    send_wecom_func=None,
):
    rows = load_watchlist_rows("data/watchlist.parquet")
    quote_map = build_realtime_quote_map_for_watchlist_v254(
        rows,
        duckdb_path="data/stock_data.duckdb",
        trade_date=trade_date,
    )
    candidates = build_candidates_from_watchlist_v254(rows, quote_map=quote_map)
    candidates = enrich_candidates_from_project_data_v254(
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
