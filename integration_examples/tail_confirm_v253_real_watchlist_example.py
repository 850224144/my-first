"""
v2.5.3 真实 watchlist -> tail_confirm 接入示例。

目标：
- 从 data/watchlist.parquet 读取真实观察池
- 接入实时 quote_map
- 补齐评分
- 调用 process_tail_candidate_v252
"""

from core.watchlist_pipeline_v253 import (
    load_watchlist_rows,
    build_candidates_from_watchlist_v253,
    enrich_candidates_from_project_data_v253,
)
from core.pipeline_v252 import process_tail_candidate_v252


def run_tail_confirm_from_real_watchlist_v253(
    *,
    trade_date,
    quote_map,
    core_pools=None,
    trading_days=None,
    send_wecom_func=None,
):
    rows = load_watchlist_rows("data/watchlist.parquet")
    candidates = build_candidates_from_watchlist_v253(rows, quote_map=quote_map)
    candidates = enrich_candidates_from_project_data_v253(
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
