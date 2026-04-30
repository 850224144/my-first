"""
v2.6.1 scheduler 接入示例。

建议新增：
14:40 build_observe_gate_outputs
14:50 tail_confirm 读取 watchlist_tail_focus.parquet
"""

def register_observe_gate_jobs_v261(scheduler):
    scheduler.add_job(
        func=build_observe_gate_outputs_job_v261,
        trigger="cron",
        hour=14,
        minute=40,
        id="build_observe_gate_outputs_1440",
        replace_existing=True,
    )


def build_observe_gate_outputs_job_v261():
    import datetime as dt
    from core.observe_gate_store_v261 import build_observe_gate_outputs_v261

    trade_date = dt.date.today().isoformat()
    return build_observe_gate_outputs_v261(
        trade_date=trade_date,
        watchlist_path="data/watchlist.parquet",
        duckdb_path="data/stock_data.duckdb",
        xgb_cache_root="data/xgb",
        output_quality_path="data/watchlist_quality.parquet",
        output_tail_focus_path="data/watchlist_tail_focus.parquet",
        output_low_priority_path="data/watchlist_low_priority.parquet",
        report_dir="data/reports",
        limit=500,
        fetch_xgb_if_empty=True,
    )
