"""
v2.8.0 接入示例。

建议在 14:50 tail_confirm 后，立刻运行 build_buy_bridge_v280。
"""

def after_tail_confirm_buy_bridge_job_v280():
    import datetime as dt
    from core.buy_bridge_v280 import build_buy_bridge_v280

    trade_date = dt.date.today().isoformat()
    return build_buy_bridge_v280(
        trade_date=trade_date,
        tail_results_path="data/tail_confirm_results_v265.parquet",
        paper_candidates_path="data/paper_trade_candidates.parquet",
        open_recheck_path="data/trade_plan_open_recheck.parquet",
        report_dir="data/reports",
    )
