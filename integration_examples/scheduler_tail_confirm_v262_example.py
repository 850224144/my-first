"""
v2.6.2 scheduler 接入示例。

建议：
14:40 build_observe_gate_outputs_v261
14:50 run_tail_confirm_from_tail_focus_v262
"""

def register_tail_confirm_v262_job(scheduler):
    scheduler.add_job(
        func=tail_confirm_v262_job,
        trigger="cron",
        hour=14,
        minute=50,
        id="tail_confirm_v262",
        replace_existing=True,
    )


def tail_confirm_v262_job():
    import datetime as dt
    from core.tail_confirm_runner_v262 import run_tail_confirm_from_tail_focus_v262

    trade_date = dt.date.today().isoformat()
    return run_tail_confirm_from_tail_focus_v262(
        trade_date=trade_date,
        tail_focus_path="data/watchlist_tail_focus.parquet",
        output_results_path="data/tail_confirm_results.parquet",
        report_dir="data/reports",
        db_path="data/trading_state.db",
        persist=True,
    )
