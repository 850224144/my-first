"""
v2.6.4 scheduler 接入示例。

建议：
14:40 observe gate
14:50 tail confirm

也可以先用一个 14:50 job 一键执行 run_intraday_tail_pipeline_v264。
"""

def register_intraday_tail_jobs_v264(scheduler):
    scheduler.add_job(
        func=intraday_tail_pipeline_job_v264,
        trigger="cron",
        hour=14,
        minute=50,
        id="intraday_tail_pipeline_v264",
        replace_existing=True,
    )


def intraday_tail_pipeline_job_v264():
    import datetime as dt
    from core.intraday_pipeline_v264 import run_intraday_tail_pipeline_v264

    trade_date = dt.date.today().isoformat()
    return run_intraday_tail_pipeline_v264(
        trade_date=trade_date,
        root=".",
        persist_tail=True,
        fetch_xgb_if_empty=True,
    )
