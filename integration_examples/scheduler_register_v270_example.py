"""
v2.7.0 APScheduler 接入示例。

把 register_tail_jobs_v270(scheduler) 接入你的真实 scheduler 初始化逻辑即可。
"""

def register_tail_jobs_v270(scheduler):
    scheduler.add_job(
        func=build_observe_gate_1440_job,
        trigger="cron",
        hour=14,
        minute=40,
        id="build_observe_gate_1440_v270",
        replace_existing=True,
        max_instances=1,
    )

    scheduler.add_job(
        func=tail_confirm_1450_job,
        trigger="cron",
        hour=14,
        minute=50,
        id="tail_confirm_1450_v270",
        replace_existing=True,
        max_instances=1,
    )


def build_observe_gate_1440_job():
    import datetime as dt
    from core.scheduler_tail_jobs_v270 import build_observe_gate_job_v270

    return build_observe_gate_job_v270(
        root=".",
        trade_date=dt.date.today().isoformat(),
        force=False,
        fetch_xgb_if_empty=True,
    )


def tail_confirm_1450_job():
    import datetime as dt
    from core.scheduler_tail_jobs_v270 import tail_confirm_job_v270

    return tail_confirm_job_v270(
        root=".",
        trade_date=dt.date.today().isoformat(),
        force=False,
        persist_tail=True,
    )
