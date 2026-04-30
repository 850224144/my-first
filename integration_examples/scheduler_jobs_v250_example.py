"""
APScheduler 新增任务示例。

把下面两行合并到你当前 scheduler job 注册逻辑中。
函数名需要替换成你项目里的实际函数。
"""

def register_v250_jobs(scheduler):
    # 建议 09:26，不要卡 09:25:00，避免接口开盘价还没稳定。
    scheduler.add_job(
        func=job_open_recheck_0926,
        trigger="cron",
        hour=9,
        minute=26,
        id="open_recheck_0926",
        replace_existing=True,
    )

    # 09:30 连续竞价开始后再确认一次。
    scheduler.add_job(
        func=job_open_confirm_0930,
        trigger="cron",
        hour=9,
        minute=30,
        id="open_confirm_0930",
        replace_existing=True,
    )
