"""
v2.9.0 scheduler 接入示例。

建议：
20:30 daily_report_v290
"""

def register_daily_report_v290_job(scheduler):
    scheduler.add_job(
        func=daily_report_v290_job,
        trigger="cron",
        hour=20,
        minute=30,
        id="daily_report_v290",
        replace_existing=True,
        max_instances=1,
    )


def daily_report_v290_job():
    import datetime as dt
    from core.daily_report_aggregator_v290 import build_daily_report_v290
    from core.wecom_sender_v290 import send_wecom_markdown_v290
    from pathlib import Path

    trade_date = dt.date.today().isoformat()
    result = build_daily_report_v290(trade_date=trade_date, root=".")
    content = Path(result["md_path"]).read_text(encoding="utf-8")

    # 依赖环境变量 WECOM_WEBHOOK_URL
    return send_wecom_markdown_v290(content=content, dry_run=False)
