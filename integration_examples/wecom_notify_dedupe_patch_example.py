"""
企业微信去重接入示例。

目标：
- 同一交易日、同一股票、同一状态不重复发
- 状态变化才发
"""

from core.notify_dedupe import should_notify, record_notification


def send_wecom_with_dedupe(*, db_path, trade_date, symbol, status, message, send_func):
    ok, reason = should_notify(
        db_path,
        trade_date=trade_date,
        symbol=symbol,
        status=status,
        channel="wecom",
        message_key=message[:300],
    )
    if not ok:
        print(f"跳过企业微信提醒：{symbol} {status}，原因：{reason}")
        return False

    send_func(message)

    record_notification(
        db_path,
        trade_date=trade_date,
        symbol=symbol,
        status=status,
        channel="wecom",
        message_key=message[:300],
    )
    return True
