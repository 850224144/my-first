"""
paper_trader v2.5.0 接入示例。

你现有 paper_trader 写入时，建议增加：
- entry_type
- sellable_date
- score 字段
- signal_reasons
- risk_flags
"""

from core.paper_trade_ext import build_paper_trade_record, is_sellable, check_t1_stop_risk


def insert_v250_paper_trade(candidate, signal, *, trade_date, trading_days=None):
    record = build_paper_trade_record(
        candidate=candidate,
        signal=signal,
        trade_date=trade_date,
        entry_type=signal.get("entry_type") or "close_tail",
        trading_days=trading_days,
    )

    # TODO: 映射到你现有 paper_trader 表结构
    # db.insert("paper_trades", record)
    return record


def handle_position_stop_check(position, *, current_price, trade_date):
    result = check_t1_stop_risk(position, current_price=current_price, trade_date=trade_date)
    if result.get("has_stop_risk") and not result.get("can_sell"):
        # TODO: 企业微信提醒 T+1 风险
        pass
    elif result.get("has_stop_risk") and result.get("can_sell"):
        # TODO: 执行纸面止损
        pass
    return result
