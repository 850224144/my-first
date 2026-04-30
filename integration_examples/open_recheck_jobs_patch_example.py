"""
09:26 / 09:30 开盘复核任务接入示例。

重点：
- 只检查昨晚 trade_plan 里的 OPEN_RECHECK 标的
- 不扫全市场
"""

from core.open_recheck import filter_open_recheck_plans, open_recheck, open_confirm
from core.paper_trade_ext import build_paper_trade_record


def job_open_recheck_0926(*, trade_plans, quote_map, market_state="normal"):
    """
    trade_plans: 昨晚 trade_plan 列表
    quote_map: {symbol: quote}
    """
    results = []
    for plan in filter_open_recheck_plans(trade_plans):
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue
        result = open_recheck(plan, quote, market_state=market_state)
        results.append({**plan, **result})
        # TODO: 企业微信提醒 result
    return results


def job_open_confirm_0930(*, recheck_passed_plans, quote_map, trade_date, trading_days=None, market_state="normal"):
    results = []
    for plan in recheck_passed_plans:
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue

        result = open_confirm(plan, quote, market_state=market_state)
        enriched = {**plan, **result}
        if result.get("should_write_paper_trade"):
            record = build_paper_trade_record(
                candidate=enriched,
                signal={**result, "signal_status": result["open_status"]},
                trade_date=trade_date,
                entry_type="next_open",
                trading_days=trading_days,
            )
            # TODO: 调用你现有 paper_trader 写入函数
            # paper_trader.insert_trade(record)
            enriched["paper_trade_record"] = record

        results.append(enriched)
    return results
