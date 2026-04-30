"""
真实 09:26 / 09:30 接入示例。
"""

from core.pipeline_v252 import process_open_recheck_v252, process_open_confirm_v252
from core.open_recheck import filter_open_recheck_plans


def handle_real_open_recheck_0926_v252(trade_plans, quote_map, *, trade_date, send_wecom_func=None):
    results = []
    passed = []
    for plan in filter_open_recheck_plans(trade_plans):
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue
        out = process_open_recheck_v252(
            plan,
            quote,
            trade_date=trade_date,
            db_path="data/trading_state.db",
            persist=True,
            allow_demo=False,
        )
        results.append(out)
        if out.get("open_status") == "OPEN_RECHECK_PASSED":
            passed.append(out)
        if send_wecom_func:
            send_wecom_func(out["wecom_message"])
    return results, passed


def handle_real_open_confirm_0930_v252(passed_plans, quote_map, *, trade_date, trading_days=None, send_wecom_func=None):
    results = []
    for plan in passed_plans:
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue
        out = process_open_confirm_v252(
            plan,
            quote,
            trade_date=trade_date,
            trading_days=trading_days,
            db_path="data/trading_state.db",
            persist=True,
            allow_demo=False,
        )
        results.append(out)
        if send_wecom_func:
            send_wecom_func(out["wecom_message"])
    return results
