"""
09:26 / 09:30 开盘复核主流程接入示例。

注意：
这里只处理 trade_plan.parquet 里 plan_type=OPEN_RECHECK 的标的。
"""

from core.pipeline_v251 import process_open_recheck_v251, process_open_confirm_v251
from core.open_recheck import filter_open_recheck_plans


def handle_open_recheck_0926_v251(
    trade_plans,
    quote_map,
    *,
    trade_date,
    market_state="normal",
    db_path="./data/trading_state.db",
    strategy_config=None,
    send_wecom_func=None,
):
    results = []
    passed = []
    for plan in filter_open_recheck_plans(trade_plans):
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue
        out = process_open_recheck_v251(
            plan,
            quote,
            trade_date=trade_date,
            market_state=market_state,
            strategy_config=strategy_config,
            db_path=db_path,
            persist=True,
        )
        results.append(out)
        if out.get("open_status") == "OPEN_RECHECK_PASSED":
            passed.append(out)
        if send_wecom_func:
            send_wecom_func(out["wecom_message"])
    return results, passed


def handle_open_confirm_0930_v251(
    recheck_passed_plans,
    quote_map,
    *,
    trade_date,
    trading_days=None,
    market_state="normal",
    db_path="./data/trading_state.db",
    strategy_config=None,
    send_wecom_func=None,
):
    results = []
    for plan in recheck_passed_plans:
        symbol = plan.get("symbol") or plan.get("code")
        quote = quote_map.get(symbol)
        if not quote:
            continue
        out = process_open_confirm_v251(
            plan,
            quote,
            trade_date=trade_date,
            trading_days=trading_days,
            market_state=market_state,
            strategy_config=strategy_config,
            db_path=db_path,
            persist=True,
        )
        results.append(out)
        if send_wecom_func:
            send_wecom_func(out["wecom_message"])
    return results
