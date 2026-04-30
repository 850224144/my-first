"""
v2.5.1 主流程安全接入封装。

用途：
- tail_confirm 里调用 process_tail_candidate_v251()
- open_recheck/open_confirm 里调用 process_open_recheck_v251()/process_open_confirm_v251()
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List

from .final_signal_engine import build_final_signal, is_buy_signal
from .paper_trade_ext import build_paper_trade_record
from .open_recheck import open_recheck, open_confirm
from .message_formatter_v251 import normalize_signal_reasons, format_final_signal_message, format_open_check_message
from .trade_plan_v251 import should_create_open_recheck_plan, build_open_recheck_plan
from .state_store_v251 import upsert_final_signal, insert_open_recheck, insert_paper_trade_ext, DEFAULT_DB_PATH


def process_tail_candidate_v251(
    candidate: Dict[str, Any],
    *,
    trade_date: str,
    trading_days: Optional[List[str]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = DEFAULT_DB_PATH,
    persist: bool = True,
) -> Dict[str, Any]:
    """
    尾盘候选处理：
    1. build_final_signal
    2. 修复/合并 signal_reasons
    3. 写 final_signal_daily
    4. BUY/STRONG_BUY 生成 paper_trade_record
    5. 非买入但 NEAR_TRIGGER 可生成 OPEN_RECHECK 明日计划
    """
    raw_signal = build_final_signal(candidate, config=strategy_config)
    signal = normalize_signal_reasons(raw_signal)

    enriched = dict(candidate)
    enriched.update(signal)

    if persist:
        upsert_final_signal(db_path=db_path, trade_date=trade_date, candidate=candidate, signal=signal)

    if is_buy_signal(signal):
        record = build_paper_trade_record(
            candidate=enriched,
            signal=signal,
            trade_date=trade_date,
            entry_type="close_tail",
            trading_days=trading_days,
        )
        enriched["paper_trade_record"] = record
        if persist:
            insert_paper_trade_ext(db_path=db_path, record=record)
    else:
        if should_create_open_recheck_plan(candidate, signal):
            plan = build_open_recheck_plan(candidate, signal, plan_date=trade_date)
            enriched["open_recheck_plan"] = plan

    enriched["wecom_message"] = format_final_signal_message(candidate, signal)
    return enriched


def process_open_recheck_v251(
    plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    trade_date: str,
    market_state: str = "normal",
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = DEFAULT_DB_PATH,
    persist: bool = True,
) -> Dict[str, Any]:
    result = open_recheck(plan, quote, market_state=market_state, config=strategy_config)
    if persist:
        insert_open_recheck(db_path=db_path, trade_date=trade_date, plan=plan, quote=quote, result=result)

    out = dict(plan)
    out.update(result)
    out["wecom_message"] = format_open_check_message(plan, result)
    return out


def process_open_confirm_v251(
    plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    trade_date: str,
    trading_days: Optional[List[str]] = None,
    market_state: str = "normal",
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = DEFAULT_DB_PATH,
    persist: bool = True,
) -> Dict[str, Any]:
    result = open_confirm(plan, quote, market_state=market_state, config=strategy_config)
    if persist:
        insert_open_recheck(db_path=db_path, trade_date=trade_date, plan=plan, quote=quote, result=result)

    out = dict(plan)
    out.update(result)

    if result.get("should_write_paper_trade"):
        record = build_paper_trade_record(
            candidate=out,
            signal={**result, "signal_status": result.get("open_status")},
            trade_date=trade_date,
            entry_type="next_open",
            trading_days=trading_days,
        )
        out["paper_trade_record"] = record
        if persist:
            insert_paper_trade_ext(db_path=db_path, record=record)

    out["wecom_message"] = format_open_check_message(plan, result)
    return out
