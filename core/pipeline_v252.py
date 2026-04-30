"""
v2.5.2 pipeline 包装器。

在 v2.5.1 基础上增加生产保护：
- 默认禁止 demo/sample 数据进入真实写入流程
- 自动补 data_mode=real
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List

from .production_guard_v252 import (
    validate_candidate_for_production,
    validate_plan_for_production,
    attach_real_mode,
)
from .pipeline_v251 import (
    process_tail_candidate_v251,
    process_open_recheck_v251,
    process_open_confirm_v251,
)


def process_tail_candidate_v252(
    candidate: Dict[str, Any],
    *,
    trade_date: str,
    trading_days: Optional[List[str]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = "data/trading_state.db",
    persist: bool = True,
    allow_demo: bool = False,
) -> Dict[str, Any]:
    candidate = attach_real_mode(candidate)
    warnings = validate_candidate_for_production(candidate, allow_demo=allow_demo)
    out = process_tail_candidate_v251(
        candidate,
        trade_date=trade_date,
        trading_days=trading_days,
        strategy_config=strategy_config,
        db_path=db_path,
        persist=persist,
    )
    out["production_warnings"] = warnings
    return out


def process_open_recheck_v252(
    plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    trade_date: str,
    market_state: str = "normal",
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = "data/trading_state.db",
    persist: bool = True,
    allow_demo: bool = False,
) -> Dict[str, Any]:
    plan = attach_real_mode(plan)
    warnings = validate_plan_for_production(plan, allow_demo=allow_demo)
    out = process_open_recheck_v251(
        plan,
        quote,
        trade_date=trade_date,
        market_state=market_state,
        strategy_config=strategy_config,
        db_path=db_path,
        persist=persist,
    )
    out["production_warnings"] = warnings
    return out


def process_open_confirm_v252(
    plan: Dict[str, Any],
    quote: Dict[str, Any],
    *,
    trade_date: str,
    trading_days: Optional[List[str]] = None,
    market_state: str = "normal",
    strategy_config: Optional[Dict[str, Any]] = None,
    db_path: str = "data/trading_state.db",
    persist: bool = True,
    allow_demo: bool = False,
) -> Dict[str, Any]:
    plan = attach_real_mode(plan)
    warnings = validate_plan_for_production(plan, allow_demo=allow_demo)
    out = process_open_confirm_v251(
        plan,
        quote,
        trade_date=trade_date,
        trading_days=trading_days,
        market_state=market_state,
        strategy_config=strategy_config,
        db_path=db_path,
        persist=persist,
    )
    out["production_warnings"] = warnings
    return out
