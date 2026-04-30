"""
v2.5.2 生产保护。

目标：
- 明确 demo 数据与真实数据边界
- 默认禁止 is_demo=True 的候选写入状态库 / paper_trader
- 防止 demo 脚本里的 603019.SH 中科曙光被误认为真实选股结果

注意：
如果真实行情系统真的选出了中科曙光，不会因为 symbol=603019.SH 被拦截；
只拦截 is_demo=True 或 data_mode=demo 的数据。
"""

from __future__ import annotations

from typing import Any, Dict, List


class ProductionGuardError(RuntimeError):
    pass


def is_demo_payload(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False

    if payload.get("is_demo") is True:
        return True

    data_mode = str(payload.get("data_mode") or payload.get("mode") or "").lower()
    if data_mode in {"demo", "test_demo", "sample"}:
        return True

    source = str(payload.get("source") or payload.get("data_source") or "").lower()
    if source in {"demo", "sample", "mock"}:
        return True

    # v2.5.0/v2.5.1 demo 样例可能没显式 is_demo；
    # 这里不按 symbol 拦截，只按明确标记拦截，避免误杀真实 603019.SH。
    return False


def assert_not_demo_payload(payload: Dict[str, Any], *, allow_demo: bool = False, context: str = "") -> None:
    if allow_demo:
        return

    if is_demo_payload(payload):
        prefix = f"{context}: " if context else ""
        raise ProductionGuardError(
            prefix + "检测到 demo/sample 数据，生产流程默认禁止写入。"
        )


def validate_candidate_for_production(candidate: Dict[str, Any], *, allow_demo: bool = False) -> List[str]:
    """
    返回 warnings。
    严重问题抛异常。
    """
    assert_not_demo_payload(candidate, allow_demo=allow_demo, context="candidate")

    warnings: List[str] = []
    symbol = candidate.get("symbol") or candidate.get("code")
    if not symbol:
        warnings.append("缺少 symbol/code")

    required_soft = [
        "daily_2buy_score",
        "risk_pct",
        "current_price",
        "trigger_price",
        "fresh_quote",
    ]
    for k in required_soft:
        if k not in candidate:
            warnings.append(f"缺少候选字段 {k}")

    score_fields = ["weekly_score", "sector_score", "leader_score", "yuanjun_score"]
    missing_scores = [k for k in score_fields if k not in candidate]
    if missing_scores:
        warnings.append("缺少评分字段：" + ",".join(missing_scores))

    return warnings


def validate_plan_for_production(plan: Dict[str, Any], *, allow_demo: bool = False) -> List[str]:
    assert_not_demo_payload(plan, allow_demo=allow_demo, context="trade_plan")

    warnings: List[str] = []
    for k in ["symbol", "plan_type", "planned_buy_price"]:
        if not plan.get(k):
            warnings.append(f"缺少计划字段 {k}")

    if str(plan.get("plan_type") or "").upper() == "OPEN_RECHECK":
        for k in ["yj_candle_mid", "yj_stop_loss"]:
            if plan.get(k) is None:
                warnings.append(f"OPEN_RECHECK 缺少 {k}")

    return warnings


def attach_real_mode(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    out.setdefault("is_demo", False)
    out.setdefault("data_mode", "real")
    return out


def attach_demo_mode(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    out["is_demo"] = True
    out["data_mode"] = "demo"
    out.setdefault("source", "demo")
    return out
