"""
v2.5.4 候选评分补齐。

相比 v2.5.3 的变化：
- 没有 core_pools / 主线数据时，不再计算出 no_sector_follow
- 数据缺失只做降级提醒，不直接等同“无板块效应”
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .weekly import score_weekly_trend
from .sector import score_sector_for_stock
from .yuanjun import score_yuanjun


def default_weekly_score(reason: str = "缺少日K，周线未知") -> Dict[str, Any]:
    return {
        "weekly_score": 50.0,
        "weekly_state": "weekly_unknown",
        "weekly_flags": [f"周线数据缺失(weekly_data_missing):{reason}"],
        "weekly_reasons": [],
    }


def default_sector_score(reason: str = "缺少板块数据") -> Dict[str, Any]:
    return {
        "sector_score": 50.0,
        "leader_score": 50.0,
        "sector_state": "sector_unknown",
        "leader_type": "unknown",
        "theme_name": None,
        "sector_flags": [f"板块数据缺失(sector_data_missing):{reason}"],
        "sector_reasons": [],
    }


def default_yuanjun_score(reason: str = "缺少主线/板块跟风数据") -> Dict[str, Any]:
    return {
        "yuanjun_score": 50.0,
        "yuanjun_state": "YJ_UNKNOWN",
        "yuanjun_flags": [f"援军数据缺失(yuanjun_data_missing):{reason}"],
        "yuanjun_reasons": [],
        "divergence_score": 50.0,
        "divergence_count": 0,
        "rescue_candle_score": 50.0,
        "yj_candle_low": None,
        "yj_candle_mid": None,
        "yj_candle_high": None,
        "yj_stop_loss": None,
    }


def has_yuanjun_context(candidate: Dict[str, Any], core_pools: Optional[Dict[str, Any]]) -> bool:
    """
    判断是否有足够的援军上下文。
    如果没有，不能把 sector_follow_limit_up_count=0 当作“无板块效应”。
    """
    if core_pools:
        return True

    keys = [
        "mainline_days",
        "theme_broken_count",
        "theme_limit_down_count",
        "sector_follow_limit_up_count",
        "previous_divergence_count",
        "leader_resilient",
    ]
    return any(candidate.get(k) is not None for k in keys)


def enrich_candidate_scores_v254(
    candidate: Dict[str, Any],
    *,
    daily_bars: Optional[Any] = None,
    core_pools: Optional[Dict[str, Any]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(candidate)
    cfg = strategy_config or {}

    # weekly
    if "weekly_score" not in out or out.get("weekly_score") is None:
        if daily_bars:
            try:
                out.update(score_weekly_trend(daily_bars, config=cfg.get("weekly", {})))
            except Exception as exc:
                out.update(default_weekly_score(str(exc)))
        else:
            out.update(default_weekly_score())

    # sector / leader
    if "sector_score" not in out or out.get("sector_score") is None or "leader_score" not in out or out.get("leader_score") is None:
        if core_pools:
            try:
                out.update(score_sector_for_stock(out.get("symbol") or out.get("code"), core_pools=core_pools, config=cfg.get("sector", {})))
            except Exception as exc:
                out.update(default_sector_score(str(exc)))
        else:
            out.update(default_sector_score())

    # yuanjun
    if "yuanjun_score" not in out or out.get("yuanjun_score") is None:
        if daily_bars and has_yuanjun_context(out, core_pools):
            try:
                out.update(score_yuanjun(
                    daily_bars=daily_bars,
                    theme_heat_score=float(out.get("sector_score") or 50),
                    leader_score=float(out.get("leader_score") or 50),
                    leader_type=str(out.get("leader_type") or "unknown"),
                    mainline_days=int(out.get("mainline_days") or 0),
                    broken_count=int(out.get("theme_broken_count") or 0),
                    limit_down_count=int(out.get("theme_limit_down_count") or 0),
                    sector_follow_limit_up_count=int(out.get("sector_follow_limit_up_count") or 0),
                    previous_divergence_count=int(out.get("previous_divergence_count") or 0),
                    leader_resilient=bool(out.get("leader_resilient") or False),
                    stage_gain_pct=out.get("stage_gain_pct"),
                    pullback_new_low=bool(out.get("pullback_new_low") or False),
                    config=cfg.get("yuanjun", {}),
                ))
            except Exception as exc:
                out.update(default_yuanjun_score(str(exc)))
        else:
            out.update(default_yuanjun_score())

    risk_flags = list(out.get("risk_flags") or [])
    for k in ["weekly_flags", "sector_flags", "yuanjun_flags"]:
        v = out.get(k)
        if isinstance(v, list):
            risk_flags.extend(v)
    out["risk_flags"] = list(dict.fromkeys(str(x) for x in risk_flags if str(x)))

    return out
