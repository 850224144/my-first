"""
v2.5.3 候选评分补齐。

输入 candidate，补：
- weekly_score
- sector_score
- leader_score
- yuanjun_score

如果缺数据，使用中性默认值 + 风险 flags，避免误触发强买。
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List

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


def default_yuanjun_score(reason: str = "缺少援军数据") -> Dict[str, Any]:
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


def enrich_candidate_scores_v253(
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
        if daily_bars:
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

    # 将评分缺失 flag 合并到 risk_flags，便于 final_signal 降级/拒绝说明
    risk_flags = list(out.get("risk_flags") or [])
    for k in ["weekly_flags", "sector_flags", "yuanjun_flags"]:
        v = out.get(k)
        if isinstance(v, list):
            risk_flags.extend(v)
    out["risk_flags"] = list(dict.fromkeys(str(x) for x in risk_flags if str(x)))

    return out
