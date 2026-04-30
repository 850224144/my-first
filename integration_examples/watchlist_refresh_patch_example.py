"""
watchlist_refresh 接入示例。

不要直接当完整文件覆盖。
把核心逻辑合并到你现有 watchlist_refresh 里。

目标：
- 刷新观察池时附加 sector/weekly/yuanjun 字段
- 不改变 paper_trader 买入逻辑
"""

from core.sector import score_sector_for_stock
from core.weekly import score_weekly_trend
from core.yuanjun import score_yuanjun
from core.signal_engine import build_observation_signal


def enrich_watchlist_candidate(candidate, *, core_pools=None, strategy_config=None):
    """
    candidate 建议包含：
    - symbol/code
    - daily_bars
    - daily_2buy_score or total_score
    - risk_pct
    - current_price/price
    - trigger_price
    """
    symbol = candidate.get("symbol") or candidate.get("code")
    bars = candidate.get("daily_bars") or candidate.get("bars") or []

    sector = score_sector_for_stock(symbol, core_pools=core_pools or {})
    weekly = score_weekly_trend(bars, config=(strategy_config or {}).get("weekly", {}))

    yuanjun = score_yuanjun(
        daily_bars=bars,
        theme_heat_score=sector.get("sector_score", 50),
        leader_score=sector.get("leader_score", 50),
        leader_type=sector.get("leader_type", "normal_stock"),
        mainline_days=candidate.get("mainline_days", 0),
        broken_count=candidate.get("theme_broken_count", 0),
        limit_down_count=candidate.get("theme_limit_down_count", 0),
        sector_follow_limit_up_count=candidate.get("sector_follow_limit_up_count", 0),
        previous_divergence_count=candidate.get("previous_divergence_count", 0),
        leader_resilient=candidate.get("leader_resilient", False),
        stage_gain_pct=candidate.get("stage_gain_pct"),
        pullback_new_low=candidate.get("pullback_new_low", False),
        config=(strategy_config or {}).get("yuanjun", {}),
    )

    enriched = dict(candidate)
    enriched.update(sector)
    enriched.update(weekly)
    enriched.update(yuanjun)
    enriched.update(build_observation_signal(enriched, config={
        "risk_pct_max": 8,
        "daily_2buy_buy_score": 80,
        "weekly_hard_reject_score": 45,
        "sector_hard_reject_score": 45,
        "leader_reject_score": 50,
        "yuanjun_reject_score": 50,
    }))
    return enriched
