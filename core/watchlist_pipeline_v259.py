"""
v2.5.9 watchlist + XGB live pools pipeline.

基于 v2.5.8：
- XGB 匹配成功后清理旧缺失标签
- 诊断更准确
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt

from .watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255
from .xgb_live_pools_v258 import load_xgb_live_pools_v258, live_pools_report
from .xgb_pool_enricher_v259 import enrich_candidate_with_xgb_pools_v259, xgb_enrich_report_v259
from .pipeline_v252 import process_tail_candidate_v252


def preview_watchlist_with_xgb_clean_v259(
    *,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    xgb_cache_root: str | Path = "data/xgb",
    trade_date: Optional[str] = None,
    limit: int = 80,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()

    base = preview_final_signals_from_watchlist_v255(
        watchlist_path=watchlist_path,
        duckdb_path=duckdb_path,
        xgb_cache_root=xgb_cache_root,
        trade_date=trade_date,
        limit=limit,
        allow_xgb_fetch=False,
    )

    pools = load_xgb_live_pools_v258(
        trade_date=trade_date,
        cache_root=xgb_cache_root,
        fetch_if_empty=fetch_xgb_if_empty,
    )

    enriched_candidates = []
    for item in base["results"]:
        enriched_candidates.append(enrich_candidate_with_xgb_pools_v259(item, pools))

    rerun = []
    for c in enriched_candidates:
        # 先清理后重跑 final_signal。注意：final_signal 仍会根据 risk/price/hot 做硬拒绝。
        rerun.append(process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            persist=False,
            allow_demo=False,
        ))

    # 再把 XGB 字段补回 rerun 结果，避免 process_tail_candidate_v252 覆盖显示字段
    by_symbol = {c.get("symbol") or c.get("code"): c for c in enriched_candidates}
    final_results = []
    for r in rerun:
        key = r.get("symbol") or r.get("code")
        src = by_symbol.get(key, {})
        merged = dict(r)
        for k in [
            "theme_name", "xgb_pools", "xgb_pool_matched",
            "sector_score", "leader_score", "yuanjun_score",
            "leader_type", "sector_state", "yuanjun_state",
            "sector_reasons", "yuanjun_reasons",
        ]:
            if k in src:
                merged[k] = src[k]
        final_results.append(merged)

    out = dict(base)
    out["xgb_live_pools_report"] = live_pools_report(pools)
    out["xgb_enrich_report"] = xgb_enrich_report_v259(enriched_candidates)
    out["results_before_xgb_clean"] = base["results"]
    out["results"] = final_results
    return out
