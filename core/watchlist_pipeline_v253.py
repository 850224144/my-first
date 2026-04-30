"""
v2.5.3 真实 watchlist pipeline。

用途：
- 读取 data/watchlist.parquet
- 映射 candidate
- 可选补实时 quote
- 可选补日 K / 周线 / 板块 / 援军
- 调用 v2.5.2 pipeline 生成 final_signal 预览或真实写入
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from .project_data_adapter_v252 import read_parquet_records
from .watchlist_candidate_v253 import build_candidate_from_watchlist_row, candidate_ready_report
from .duckdb_daily_loader_v253 import load_daily_bars_from_duckdb
from .score_enricher_v253 import enrich_candidate_scores_v253
from .pipeline_v252 import process_tail_candidate_v252


def load_watchlist_rows(path: str | Path = "data/watchlist.parquet", limit: Optional[int] = None) -> List[Dict[str, Any]]:
    return read_parquet_records(path, limit=limit)


def build_candidates_from_watchlist_v253(
    rows: List[Dict[str, Any]],
    *,
    quote_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    quote_map = quote_map or {}
    for row in rows:
        code = row.get("symbol") or row.get("code")
        quote = quote_map.get(code) or quote_map.get(str(code)) or None
        out.append(build_candidate_from_watchlist_row(row, quote=quote))
    return out


def enrich_candidates_from_project_data_v253(
    candidates: List[Dict[str, Any]],
    *,
    duckdb_path: str | Path = "data/stock_data.duckdb",
    core_pools: Optional[Dict[str, Any]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
    max_daily_load: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, c in enumerate(candidates):
        daily_bars = []
        if max_daily_load is None or i < max_daily_load:
            try:
                daily_bars = load_daily_bars_from_duckdb(c.get("symbol") or c.get("code"), db_path=duckdb_path)
            except Exception:
                daily_bars = []
        enriched = enrich_candidate_scores_v253(
            c,
            daily_bars=daily_bars,
            core_pools=core_pools,
            strategy_config=strategy_config,
        )
        enriched["daily_bars_loaded"] = bool(daily_bars)
        out.append(enriched)
    return out


def preview_final_signals_from_watchlist_v253(
    *,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    trade_date: str,
    limit: int = 20,
    quote_map: Optional[Dict[str, Dict[str, Any]]] = None,
    core_pools: Optional[Dict[str, Any]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows = load_watchlist_rows(watchlist_path, limit=limit)
    candidates = build_candidates_from_watchlist_v253(rows, quote_map=quote_map)
    enriched = enrich_candidates_from_project_data_v253(
        candidates,
        duckdb_path=duckdb_path,
        core_pools=core_pools,
        strategy_config=strategy_config,
        max_daily_load=limit,
    )

    results = []
    for c in enriched:
        # 预览模式 persist=False，不写库
        result = process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            persist=False,
            allow_demo=False,
        )
        results.append(result)

    return {
        "rows": len(rows),
        "candidate_ready_report_before_score": candidate_ready_report(candidates),
        "candidate_ready_report_after_score": candidate_ready_report(enriched),
        "results": results,
    }
