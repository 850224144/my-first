"""
v2.5.5 watchlist 全链路预览 pipeline。

数据流：
watchlist.parquet
 -> realtime_quote
 -> stock_daily
 -> xgb core_pools cache
 -> final_signal preview
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt

from .project_data_adapter_v252 import read_parquet_records
from .watchlist_candidate_v253 import build_candidate_from_watchlist_row, candidate_ready_report
from .duckdb_daily_loader_v253 import load_daily_bars_from_duckdb
from .realtime_quote_loader_v255 import load_realtime_quote_map_from_duckdb, quote_map_report
from .xgb_core_pools_v255 import load_xgb_core_pools_v255, core_pools_report
from .score_enricher_v255 import enrich_candidate_scores_v255
from .pipeline_v252 import process_tail_candidate_v252


def load_watchlist_rows(path: str | Path = "data/watchlist.parquet", limit: Optional[int] = None) -> List[Dict[str, Any]]:
    return read_parquet_records(path, limit=limit)


def build_realtime_quote_map_for_watchlist_v255(
    rows: List[Dict[str, Any]],
    *,
    duckdb_path: str | Path = "data/stock_data.duckdb",
    trade_date: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    symbols = [str(r.get("symbol") or r.get("code")) for r in rows if r.get("symbol") or r.get("code")]
    return load_realtime_quote_map_from_duckdb(
        db_path=duckdb_path,
        trade_date=trade_date,
        only_symbols=symbols,
        allow_latest_if_date_missing=True,
    )


def build_candidates_from_watchlist_v255(
    rows: List[Dict[str, Any]],
    *,
    quote_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    out = []
    quote_map = quote_map or {}
    for row in rows:
        code = row.get("symbol") or row.get("code")
        quote = None
        if code is not None:
            quote = quote_map.get(str(code))
            if quote is None:
                try:
                    from .data_normalizer import normalize_symbol
                    std = normalize_symbol(code)
                    quote = quote_map.get(std) or quote_map.get(std.split(".")[0])
                except Exception:
                    pass
        out.append(build_candidate_from_watchlist_row(row, quote=quote))
    return out


def enrich_candidates_from_project_data_v255(
    candidates: List[Dict[str, Any]],
    *,
    duckdb_path: str | Path = "data/stock_data.duckdb",
    core_pools: Optional[Dict[str, Any]] = None,
    strategy_config: Optional[Dict[str, Any]] = None,
    max_daily_load: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    out = []
    for i, c in enumerate(candidates):
        daily_bars = []
        if max_daily_load is None or i < max_daily_load:
            try:
                daily_bars = load_daily_bars_from_duckdb(c.get("symbol") or c.get("code"), db_path=duckdb_path)
            except Exception:
                daily_bars = []

        enriched = enrich_candidate_scores_v255(
            c,
            daily_bars=daily_bars,
            core_pools=core_pools,
            strategy_config=strategy_config,
        )
        enriched["daily_bars_loaded"] = bool(daily_bars)
        out.append(enriched)
    return out


def preview_final_signals_from_watchlist_v255(
    *,
    watchlist_path: str | Path = "data/watchlist.parquet",
    duckdb_path: str | Path = "data/stock_data.duckdb",
    xgb_cache_root: str | Path = "data/xgb",
    trade_date: Optional[str] = None,
    limit: int = 50,
    quote_map: Optional[Dict[str, Dict[str, Any]]] = None,
    core_pools: Optional[Dict[str, Any]] = None,
    allow_xgb_fetch: bool = False,
    strategy_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    rows = load_watchlist_rows(watchlist_path, limit=limit)
    symbols = [str(r.get("symbol") or r.get("code")) for r in rows if r.get("symbol") or r.get("code")]

    if quote_map is None:
        quote_map = build_realtime_quote_map_for_watchlist_v255(
            rows,
            duckdb_path=duckdb_path,
            trade_date=trade_date,
        )

    if core_pools is None:
        core_pools = load_xgb_core_pools_v255(
            trade_date=trade_date,
            cache_root=xgb_cache_root,
            allow_fetch=allow_xgb_fetch,
        )

    candidates = build_candidates_from_watchlist_v255(rows, quote_map=quote_map)
    enriched = enrich_candidates_from_project_data_v255(
        candidates,
        duckdb_path=duckdb_path,
        core_pools=core_pools,
        strategy_config=strategy_config,
        max_daily_load=limit,
    )

    results = []
    for c in enriched:
        results.append(process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            persist=False,
            allow_demo=False,
        ))

    return {
        "trade_date": trade_date,
        "rows": len(rows),
        "quote_report": quote_map_report(quote_map, symbols),
        "core_pools_report": core_pools_report(core_pools),
        "candidate_ready_report_before_score": candidate_ready_report(candidates),
        "candidate_ready_report_after_score": candidate_ready_report(enriched),
        "results": results,
    }
