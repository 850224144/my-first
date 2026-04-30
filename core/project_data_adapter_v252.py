"""
v2.5.2 项目真实数据适配器。

你的项目当前主要数据文件：
- data/watchlist.parquet
- data/trade_plan.parquet
- data/positions.parquet
- data/trading_state.db
- data/stock_data.duckdb

本模块只做只读检查和轻量字段映射，不修改数据。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import sqlite3


PROJECT_DATA_FILES = {
    "watchlist": "data/watchlist.parquet",
    "trade_plan": "data/trade_plan.parquet",
    "positions": "data/positions.parquet",
    "trading_state": "data/trading_state.db",
    "stock_duckdb": "data/stock_data.duckdb",
}


def _import_pandas():
    try:
        import pandas as pd
        return pd
    except Exception as exc:
        raise RuntimeError("需要 pandas/pyarrow 才能读取 parquet。请确认环境已安装。") from exc


def project_path(root: str | Path, rel: str) -> Path:
    return Path(root).resolve() / rel


def file_status(root: str | Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for name, rel in PROJECT_DATA_FILES.items():
        p = project_path(root, rel)
        out[name] = {
            "path": str(p),
            "exists": p.exists(),
            "size": p.stat().st_size if p.exists() else 0,
        }
    return out


def read_parquet_records(path: str | Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    pd = _import_pandas()
    df = pd.read_parquet(p)
    if limit:
        df = df.head(limit)
    # NaN -> None
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def inspect_parquet(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"exists": False, "path": str(p)}
    pd = _import_pandas()
    df = pd.read_parquet(p)
    return {
        "exists": True,
        "path": str(p),
        "rows": int(len(df)),
        "columns": list(df.columns),
    }


def inspect_state_db(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"exists": False, "path": str(p)}

    with sqlite3.connect(p) as conn:
        tables = [x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
        counts = {}
        demo_hits = {}
        for t in tables:
            try:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except Exception:
                counts[t] = None

            # 尝试检查 raw_json 中是否有 demo 标记
            try:
                cols = [x[1] for x in conn.execute(f"PRAGMA table_info({t})").fetchall()]
                if "raw_json" in cols:
                    demo_hits[t] = conn.execute(
                        f"SELECT COUNT(*) FROM {t} WHERE raw_json LIKE '%\"is_demo\": true%' OR raw_json LIKE '%\"data_mode\": \"demo\"%'"
                    ).fetchone()[0]
            except Exception:
                pass

    return {
        "exists": True,
        "path": str(p),
        "tables": tables,
        "counts": counts,
        "demo_hits": demo_hits,
    }


def normalize_watchlist_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    尝试把当前 watchlist 行映射成 v2.5.1 pipeline 可用字段。
    不猜测太多，缺失字段留空，让检查报告提示。
    """
    out = dict(row)
    if "symbol" not in out:
        for k in ["code", "ts_code", "stock_code"]:
            if row.get(k):
                out["symbol"] = row.get(k)
                break
    if "stock_name" not in out:
        for k in ["name", "stock_name", "股票名称"]:
            if row.get(k):
                out["stock_name"] = row.get(k)
                break

    # 兼容 total_score
    if "daily_2buy_score" not in out and "total_score" in out:
        out["daily_2buy_score"] = out.get("total_score")

    if "current_price" not in out:
        for k in ["price", "last_price", "close"]:
            if row.get(k) is not None:
                out["current_price"] = row.get(k)
                break

    out.setdefault("is_demo", False)
    out.setdefault("data_mode", "real")
    return out


def normalize_trade_plan_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    if "symbol" not in out:
        for k in ["code", "ts_code", "stock_code"]:
            if row.get(k):
                out["symbol"] = row.get(k)
                break
    if "stock_name" not in out:
        for k in ["name", "stock_name", "股票名称"]:
            if row.get(k):
                out["stock_name"] = row.get(k)
                break
    out.setdefault("is_demo", False)
    out.setdefault("data_mode", "real")
    return out


def required_candidate_fields_report(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    required = [
        "symbol",
        "daily_2buy_score",
        "risk_pct",
        "current_price",
        "trigger_price",
        "fresh_quote",
        "weekly_score",
        "sector_score",
        "leader_score",
        "yuanjun_score",
    ]
    if not records:
        return {"rows": 0, "missing_fields": required, "field_coverage": {}}

    coverage = {}
    for f in required:
        count = sum(1 for r in records if r.get(f) is not None)
        coverage[f] = {
            "non_null": count,
            "ratio": round(count / len(records), 4),
        }
    missing = [f for f, v in coverage.items() if v["non_null"] == 0]
    return {
        "rows": len(records),
        "missing_fields": missing,
        "field_coverage": coverage,
    }


def inspect_project_real_data(root: str | Path = ".") -> Dict[str, Any]:
    root = Path(root).resolve()
    status = file_status(root)

    watchlist_info = inspect_parquet(project_path(root, PROJECT_DATA_FILES["watchlist"]))
    trade_plan_info = inspect_parquet(project_path(root, PROJECT_DATA_FILES["trade_plan"]))
    positions_info = inspect_parquet(project_path(root, PROJECT_DATA_FILES["positions"]))
    db_info = inspect_state_db(project_path(root, PROJECT_DATA_FILES["trading_state"]))

    watchlist_records = []
    if watchlist_info.get("exists"):
        raw = read_parquet_records(watchlist_info["path"], limit=200)
        watchlist_records = [normalize_watchlist_row(x) for x in raw]

    trade_plan_records = []
    if trade_plan_info.get("exists"):
        raw = read_parquet_records(trade_plan_info["path"], limit=200)
        trade_plan_records = [normalize_trade_plan_row(x) for x in raw]

    return {
        "root": str(root),
        "file_status": status,
        "watchlist": watchlist_info,
        "trade_plan": trade_plan_info,
        "positions": positions_info,
        "trading_state": db_info,
        "watchlist_candidate_field_report": required_candidate_fields_report(watchlist_records),
        "trade_plan_rows_sampled": len(trade_plan_records),
    }
