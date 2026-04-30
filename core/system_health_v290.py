"""
v2.9.0 系统健康检查。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import json
import sqlite3


REQUIRED_FILES = [
    "data/watchlist.parquet",
    "data/watchlist_tail_focus.parquet",
    "data/tail_confirm_results_v265.parquet",
    "data/paper_trade_candidates.parquet",
    "data/trade_plan_open_recheck.parquet",
    "data/stock_data.duckdb",
]


def file_info(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else 0,
    }


def check_sqlite_tables(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "tables": []}
    try:
        with sqlite3.connect(path) as conn:
            tables = [x[0] for x in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
        return {"exists": True, "tables": tables}
    except Exception as exc:
        return {"exists": True, "error": str(exc), "tables": []}


def build_system_health_v290(root: str | Path = ".") -> Dict[str, Any]:
    root = Path(root)
    files = {rel: file_info(root / rel) for rel in REQUIRED_FILES}
    state_db = check_sqlite_tables(root / "data" / "trading_state.db")
    scheduler_db = check_sqlite_tables(root / "data" / "scheduler_state_v270.db")

    missing_required = [rel for rel, info in files.items() if not info["exists"]]
    status = "ok" if not missing_required else "warning"

    return {
        "status": status,
        "missing_required": missing_required,
        "files": files,
        "trading_state_db": state_db,
        "scheduler_state_db": scheduler_db,
    }
