"""
v2.7.0 调度接入：14:40 observe gate + 14:50 tail confirm。

特点：
- 不覆盖原 scheduler
- 提供可被 APScheduler 调用的 job 函数
- 防重复执行
- 记录 job run state 到 SQLite
- 记录日志到 logs/scheduler_tail_v270.log
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import datetime as dt
import json
import sqlite3
import traceback

from .observe_gate_store_v261 import build_observe_gate_outputs_v261
from .tail_confirm_runner_v265 import run_tail_confirm_from_tail_focus_v265
from .intraday_pipeline_v265 import run_intraday_tail_pipeline_v265


def _today() -> str:
    return dt.date.today().isoformat()


def is_weekend_v270(trade_date: str) -> bool:
    d = dt.date.fromisoformat(trade_date)
    return d.weekday() >= 5


def append_log_v270(root: str | Path, msg: str) -> None:
    root = Path(root)
    p = root / "logs" / "scheduler_tail_v270.log"
    p.parent.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().isoformat(timespec="seconds")
    p.write_text((p.read_text(encoding="utf-8") if p.exists() else "") + f"{ts} | {msg}\n", encoding="utf-8")


def _db_path(root: str | Path) -> Path:
    return Path(root) / "data" / "scheduler_state_v270.db"


def ensure_job_table_v270(root: str | Path) -> None:
    p = _db_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(p) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduler_job_runs_v270 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            job_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            result_json TEXT,
            error TEXT,
            UNIQUE(trade_date, job_name)
        )
        """)
        conn.commit()


def already_ran_v270(root: str | Path, trade_date: str, job_name: str) -> bool:
    ensure_job_table_v270(root)
    with sqlite3.connect(_db_path(root)) as conn:
        row = conn.execute(
            "SELECT status FROM scheduler_job_runs_v270 WHERE trade_date=? AND job_name=?",
            (trade_date, job_name),
        ).fetchone()
    return bool(row and row[0] == "success")


def record_job_v270(
    root: str | Path,
    *,
    trade_date: str,
    job_name: str,
    status: str,
    started_at: str,
    finished_at: str,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    ensure_job_table_v270(root)
    with sqlite3.connect(_db_path(root)) as conn:
        conn.execute("""
        INSERT OR REPLACE INTO scheduler_job_runs_v270
        (trade_date, job_name, status, started_at, finished_at, result_json, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_date,
            job_name,
            status,
            started_at,
            finished_at,
            json.dumps(result or {}, ensure_ascii=False, default=str),
            error,
        ))
        conn.commit()


def run_job_guarded_v270(
    *,
    root: str | Path = ".",
    trade_date: Optional[str] = None,
    job_name: str,
    force: bool = False,
    skip_weekend: bool = True,
    func,
) -> Dict[str, Any]:
    root = Path(root)
    trade_date = trade_date or _today()

    if skip_weekend and is_weekend_v270(trade_date):
        result = {"skipped": True, "reason": "weekend", "trade_date": trade_date, "job_name": job_name}
        append_log_v270(root, f"SKIP {job_name} {trade_date} weekend")
        return result

    if not force and already_ran_v270(root, trade_date, job_name):
        result = {"skipped": True, "reason": "already_ran", "trade_date": trade_date, "job_name": job_name}
        append_log_v270(root, f"SKIP {job_name} {trade_date} already_ran")
        return result

    started = dt.datetime.now().isoformat(timespec="seconds")
    append_log_v270(root, f"START {job_name} {trade_date}")

    try:
        result = func()
        finished = dt.datetime.now().isoformat(timespec="seconds")
        record_job_v270(
            root,
            trade_date=trade_date,
            job_name=job_name,
            status="success",
            started_at=started,
            finished_at=finished,
            result=result,
        )
        append_log_v270(root, f"SUCCESS {job_name} {trade_date}")
        return result
    except Exception as exc:
        finished = dt.datetime.now().isoformat(timespec="seconds")
        err = traceback.format_exc()
        record_job_v270(
            root,
            trade_date=trade_date,
            job_name=job_name,
            status="failed",
            started_at=started,
            finished_at=finished,
            result={},
            error=err,
        )
        append_log_v270(root, f"FAILED {job_name} {trade_date} {exc}")
        raise


def build_observe_gate_job_v270(
    *,
    root: str | Path = ".",
    trade_date: Optional[str] = None,
    force: bool = False,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    root = Path(root)
    trade_date = trade_date or _today()

    def _run():
        return build_observe_gate_outputs_v261(
            trade_date=trade_date,
            watchlist_path=root / "data" / "watchlist.parquet",
            duckdb_path=root / "data" / "stock_data.duckdb",
            xgb_cache_root=root / "data" / "xgb",
            output_quality_path=root / "data" / "watchlist_quality.parquet",
            output_tail_focus_path=root / "data" / "watchlist_tail_focus.parquet",
            output_low_priority_path=root / "data" / "watchlist_low_priority.parquet",
            report_dir=root / "data" / "reports",
            limit=500,
            fetch_xgb_if_empty=fetch_xgb_if_empty,
        )

    return run_job_guarded_v270(
        root=root,
        trade_date=trade_date,
        job_name="build_observe_gate_1440",
        force=force,
        func=_run,
    )


def tail_confirm_job_v270(
    *,
    root: str | Path = ".",
    trade_date: Optional[str] = None,
    force: bool = False,
    persist_tail: bool = False,
) -> Dict[str, Any]:
    root = Path(root)
    trade_date = trade_date or _today()

    def _run():
        return run_tail_confirm_from_tail_focus_v265(
            trade_date=trade_date,
            tail_focus_path=root / "data" / "watchlist_tail_focus.parquet",
            output_results_path=root / "data" / "tail_confirm_results_v265.parquet",
            report_dir=root / "data" / "reports",
            db_path=str(root / "data" / "trading_state.db"),
            persist=persist_tail,
        )

    return run_job_guarded_v270(
        root=root,
        trade_date=trade_date,
        job_name="tail_confirm_1450",
        force=force,
        func=_run,
    )


def intraday_tail_pipeline_job_v270(
    *,
    root: str | Path = ".",
    trade_date: Optional[str] = None,
    force: bool = False,
    persist_tail: bool = False,
    fetch_xgb_if_empty: bool = True,
) -> Dict[str, Any]:
    root = Path(root)
    trade_date = trade_date or _today()

    def _run():
        return run_intraday_tail_pipeline_v265(
            trade_date=trade_date,
            root=root,
            persist_tail=persist_tail,
            fetch_xgb_if_empty=fetch_xgb_if_empty,
        )

    return run_job_guarded_v270(
        root=root,
        trade_date=trade_date,
        job_name="intraday_tail_pipeline",
        force=force,
        func=_run,
    )
