"""
v2.5.1 SQLite 状态库写入工具。

默认数据库：
./data/trading_state.db

用途：
- final_signal_daily
- open_recheck_daily
- paper_trade_ext
- notification_state 仍由 notify_dedupe.py 管
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import sqlite3
import datetime as dt
import json


DEFAULT_DB_PATH = "data/trading_state.db"


def ensure_state_db(db_path: str = DEFAULT_DB_PATH) -> str:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    sql = """
    CREATE TABLE IF NOT EXISTS final_signal_daily (
        trade_date TEXT,
        symbol TEXT,
        stock_name TEXT,
        signal_status TEXT,
        signal_level TEXT,
        entry_type TEXT,
        should_write_paper_trade INTEGER,
        planned_buy_price REAL,
        stop_loss REAL,
        target_1 REAL,
        target_2 REAL,
        time_stop_days INTEGER,

        daily_2buy_score REAL,
        sector_score REAL,
        leader_score REAL,
        weekly_score REAL,
        yuanjun_score REAL,
        rescue_candle_score REAL,
        risk_pct REAL,

        theme_name TEXT,
        sector_state TEXT,
        leader_type TEXT,
        weekly_state TEXT,
        yuanjun_state TEXT,
        divergence_count INTEGER,

        signal_reasons TEXT,
        risk_flags TEXT,
        blocking_flags TEXT,
        downgrade_flags TEXT,
        upgrade_reasons TEXT,
        raw_json TEXT,
        created_at TEXT,
        PRIMARY KEY (trade_date, symbol)
    );

    CREATE TABLE IF NOT EXISTS open_recheck_daily (
        trade_date TEXT,
        symbol TEXT,
        stock_name TEXT,
        open_status TEXT,
        entry_type TEXT,
        should_write_paper_trade INTEGER,
        open_price REAL,
        current_price REAL,
        planned_buy_price REAL,
        stop_loss REAL,
        reasons TEXT,
        risk_flags TEXT,
        raw_json TEXT,
        created_at TEXT,
        PRIMARY KEY (trade_date, symbol, open_status)
    );

    CREATE TABLE IF NOT EXISTS paper_trade_ext (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        stock_name TEXT,
        signal_status TEXT,
        entry_type TEXT,
        buy_date TEXT,
        sellable_date TEXT,
        buy_price REAL,
        planned_buy_price REAL,
        stop_loss REAL,
        target_1 REAL,
        target_2 REAL,
        time_stop_days INTEGER,
        risk_pct REAL,

        daily_2buy_score REAL,
        sector_score REAL,
        leader_score REAL,
        weekly_score REAL,
        yuanjun_score REAL,
        theme_name TEXT,
        leader_type TEXT,
        yuanjun_state TEXT,

        signal_reasons TEXT,
        risk_flags TEXT,
        raw_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS notification_state (
        trade_date TEXT,
        symbol TEXT,
        channel TEXT,
        last_status TEXT,
        last_key_hash TEXT,
        last_sent_at TEXT,
        PRIMARY KEY (trade_date, symbol, channel)
    );
    """
    with sqlite3.connect(path) as conn:
        conn.executescript(sql)
        conn.commit()
    return str(path)


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False)


def _f(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def upsert_final_signal(
    *,
    db_path: str = DEFAULT_DB_PATH,
    trade_date: str,
    candidate: Dict[str, Any],
    signal: Dict[str, Any],
) -> None:
    ensure_state_db(db_path)
    symbol = candidate.get("symbol") or candidate.get("code")
    stock_name = candidate.get("stock_name") or candidate.get("name")

    raw_json = {
        "candidate": candidate,
        "signal": signal,
    }
    now = dt.datetime.now().isoformat(timespec="seconds")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO final_signal_daily (
                trade_date, symbol, stock_name,
                signal_status, signal_level, entry_type, should_write_paper_trade,
                planned_buy_price, stop_loss, target_1, target_2, time_stop_days,
                daily_2buy_score, sector_score, leader_score, weekly_score, yuanjun_score,
                rescue_candle_score, risk_pct,
                theme_name, sector_state, leader_type, weekly_state, yuanjun_state, divergence_count,
                signal_reasons, risk_flags, blocking_flags, downgrade_flags, upgrade_reasons,
                raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, symbol)
            DO UPDATE SET
                stock_name=excluded.stock_name,
                signal_status=excluded.signal_status,
                signal_level=excluded.signal_level,
                entry_type=excluded.entry_type,
                should_write_paper_trade=excluded.should_write_paper_trade,
                planned_buy_price=excluded.planned_buy_price,
                stop_loss=excluded.stop_loss,
                target_1=excluded.target_1,
                target_2=excluded.target_2,
                time_stop_days=excluded.time_stop_days,
                daily_2buy_score=excluded.daily_2buy_score,
                sector_score=excluded.sector_score,
                leader_score=excluded.leader_score,
                weekly_score=excluded.weekly_score,
                yuanjun_score=excluded.yuanjun_score,
                rescue_candle_score=excluded.rescue_candle_score,
                risk_pct=excluded.risk_pct,
                theme_name=excluded.theme_name,
                sector_state=excluded.sector_state,
                leader_type=excluded.leader_type,
                weekly_state=excluded.weekly_state,
                yuanjun_state=excluded.yuanjun_state,
                divergence_count=excluded.divergence_count,
                signal_reasons=excluded.signal_reasons,
                risk_flags=excluded.risk_flags,
                blocking_flags=excluded.blocking_flags,
                downgrade_flags=excluded.downgrade_flags,
                upgrade_reasons=excluded.upgrade_reasons,
                raw_json=excluded.raw_json,
                created_at=excluded.created_at
            """,
            (
                trade_date, symbol, stock_name,
                signal.get("signal_status"), signal.get("signal_level"), signal.get("entry_type"),
                1 if signal.get("should_write_paper_trade") else 0,
                _f(signal.get("planned_buy_price")), _f(signal.get("stop_loss")),
                _f(signal.get("target_1")), _f(signal.get("target_2")),
                signal.get("time_stop_days"),
                _f(candidate.get("daily_2buy_score", candidate.get("total_score"))),
                _f(candidate.get("sector_score")),
                _f(candidate.get("leader_score")),
                _f(candidate.get("weekly_score")),
                _f(candidate.get("yuanjun_score")),
                _f(candidate.get("rescue_candle_score")),
                _f(candidate.get("risk_pct")),
                candidate.get("theme_name"),
                candidate.get("sector_state"),
                candidate.get("leader_type"),
                candidate.get("weekly_state"),
                candidate.get("yuanjun_state"),
                candidate.get("divergence_count"),
                _json(signal.get("signal_reasons")),
                _json(signal.get("risk_flags")),
                _json(signal.get("blocking_flags")),
                _json(signal.get("downgrade_flags")),
                _json(signal.get("upgrade_reasons")),
                json.dumps(raw_json, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()


def insert_open_recheck(
    *,
    db_path: str = DEFAULT_DB_PATH,
    trade_date: str,
    plan: Dict[str, Any],
    quote: Dict[str, Any],
    result: Dict[str, Any],
) -> None:
    ensure_state_db(db_path)
    symbol = plan.get("symbol") or plan.get("code")
    stock_name = plan.get("stock_name") or plan.get("name")
    now = dt.datetime.now().isoformat(timespec="seconds")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO open_recheck_daily (
                trade_date, symbol, stock_name, open_status, entry_type,
                should_write_paper_trade, open_price, current_price,
                planned_buy_price, stop_loss, reasons, risk_flags, raw_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_date, symbol, stock_name,
                result.get("open_status"),
                result.get("entry_type"),
                1 if result.get("should_write_paper_trade") else 0,
                _f(quote.get("open") or quote.get("open_price")),
                _f(quote.get("price") or quote.get("current_price")),
                _f(result.get("planned_buy_price")),
                _f(result.get("stop_loss")),
                _json(result.get("reasons")),
                _json(result.get("risk_flags")),
                json.dumps({"plan": plan, "quote": quote, "result": result}, ensure_ascii=False),
                now,
            ),
        )
        conn.commit()


def insert_paper_trade_ext(
    *,
    db_path: str = DEFAULT_DB_PATH,
    record: Dict[str, Any],
) -> None:
    ensure_state_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO paper_trade_ext (
                symbol, stock_name, signal_status, entry_type,
                buy_date, sellable_date, buy_price, planned_buy_price,
                stop_loss, target_1, target_2, time_stop_days, risk_pct,
                daily_2buy_score, sector_score, leader_score, weekly_score, yuanjun_score,
                theme_name, leader_type, yuanjun_state,
                signal_reasons, risk_flags, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.get("symbol"), record.get("stock_name"), record.get("signal_status"), record.get("entry_type"),
                record.get("buy_date"), record.get("sellable_date"),
                _f(record.get("buy_price")), _f(record.get("planned_buy_price")),
                _f(record.get("stop_loss")), _f(record.get("target_1")), _f(record.get("target_2")),
                record.get("time_stop_days"), _f(record.get("risk_pct")),
                _f(record.get("daily_2buy_score")), _f(record.get("sector_score")),
                _f(record.get("leader_score")), _f(record.get("weekly_score")), _f(record.get("yuanjun_score")),
                record.get("theme_name"), record.get("leader_type"), record.get("yuanjun_state"),
                record.get("signal_reasons"), record.get("risk_flags"), record.get("raw_json"),
            ),
        )
        conn.commit()


def fetch_today_final_signals(db_path: str = DEFAULT_DB_PATH, trade_date: Optional[str] = None) -> list[dict]:
    ensure_state_db(db_path)
    if trade_date is None:
        trade_date = dt.date.today().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM final_signal_daily WHERE trade_date=? ORDER BY signal_status, symbol",
            (trade_date,),
        ).fetchall()
    return [dict(x) for x in rows]
