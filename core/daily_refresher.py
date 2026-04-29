# core/daily_refresher.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import time

import polars as pl


def _get_logger():
    try:
        from core.logger import get_logger
        return get_logger()
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("a_stock")


def _get_db_connection():
    from core.data import get_db_connection
    return get_db_connection()


def _read_existing_codes(limit: Optional[int] = None) -> List[str]:
    con = _get_db_connection()
    try:
        rows = con.execute("""
            SELECT code, COUNT(*) AS n, MAX(date) AS last_date
            FROM stock_daily
            GROUP BY code
            HAVING COUNT(*) >= 250
            ORDER BY last_date ASC, code ASC
        """).fetchall()
    finally:
        con.close()

    codes = [str(r[0]).zfill(6) for r in rows]
    if limit:
        codes = codes[:limit]
    return codes


def _save_history_to_stock_daily(code: str, df: pl.DataFrame, source: str):
    if df is None or df.is_empty():
        return

    work = df.clone()
    required = ["date", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in work.columns:
            raise ValueError(f"历史K缺少字段: {col}")

    if "amount" not in work.columns:
        work = work.with_columns((pl.col("close").cast(pl.Float64, strict=False) * pl.col("volume").cast(pl.Float64, strict=False)).alias("amount"))

    if "adj_type" not in work.columns:
        work = work.with_columns(pl.lit("qfq").alias("adj_type"))

    work = work.with_columns([
        pl.lit(str(code).zfill(6)).alias("code"),
        pl.lit(source).alias("source"),
        pl.col("date").cast(pl.Utf8),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Float64, strict=False),
        pl.col("amount").cast(pl.Float64, strict=False),
        pl.col("adj_type").cast(pl.Utf8),
    ]).select([
        "code", "date", "open", "high", "low", "close", "volume", "amount", "adj_type", "source"
    ])

    con = _get_db_connection()
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS stock_daily (
            code VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            adj_type VARCHAR,
            source VARCHAR
        )
        """)
        try:
            con.execute("ALTER TABLE stock_daily ADD COLUMN source VARCHAR")
        except Exception:
            pass
        con.execute("DELETE FROM stock_daily WHERE code = ?", [str(code).zfill(6)])
        con.register("_tmp_stock_daily", work)
        con.execute("""
        INSERT INTO stock_daily (
            code, date, open, high, low, close, volume, amount, adj_type, source
        )
        SELECT code, CAST(date AS DATE), open, high, low, close, volume, amount, adj_type, source
        FROM _tmp_stock_daily
        """)
        con.unregister("_tmp_stock_daily")
    finally:
        con.close()


def refresh_daily_existing(limit: Optional[int] = None, workers: int = 1, sleep_seconds: float = 0.2) -> Dict[str, Any]:
    """
    正式收盘更新：优先刷新已有有效缓存股票到最新日线。
    不用于历史扩容，历史扩容仍由 build-daily-cache 夜间执行。
    """
    logger = _get_logger()
    from core.data import get_sina_history, get_efinance

    codes = _read_existing_codes(limit=limit)
    stats = {
        "processed": 0,
        "success": 0,
        "failed": 0,
        "sina_history": 0,
        "efinance": 0,
        "latest_date": "",
    }

    logger.info(f"开始刷新已有日线缓存：候选={len(codes)} limit={limit}")

    for code in codes:
        stats["processed"] += 1
        df = None
        source = "failed"
        try:
            df = get_sina_history(code, bars=520)
            if df is not None and not df.is_empty() and len(df) >= 250:
                source = "sina_history"
            else:
                df = get_efinance(code, bars=520)
                if df is not None and not df.is_empty() and len(df) >= 250:
                    source = "efinance"

            if df is None or df.is_empty() or len(df) < 250:
                stats["failed"] += 1
                continue

            _save_history_to_stock_daily(code, df, source)
            stats["success"] += 1
            stats[source] = stats.get(source, 0) + 1
            try:
                last_date = str(df.select(pl.col("date").max()).item())[:10]
                if last_date > stats["latest_date"]:
                    stats["latest_date"] = last_date
            except Exception:
                pass

        except Exception as e:
            stats["failed"] += 1
            logger.debug(f"刷新日线失败 code={code} err={e}", exc_info=True)

        if sleep_seconds:
            time.sleep(sleep_seconds)

    logger.info(
        "刷新已有日线缓存完成："
        f"processed={stats['processed']} success={stats['success']} failed={stats['failed']} "
        f"latest_date={stats['latest_date']}"
    )
    return stats
