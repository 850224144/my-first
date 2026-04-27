# core/daily_cache_builder.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
import time

import polars as pl

# ===== logger 兼容处理 =====
try:
    from core.logger import get_logger
except Exception:
    import logging

    logging.basicConfig(level=logging.INFO)

    def get_logger():
        return logging.getLogger("a_stock")


logger = get_logger()

try:
    from core.logger import print_reject_summary
except Exception:
    def print_reject_summary():
        pass

try:
    from core.logger import record_reject
except Exception:
    def record_reject(code, reason, detail=""):
        logger.info(f"[reject] {code} {reason} {detail}")


from core.data import (
    get_db_connection,
    get_sina_history,
    get_efinance,
    get_tencent,
)


@dataclass
class CacheBuildStats:
    processed: int = 0
    success: int = 0
    failed: int = 0
    cache_hit: int = 0
    tencent: int = 0
    sina_history: int = 0
    efinance: int = 0
    mixed: int = 0
    unknown_source: int = 0


def _safe_read_sql(sql: str) -> pl.DataFrame:
    con = get_db_connection()
    try:
        rows = con.execute(sql).fetchall()
        cols = [x[0] for x in con.description]
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        con.close()


def _get_cached_codes() -> set[str]:
    try:
        df = _safe_read_sql("""
                            SELECT code
                            FROM stock_daily
                            GROUP BY code
                            HAVING COUNT(*) >= 250
                            """)
        if df.is_empty():
            return set()
        return set(df["code"].cast(pl.Utf8).to_list())
    except Exception:
        return set()


def _load_daily_candidates(limit: Optional[int] = None) -> pl.DataFrame:
    """
    只选择实时行情有效、价格正常、成交额靠前、尚未缓存日线的股票。
    """

    sql = """
    SELECT
        b.code,
        b.name,
        b.market,
        b.board,
        COALESCE(b.is_st, false) AS is_st,
        q.price,
        q.volume,
        q.amount,
        q.date,
        q.time
    FROM stock_basic b
    INNER JOIN realtime_quote q
        ON b.code = q.code
    """

    df = _safe_read_sql(sql)

    if df.is_empty():
        logger.warning("历史K候选为空：stock_basic 与 realtime_quote 没有可用交集")
        return df

    for col in ["price", "volume", "amount"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    if "name" not in df.columns:
        df = df.with_columns(pl.lit("").alias("name"))

    df = df.filter(
        (pl.col("price").fill_null(0) >= 3)
        & (pl.col("volume").fill_null(0) > 0)
        & (pl.col("amount").fill_null(0) > 0)
        & (~pl.col("name").cast(pl.Utf8).str.contains("ST", literal=False).fill_null(False))
        & (~pl.col("name").cast(pl.Utf8).str.contains("退", literal=False).fill_null(False))
    )

    cached_codes = _get_cached_codes()
    if cached_codes:
        df = df.filter(~pl.col("code").cast(pl.Utf8).is_in(list(cached_codes)))

    df = df.sort("amount", descending=True)

    if limit:
        df = df.head(limit)

    logger.info(f"历史K优化候选池：{len(df)} 只（实时有效 + 按成交额排序 + 未缓存）")
    return df


def _fetch_history_direct(code: str, bars: int = 520):
    """
    直接调用真正历史K函数。
    当前测试结果：
    - get_sina_history 可返回 520 行
    - get_efinance 可返回 520 行
    - get_tencent 当前只返回 1 行，降级为最后兜底
    """
    sources = [
        ("sina_history", get_sina_history),
        ("efinance", get_efinance),
        ("tencent", get_tencent),
    ]

    last_error = None

    for source, fn in sources:
        try:
            df = fn(code, bars=bars)

            if df is not None and not df.is_empty() and len(df) >= 250:
                return df, source

            logger.debug(
                f"历史K源无效 code={code} source={source} rows={0 if df is None else len(df)}"
            )

        except Exception as e:
            last_error = e
            logger.debug(f"历史K源异常 code={code} source={source} | {e}", exc_info=True)

    return None, f"failed: {last_error}" if last_error else "failed"
def _save_history_to_stock_daily(code: str, df: pl.DataFrame, source: str):
    """
    直接写入 stock_daily，绕开 save_to_db 版本差异问题。
    每次按 code 删除旧数据，再插入完整历史K。
    """
    if df is None or df.is_empty():
        return

    work = df.clone()

    required = ["date", "open", "high", "low", "close", "volume"]

    for col in required:
        if col not in work.columns:
            raise ValueError(f"历史K缺少字段: {col}")

    if "amount" not in work.columns:
        work = work.with_columns(
            (pl.col("close").cast(pl.Float64) * pl.col("volume").cast(pl.Float64)).alias("amount")
        )

    if "adj_type" not in work.columns:
        work = work.with_columns(pl.lit("qfq").alias("adj_type"))

    work = work.with_columns([
        pl.lit(code).alias("code"),
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
        "code", "date", "open", "high", "low", "close",
        "volume", "amount", "adj_type", "source"
    ])

    con = get_db_connection()
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

        # 兼容旧表没有 source 字段的情况
        try:
            con.execute("ALTER TABLE stock_daily ADD COLUMN source VARCHAR")
        except Exception:
            pass

        con.execute("DELETE FROM stock_daily WHERE code = ?", [code])

        con.register("_tmp_stock_daily", work)

        con.execute("""
        INSERT INTO stock_daily (
            code, date, open, high, low, close,
            volume, amount, adj_type, source
        )
        SELECT
            code,
            CAST(date AS DATE),
            open,
            high,
            low,
            close,
            volume,
            amount,
            adj_type,
            source
        FROM _tmp_stock_daily
        """)

        con.unregister("_tmp_stock_daily")

    finally:
        con.close()

def build_daily_cache_optimized(
    daily_limit: Optional[int] = None,
    daily_workers: int = 1,
    fail_rate_threshold: float = 0.80,
    max_consecutive_fail: int = 40,
) -> Dict[str, int]:
    """
    历史K缓存优化版：
    1. 只选 realtime_quote 有效股票
    2. 过滤 ST / 退市 / 低价 / 无成交
    3. 按成交额从高到低补缓存
    4. 历史源顺序：sina_history -> efinance -> tencent
    """

    if daily_workers and daily_workers > 1:
        logger.warning("历史K缓存优化版强制单线程，避免接口限流与 DuckDB 冲突")
        daily_workers = 1

    candidates = _load_daily_candidates(limit=daily_limit)

    stats = CacheBuildStats()
    failed_codes: Dict[str, str] = {}

    if candidates.is_empty():
        logger.warning("没有可补充历史K缓存的候选股票")
        return stats.__dict__

    codes = candidates["code"].cast(pl.Utf8).to_list()
    logger.info(f"开始优化历史K缓存：候选={len(codes)} daily_limit={daily_limit}")

    consecutive_fail = 0

    for idx, code in enumerate(codes, start=1):
        stats.processed += 1

        try:
            df, source = _fetch_history_direct(code, bars=520)

            if df is None or df.is_empty() or len(df) < 250:
                stats.failed += 1
                consecutive_fail += 1
                failed_codes[code] = f"no_kline_data source={source}"
                record_reject(code, "no_kline_data", f"历史K为空或不足250根 source={source}")
            else:
                _save_history_to_stock_daily(code, df, source)
                stats.success += 1
                consecutive_fail = 0

                if source == "sina_history":
                    stats.sina_history += 1
                elif source == "efinance":
                    stats.efinance += 1
                elif source == "tencent":
                    stats.tencent += 1
                else:
                    stats.unknown_source += 1

        except Exception as e:
            stats.failed += 1
            consecutive_fail += 1
            failed_codes[code] = f"exception: {e}"
            record_reject(code, "exception", str(e))
            logger.debug(f"历史K缓存异常 code={code} | {e}", exc_info=True)

        if stats.processed % 50 == 0 or idx == len(codes):
            logger.info(
                f"历史K缓存进度：{stats.processed}/{len(codes)} "
                f"成功={stats.success} 失败={stats.failed} "
                f"cache={stats.cache_hit} tencent={stats.tencent} "
                f"sina={stats.sina_history} efinance={stats.efinance}"
            )

        if stats.processed >= 30:
            fail_rate = stats.failed / max(stats.processed, 1)
            if fail_rate > fail_rate_threshold:
                logger.error(
                    f"历史K失败率 {fail_rate:.2f} 超过阈值 {fail_rate_threshold:.2f}，熔断停止。"
                )
                break

        if consecutive_fail >= max_consecutive_fail:
            logger.error(f"历史K连续失败 {consecutive_fail} 次，熔断停止。")
            break

        time.sleep(0.05)

    logger.info(
        "\n历史K构建报告：\n"
        f"  总处理：{stats.processed}\n"
        f"  成功：{stats.success}\n"
        f"  失败：{stats.failed}\n"
        f"  缓存命中：{stats.cache_hit}\n"
        f"  腾讯成功：{stats.tencent}\n"
        f"  新浪历史成功：{stats.sina_history}\n"
        f"  efinance成功：{stats.efinance}\n"
        f"  混合/合并：{stats.mixed}\n"
        f"  未知来源：{stats.unknown_source}"
    )

    if failed_codes:
        logger.warning("失败样本前20：")
        for c, r in list(failed_codes.items())[:20]:
            logger.warning(f"{c} -> {r}")

    print_reject_summary()

    return stats.__dict__