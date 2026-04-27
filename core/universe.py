# -*- coding: utf-8 -*-
"""
core/universe.py

方案 D 股票池：
- 股票基础表来自 AkShare，并缓存 stock_basic；默认 7 天刷新一次。
- 严格代码段：SH 600/601/603/605/688，SZ 000/001/002/003/300/301，BJ 920。
- Sina 实时批量校验只由 validate-basic / prepare-data 阶段执行，正常构建股票池直接使用 stock_basic。
- 禁止默认静态代码段兜底；静态号段退出主流程。
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List

import polars as pl

from core.data import (
    build_stock_basic,
    build_daily_cache,
    load_stock_basic,
    load_from_db,
    normalize_code,
    classify_market,
    valid_a_share_code,
)
from core.feature import compute_features
from core.logger import get_logger, log_reject, log_exception, summarize_rejects

UNIVERSE_CACHE = "data/universe.parquet"
MIN_PRICE = 3.0
MIN_LISTING_DAYS = 250
MIN_AMOUNT20 = 80_000_000.0


def _load_universe_cache() -> pl.DataFrame:
    if not os.path.exists(UNIVERSE_CACHE):
        return pl.DataFrame()
    try:
        cache = pl.read_parquet(UNIVERSE_CACHE)
        if cache is not None and not cache.is_empty():
            return cache
    except Exception as e:
        log_exception("读取股票池缓存失败", e)
    return pl.DataFrame()


def _history_filter_one(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    code = normalize_code(row.get("code", ""))
    name = row.get("name", "") or code
    if not valid_a_share_code(code):
        log_reject(code, "universe", "invalid_prefix", "不属于确认后的 A 股代码段", name=name)
        return None
    try:
        # 方案F：股票池构建只读本地 stock_daily 缓存，不再远程拉历史K。
        df = load_from_db(code, adj_type="qfq")
        if df is None or len(df) == 0:
            log_reject(code, "universe", "no_daily_cache", "无本地日线缓存；请先运行 build-daily-cache", name=name)
            return None
        if len(df) < MIN_LISTING_DAYS:
            log_reject(code, "universe", "listing_days_not_enough", f"bars={len(df)}", name=name)
            return None
        last = df.tail(1)
        last_close = float(last.select("close").item())
        if last_close < MIN_PRICE:
            log_reject(code, "universe", "low_price", f"last_close={last_close}", name=name)
            return None
        amount20 = df.tail(20).select(pl.col("amount").mean()).item() if "amount" in df.columns else None
        if amount20 is None or float(amount20) < MIN_AMOUNT20:
            log_reject(code, "universe", "low_amount20", f"amount20={amount20}", name=name)
            return None
        return {
            "code": code,
            "name": name,
            "market": row.get("market") or classify_market(code),
            "board": row.get("board", ""),
            "exchange": row.get("exchange", ""),
            "industry": row.get("industry", "未知"),
            "list_source": row.get("source", "stock_basic"),
            "last_close": round(last_close, 3),
            "amount20": float(amount20),
            "bars": int(len(df)),
            "last_date": str(last.select("date").item()),
        }
    except Exception as e:
        log_reject(code, "universe", "exception", e, name=name)
        log_exception(f"历史过滤异常 code={code}", e)
        return None


def get_stock_candidates(force_refresh_basic: bool = False, basic_cache_days: int = 7) -> pl.DataFrame:
    basic = build_stock_basic(force_refresh=force_refresh_basic, cache_days=basic_cache_days)
    if basic.is_empty():
        get_logger().error("stock_basic 不可用，停止构建股票池")
        return pl.DataFrame()
    df = basic.filter(
        (pl.col("is_st") == False) &
        (pl.col("code").map_elements(valid_a_share_code, return_dtype=pl.Boolean))
    )
    if "is_valid_quote" in df.columns:
        # 已校验过就只保留有效 quote；未校验默认 True。
        df = df.filter(pl.col("is_valid_quote") != False)
    if df.is_empty():
        get_logger().error("stock_basic 过滤后为空，停止构建股票池")
    return df


def build_stock_universe(
    max_workers: int = 1,
    limit: Optional[int] = None,
    use_cache: bool = True,
    force_refresh_basic: bool = False,
    basic_cache_days: int = 7,
    allow_static_fallback: bool = False,  # 保留参数兼容旧入口，但不进入主流程
) -> pl.DataFrame:
    if use_cache:
        cache = _load_universe_cache()
        if not cache.is_empty():
            get_logger().info("使用股票池缓存：%s 只", len(cache))
            return cache.head(limit) if limit else cache

    if allow_static_fallback:
        get_logger().warning("方案D已将静态号段兜底退出主流程；该参数仅保留兼容，不会生成静态候选池。")

    candidates = get_stock_candidates(force_refresh_basic=force_refresh_basic, basic_cache_days=basic_cache_days)
    if candidates.is_empty():
        return pl.DataFrame()
    rows = candidates.to_dicts()
    if limit:
        rows = rows[:limit]

    print(f"开始历史过滤：候选 {len(rows)} 只...")
    get_logger().info("开始历史过滤：候选=%s limit=%s workers=%s", len(rows), limit, max_workers)
    kept: List[Dict[str, Any]] = []
    workers = max(1, int(max_workers or 1))
    processed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_history_filter_one, row): row.get("code", "") for row in rows}
        for fut in as_completed(futures):
            processed += 1
            try:
                res = fut.result()
                if res:
                    kept.append(res)
            except Exception as e:
                code = futures.get(fut, "")
                log_reject(code, "universe", "future_exception", e)
                log_exception(f"历史过滤线程异常 code={code}", e)
            if processed % 200 == 0:
                print(f"历史过滤进度：{processed}/{len(rows)}，保留 {len(kept)}")
                get_logger().info("历史过滤进度：%s/%s，保留=%s", processed, len(rows), len(kept))

    if not kept:
        get_logger().warning("股票池过滤后为空")
        summarize_rejects()
        return pl.DataFrame()
    out = pl.DataFrame(kept).sort(["amount20", "last_close"], descending=[True, True])
    os.makedirs(os.path.dirname(UNIVERSE_CACHE), exist_ok=True)
    out.write_parquet(UNIVERSE_CACHE)
    get_logger().info("股票池完成：%s 只，已写入 %s", len(out), UNIVERSE_CACHE)
    summarize_rejects()
    return out


def prepare_data(
    refresh_basic: bool = False,
    basic_cache_days: int = 7,
    sina_batch_size: int = 400,
    sina_batch_interval: float = 10.0,
    daily_limit: Optional[int] = None,
    daily_workers: int = 1,
) -> Dict[str, Any]:
    """数据准备总入口：基础表 -> 新浪实时校验 -> 历史K缓存。"""
    from core.data import validate_stock_basic_with_sina

    result: Dict[str, Any] = {}
    basic = build_stock_basic(force_refresh=refresh_basic, cache_days=basic_cache_days)
    if basic.is_empty():
        raise RuntimeError("stock_basic 不可用，停止 prepare-data")
    result["basic_count"] = len(basic)

    quote = validate_stock_basic_with_sina(batch_size=sina_batch_size, batch_interval=sina_batch_interval)
    if quote.is_empty():
        raise RuntimeError("新浪实时校验失败，停止 prepare-data")
    result["quote_count"] = len(quote)

    daily_stats = build_daily_cache(limit=daily_limit, workers=daily_workers)
    result["daily_stats"] = daily_stats
    return result



def get_coverage_stats(universe_count: Optional[int] = None) -> Dict[str, Any]:
    """读取本地缓存覆盖率。只查库，不请求远程。"""
    from core.data import get_db_connection, init_db
    init_db()
    stats: Dict[str, Any] = {
        "stock_basic": 0,
        "valid_basic": 0,
        "stock_daily_codes": 0,
        "realtime_quote_codes": 0,
        "daily_coverage_pct": 0.0,
        "quote_coverage_pct": 0.0,
        "universe": universe_count,
    }
    try:
        con = get_db_connection()
        try:
            stats["stock_basic"] = int(con.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0] or 0)
            stats["valid_basic"] = int(con.execute("SELECT COUNT(*) FROM stock_basic WHERE COALESCE(is_valid_quote, TRUE) = TRUE").fetchone()[0] or 0)
            stats["stock_daily_codes"] = int(con.execute("SELECT COUNT(DISTINCT code) FROM stock_daily WHERE adj_type = 'qfq'").fetchone()[0] or 0)
            stats["realtime_quote_codes"] = int(con.execute("SELECT COUNT(DISTINCT code) FROM realtime_quote").fetchone()[0] or 0)
        finally:
            con.close()
    except Exception as e:
        log_exception("读取覆盖率统计失败", e)
    base = stats["stock_basic"] or 0
    if base > 0:
        stats["daily_coverage_pct"] = round(stats["stock_daily_codes"] / base * 100, 2)
        stats["quote_coverage_pct"] = round(stats["realtime_quote_codes"] / base * 100, 2)
    return stats


def print_coverage_report(universe_count: Optional[int] = None) -> Dict[str, Any]:
    """打印数据覆盖率报告。"""
    stats = get_coverage_stats(universe_count=universe_count)
    lines = [
        "",
        "数据覆盖率报告：",
        f"  stock_basic：{stats['stock_basic']}",
        f"  有效基础股票：{stats['valid_basic']}",
        f"  stock_daily 覆盖股票：{stats['stock_daily_codes']} ({stats['daily_coverage_pct']}%)",
        f"  realtime_quote 覆盖股票：{stats['realtime_quote_codes']} ({stats['quote_coverage_pct']}%)",
    ]
    if universe_count is not None:
        lines.append(f"  universe：{universe_count}")
    msg = "\n".join(lines)
    print(msg)
    get_logger().info(msg)
    if stats["stock_basic"] and stats["daily_coverage_pct"] < 10:
        warn = "⚠️ 当前日线缓存覆盖率较低，扫描结果只代表已缓存股票，不代表全市场。"
        print(warn)
        get_logger().warning(warn)
    return stats
# ===== universe 修复版：直接基于 stock_daily 有效缓存构建股票池 =====

def _read_sql_df(sql: str):
    import polars as pl
    from core.data import get_db_connection

    con = get_db_connection()
    try:
        rows = con.execute(sql).fetchall()
        cols = [x[0] for x in con.description]
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        con.close()


def print_coverage_report():
    """打印真实有效覆盖率：只统计 stock_daily >=250 根的股票"""
    try:
        from core.data import get_db_connection

        con = get_db_connection()

        def one(sql: str):
            try:
                row = con.execute(sql).fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            except Exception:
                return 0

        stock_basic = one("SELECT COUNT(*) FROM stock_basic")

        valid_basic = one("""
        SELECT COUNT(*)
        FROM stock_basic
        WHERE code IS NOT NULL
        """)

        valid_daily = one("""
        SELECT COUNT(*)
        FROM (
            SELECT code
            FROM stock_daily
            GROUP BY code
            HAVING COUNT(*) >= 250
        )
        """)

        realtime_quote = one("""
        SELECT COUNT(DISTINCT code)
        FROM realtime_quote
        """)

        universe_count = 0
        try:
            import os
            import polars as pl
            if os.path.exists("data/universe.parquet"):
                universe_count = len(pl.read_parquet("data/universe.parquet"))
        except Exception:
            universe_count = 0

        cov = valid_daily / valid_basic * 100 if valid_basic else 0

        print("\n数据覆盖率报告：")
        print(f"  stock_basic：{stock_basic}")
        print(f"  有效基础股票：{valid_basic}")
        print(f"  stock_daily 有效缓存股票：{valid_daily} ({cov:.2f}%)")
        print(f"  realtime_quote 覆盖股票：{realtime_quote}")
        print(f"  universe 股票池：{universe_count}")

        if cov < 20:
            print("⚠️ 当前日线缓存覆盖率较低，扫描结果只代表已缓存股票，不代表全市场。")

        con.close()

    except Exception as e:
        print(f"⚠️ 覆盖率报告失败：{e}")


def build_stock_universe(limit=None, workers=1, **kwargs):
    """
    修复版股票池构建：
    1. 不再遍历 5317 只逐只拉数据
    2. 只读取 stock_daily 中已有 >=250 根日线的股票
    3. 与 stock_basic 关联
    4. 剔除 ST、低价、低成交额
    5. 写入 data/universe.parquet
    """
    import os
    import polars as pl

    try:
        from core.logger import get_logger
        logger = get_logger()
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("a_stock")

    sql = """
    WITH daily_ranked AS (
        SELECT
            code,
            date,
            close,
            amount,
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
        FROM stock_daily
    ),
    bars AS (
        SELECT
            code,
            COUNT(*) AS bars,
            MAX(date) AS last_date
        FROM stock_daily
        GROUP BY code
        HAVING COUNT(*) >= 250
    ),
    latest AS (
        SELECT
            code,
            close AS last_close
        FROM daily_ranked
        WHERE rn = 1
    ),
    amount20 AS (
        SELECT
            code,
            AVG(amount) AS amount20
        FROM daily_ranked
        WHERE rn <= 20
        GROUP BY code
    )
    SELECT
        b.*,
        bars.bars,
        bars.last_date,
        latest.last_close,
        amount20.amount20
    FROM stock_basic b
    INNER JOIN bars
        ON b.code = bars.code
    INNER JOIN latest
        ON b.code = latest.code
    INNER JOIN amount20
        ON b.code = amount20.code
    """

    df = _read_sql_df(sql)

    if df.is_empty():
        print("⚠️ 没有找到有效日线缓存股票，请先执行 build-daily-cache")
        logger.warning("没有找到有效日线缓存股票")
        return pl.DataFrame()

    # 兼容字段缺失
    if "name" not in df.columns:
        df = df.with_columns(pl.lit("").alias("name"))

    if "market" not in df.columns:
        df = df.with_columns(pl.lit("").alias("market"))

    if "board" not in df.columns:
        df = df.with_columns(pl.lit("").alias("board"))

    if "is_st" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("is_st"))

    # 类型整理
    df = df.with_columns([
        pl.col("code").cast(pl.Utf8),
        pl.col("name").cast(pl.Utf8),
        pl.col("last_close").cast(pl.Float64, strict=False),
        pl.col("amount20").cast(pl.Float64, strict=False),
        pl.col("bars").cast(pl.Int64, strict=False),
        pl.col("last_date").cast(pl.Utf8),
    ])

    before = len(df)

    # 股票池过滤
    df = df.filter(
        (pl.col("last_close").fill_null(0) >= 3)
        & (pl.col("amount20").fill_null(0) >= 80_000_000)
        & (~pl.col("name").str.contains("ST", literal=False).fill_null(False))
        & (~pl.col("name").str.contains("退", literal=False).fill_null(False))
    )

    filtered = len(df)

    # 按成交额排序，优先活跃股
    df = df.sort("amount20", descending=True)

    if limit:
        df = df.head(limit)

    os.makedirs("data", exist_ok=True)
    df.write_parquet("data/universe.parquet")

    print("\n股票池构建报告：")
    print(f"  有效日线缓存股票：{before}")
    print(f"  过滤后股票池：{filtered}")
    print(f"  输出股票池：{len(df)}")
    print("  过滤条件：价格>=3，20日均成交额>=8000万，剔除ST/退市")

    logger.info(
        f"股票池完成：valid_daily={before}, filtered={filtered}, output={len(df)}，已写入 data/universe.parquet"
    )

    return df


def prepare_data(*args, **kwargs):
    """
    兼容旧 run_scan.py 的 prepare_data 引用。
    """
    return None

if __name__ == "__main__":
    from core.logger import setup_logger
    setup_logger(level="INFO")
    print(build_stock_universe(max_workers=1, limit=20, use_cache=False))
