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
    get_data,
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
        # 优先使用本地 qfq 缓存；没有再远程获取。此处是股票池构建的最终历史过滤。
        df = load_from_db(code, adj_type="qfq")
        if df is None or len(df) < MIN_LISTING_DAYS:
            df = get_data(code, bars=520)
        if df is None or len(df) == 0:
            log_reject(code, "universe", "no_kline_data", "无历史K线", name=name)
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


if __name__ == "__main__":
    from core.logger import setup_logger
    setup_logger(level="INFO")
    print(build_stock_universe(max_workers=1, limit=20, use_cache=False))
