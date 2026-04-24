# -*- coding: utf-8 -*-
"""
core/sector.py

方案 G：板块过滤 V1

作用：
1. 用 AkShare 东方财富行业板块成分构建本地缓存；
2. 基于已缓存的 stock_daily 计算板块强度；
3. 在扫描前只保留强势板块中的股票。

注意：
- 本模块默认不拉个股历史 K，只读 stock_daily 缓存。
- 如果板块数据源失败且没有缓存，返回空，主流程会保守停止扫描。
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple

import polars as pl

from core.data import get_db_connection
from core.logger import get_logger, log_exception

SECTOR_CACHE_PATH = "data/sector_members.parquet"
SECTOR_CACHE_DAYS = 7


def _cache_fresh(path: str, days: int = SECTOR_CACHE_DAYS) -> bool:
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime <= timedelta(days=days)


def _normalize_code(code: str) -> str:
    s = str(code).strip()
    if not s:
        return ""
    # AkShare 有时会把代码读成数字，补足 6 位
    if s.isdigit():
        return s.zfill(6)
    # 去掉可能的交易所后缀/前缀
    s = s.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    s = s.replace("sh", "").replace("sz", "").replace("bj", "")
    return s.zfill(6) if s.isdigit() else s


def _is_a_share_code(code: str) -> bool:
    c = _normalize_code(code)
    return c.startswith(("600", "601", "603", "605", "688", "000", "001", "002", "003", "300", "301", "920"))


def _pick_col(cols: Iterable[str], candidates: Iterable[str]) -> str | None:
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


def load_sector_members(use_cache: bool = True, force_refresh: bool = False) -> pl.DataFrame:
    """读取或刷新行业板块成分缓存。

    返回字段：code, sector_name, sector_source, updated_at
    """
    os.makedirs("data", exist_ok=True)

    if use_cache and not force_refresh and _cache_fresh(SECTOR_CACHE_PATH):
        try:
            df = pl.read_parquet(SECTOR_CACHE_PATH)
            if not df.is_empty():
                get_logger().info("使用板块成分缓存：%s 条", len(df))
                return df
        except Exception as e:
            get_logger().warning("读取板块成分缓存失败：%s", e)

    try:
        import akshare as ak
    except Exception as e:
        get_logger().warning("未安装或无法导入 AkShare，无法刷新板块成分：%s", e)
        if os.path.exists(SECTOR_CACHE_PATH):
            return pl.read_parquet(SECTOR_CACHE_PATH)
        return pl.DataFrame(schema={"code": pl.Utf8, "sector_name": pl.Utf8, "sector_source": pl.Utf8, "updated_at": pl.Datetime})

    try:
        board_df = ak.stock_board_industry_name_em()
        name_col = _pick_col(board_df.columns, ["板块名称", "名称", "行业名称"])
        if name_col is None:
            raise ValueError(f"无法识别行业板块名称字段: {board_df.columns}")

        rows = []
        names = [str(x).strip() for x in board_df[name_col].dropna().tolist() if str(x).strip()]
        get_logger().info("开始刷新行业板块成分：%s 个行业", len(names))

        for i, sector_name in enumerate(names, start=1):
            try:
                cons = ak.stock_board_industry_cons_em(symbol=sector_name)
                code_col = _pick_col(cons.columns, ["代码", "股票代码", "code"])
                if code_col is None:
                    get_logger().warning("行业成分字段异常 sector=%s cols=%s", sector_name, cons.columns)
                    continue
                for raw_code in cons[code_col].dropna().tolist():
                    code = _normalize_code(raw_code)
                    if _is_a_share_code(code):
                        rows.append({
                            "code": code,
                            "sector_name": sector_name,
                            "sector_source": "akshare_em_industry",
                            "updated_at": datetime.now(),
                        })
            except Exception as e:
                get_logger().warning("获取行业成分失败 sector=%s err=%s", sector_name, e)
                continue

            # 轻微限速，避免东方财富接口被打爆
            if i % 10 == 0:
                get_logger().info("行业成分刷新进度：%s/%s", i, len(names))
                time.sleep(1.0)
            else:
                time.sleep(0.2)

        if not rows:
            raise RuntimeError("行业成分刷新结果为空")

        df = pl.DataFrame(rows).unique(subset=["code", "sector_name"])
        df.write_parquet(SECTOR_CACHE_PATH)
        get_logger().info("行业板块成分缓存完成：%s 条，文件=%s", len(df), SECTOR_CACHE_PATH)
        return df

    except Exception as e:
        log_exception("刷新行业板块成分失败", e)
        if os.path.exists(SECTOR_CACHE_PATH):
            get_logger().warning("板块源失败，使用旧缓存：%s", SECTOR_CACHE_PATH)
            return pl.read_parquet(SECTOR_CACHE_PATH)
        return pl.DataFrame(schema={"code": pl.Utf8, "sector_name": pl.Utf8, "sector_source": pl.Utf8, "updated_at": pl.Datetime})


def _query_daily_for_codes(codes: List[str]) -> pl.DataFrame:
    codes = [_normalize_code(c) for c in codes if _is_a_share_code(c)]
    if not codes:
        return pl.DataFrame(schema={"code": pl.Utf8, "date": pl.Date, "close": pl.Float64, "amount": pl.Float64})

    # 只拼接已经校验过的 6 位数字代码，避免 SQL 注入风险
    in_list = ",".join([f"'{c}'" for c in sorted(set(codes))])
    sql = f"""
        SELECT code, date, close, volume, amount
        FROM stock_daily
        WHERE code IN ({in_list})
        ORDER BY code, date
    """

    con = get_db_connection()
    try:
        cur = con.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        con.close()

    if not rows:
        return pl.DataFrame(schema={"code": pl.Utf8, "date": pl.Date, "close": pl.Float64, "volume": pl.Int64, "amount": pl.Float64})
    return pl.DataFrame(rows, schema=cols, orient="row")


def _stock_metrics_from_daily(daily: pl.DataFrame) -> pl.DataFrame:
    """从日线缓存计算个股 5/10 日涨幅、是否上涨、是否站上 MA20、量能放大。"""
    if daily is None or daily.is_empty():
        return pl.DataFrame(schema={
            "code": pl.Utf8,
            "ret5": pl.Float64,
            "ret10": pl.Float64,
            "is_up": pl.Int8,
            "above_ma20": pl.Int8,
            "amount_ratio": pl.Float64,
        })

    metrics = []
    for item in daily.group_by("code", maintain_order=True):
        code, g = item
        if isinstance(code, tuple):
            code = code[0]
        g = g.sort("date")
        if len(g) < 21:
            continue
        closes = [float(x) for x in g["close"].to_list() if x is not None]
        amounts = []
        for r in g.select(["close", "volume", "amount"]).iter_rows(named=True):
            amount = r.get("amount")
            if amount is None or amount <= 0:
                close = r.get("close") or 0
                vol = r.get("volume") or 0
                amount = float(close) * float(vol) * 100
            amounts.append(float(amount))
        if len(closes) < 21 or len(amounts) < 21:
            continue
        last = closes[-1]
        prev = closes[-2]
        ret5 = last / closes[-6] - 1 if len(closes) >= 6 and closes[-6] else 0.0
        ret10 = last / closes[-11] - 1 if len(closes) >= 11 and closes[-11] else 0.0
        ma20 = sum(closes[-20:]) / 20
        amt5 = sum(amounts[-5:]) / 5
        amt20_prev = sum(amounts[-25:-5]) / 20 if len(amounts) >= 25 else sum(amounts[-20:]) / 20
        amount_ratio = amt5 / amt20_prev if amt20_prev else 1.0
        metrics.append({
            "code": str(code),
            "ret5": float(ret5),
            "ret10": float(ret10),
            "is_up": 1 if last > prev else 0,
            "above_ma20": 1 if last > ma20 else 0,
            "amount_ratio": float(amount_ratio),
        })
    return pl.DataFrame(metrics) if metrics else pl.DataFrame(schema={
        "code": pl.Utf8,
        "ret5": pl.Float64,
        "ret10": pl.Float64,
        "is_up": pl.Int8,
        "above_ma20": pl.Int8,
        "amount_ratio": pl.Float64,
    })


def calculate_sector_strength(universe: pl.DataFrame) -> pl.DataFrame:
    """计算行业强度评分。"""
    if universe is None or universe.is_empty() or "code" not in universe.columns:
        return pl.DataFrame()

    members = load_sector_members(use_cache=True)
    if members.is_empty():
        get_logger().warning("行业成分为空，无法计算板块强度")
        return pl.DataFrame()

    codes = [str(x) for x in universe["code"].to_list()]
    daily = _query_daily_for_codes(codes)
    metrics = _stock_metrics_from_daily(daily)
    if metrics.is_empty():
        get_logger().warning("日线缓存不足，无法计算板块强度")
        return pl.DataFrame()

    joined = members.join(metrics, on="code", how="inner")
    if joined.is_empty():
        get_logger().warning("行业成分与股票池无交集，无法计算板块强度")
        return pl.DataFrame()

    sector = (
        joined.group_by("sector_name")
        .agg([
            pl.len().alias("member_count"),
            pl.col("ret5").mean().alias("ret5"),
            pl.col("ret10").mean().alias("ret10"),
            pl.col("is_up").mean().alias("up_ratio"),
            pl.col("above_ma20").mean().alias("ma20_ratio"),
            pl.col("amount_ratio").mean().alias("amount_ratio"),
        ])
        .filter(pl.col("member_count") >= 3)
    )
    if sector.is_empty():
        return sector

    n = max(len(sector), 1)
    sector = sector.with_columns([
        (pl.col("ret5").rank("average") / n * 25).alias("ret5_score"),
        (pl.col("ret10").rank("average") / n * 25).alias("ret10_score"),
        (pl.col("up_ratio").clip(0, 1) * 20).alias("up_score"),
        (pl.col("ma20_ratio").clip(0, 1) * 20).alias("ma20_score"),
        (((pl.col("amount_ratio") - 0.8) / 0.7).clip(0, 1) * 10).alias("amount_score"),
    ]).with_columns([
        (pl.col("ret5_score") + pl.col("ret10_score") + pl.col("up_score") + pl.col("ma20_score") + pl.col("amount_score")).alias("sector_score")
    ]).sort("sector_score", descending=True)

    return sector


def get_top_sectors(universe: pl.DataFrame, top_pct: float = 0.20, max_workers: int = 1) -> Tuple[pl.DataFrame, List[str]]:
    """返回板块强度表和强势行业列表。

    top_pct: 强势市场取 0.20；震荡市场可由 market.py 给 0.10-0.15。
    """
    sector_table = calculate_sector_strength(universe)
    if sector_table is None or sector_table.is_empty():
        return pl.DataFrame(), []

    top_pct = max(0.05, min(float(top_pct or 0.20), 0.50))
    top_n = max(1, int(len(sector_table) * top_pct))
    top = sector_table.head(top_n)
    top_sectors = [str(x) for x in top["sector_name"].to_list()]

    get_logger().info("强势行业筛选：top_pct=%.2f top_n=%s total=%s", top_pct, top_n, len(sector_table))
    return sector_table, top_sectors


def filter_universe_by_sectors(universe: pl.DataFrame, top_sectors: List[str]) -> pl.DataFrame:
    """只保留强势行业内的股票，并补充 industry 字段。"""
    if universe is None or universe.is_empty() or not top_sectors:
        return pl.DataFrame()

    members = load_sector_members(use_cache=True)
    if members.is_empty():
        return pl.DataFrame()

    mapping = (
        members.filter(pl.col("sector_name").is_in(top_sectors))
        .select(["code", pl.col("sector_name").alias("industry")])
        .unique(subset=["code"], keep="first")
    )
    out = universe.join(mapping, on="code", how="inner")
    if out.is_empty():
        return out
    return out.unique(subset=["code"], keep="first")
