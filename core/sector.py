# -*- coding: utf-8 -*-
"""
core/sector.py

方案 H：板块过滤 V2

核心思路：
1. 先获取行业板块行情，判断哪些板块热；
2. 只对强势板块获取成分股，减少请求量；
3. 默认调试模式下，板块数据失败时由 run_scan.py 跳过板块过滤；
4. 严格模式 --strict-sector 下，板块数据失败则停止扫描。

数据源优先级：
- 同花顺行业一览表：ak.stock_board_industry_summary_ths()
- 东方财富行业板块：ak.stock_board_industry_name_em()
- 本地缓存：data/sector_hot.parquet / data/sector_members.parquet
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple, Optional

import polars as pl

from core.logger import get_logger, log_exception

SECTOR_HOT_CACHE = "data/sector_hot.parquet"
SECTOR_MEMBER_CACHE = "data/sector_members.parquet"
SECTOR_CACHE_DAYS = 3


def _cache_fresh(path: str, days: int = SECTOR_CACHE_DAYS) -> bool:
    if not os.path.exists(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime <= timedelta(days=days)


def _pick_col(cols: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


def _to_float_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.replace_all("%", "")
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
    )


def normalize_code(code: str) -> str:
    s = str(code).strip()
    if not s:
        return ""
    s = s.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    s = s.replace("sh", "").replace("sz", "").replace("bj", "")
    return s.zfill(6) if s.isdigit() else s


def valid_a_share_code(code: str) -> bool:
    c = normalize_code(code)
    return c.startswith(("600", "601", "603", "605", "688", "000", "001", "002", "003", "300", "301", "920"))


def _standardize_hot(df, source: str) -> pl.DataFrame:
    """把 AkShare 行业行情表统一为 sector/pct_chg/amount/turnover/source。"""
    if df is None or len(df) == 0:
        return pl.DataFrame()

    pdf_cols = list(df.columns)
    sector_col = _pick_col(pdf_cols, ["板块名称", "行业名称", "板块", "名称", "行业"])
    pct_col = _pick_col(pdf_cols, ["涨跌幅", "涨幅", "涨跌幅%", "涨幅%"])
    amount_col = _pick_col(pdf_cols, ["成交额", "总成交额", "金额", "成交金额"])
    turnover_col = _pick_col(pdf_cols, ["换手率", "换手", "总换手率", "换手率%"])

    if not sector_col:
        get_logger().warning("板块行情字段不含名称列 source=%s cols=%s", source, pdf_cols)
        return pl.DataFrame()

    pldf = pl.from_pandas(df)
    exprs = [
        pl.col(sector_col).cast(pl.Utf8).alias("sector"),
        pl.lit(source).alias("source"),
        pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("updated_at"),
    ]
    exprs.append(_to_float_expr(pct_col).alias("pct_chg") if pct_col else pl.lit(None).cast(pl.Float64).alias("pct_chg"))
    exprs.append(_to_float_expr(amount_col).alias("amount") if amount_col else pl.lit(None).cast(pl.Float64).alias("amount"))
    exprs.append(_to_float_expr(turnover_col).alias("turnover") if turnover_col else pl.lit(None).cast(pl.Float64).alias("turnover"))

    out = pldf.select(exprs).filter(pl.col("sector").is_not_null() & (pl.col("sector") != ""))
    return out.unique(subset=["sector"], keep="first")


def fetch_sector_hot(force_refresh: bool = False) -> pl.DataFrame:
    """获取行业热度行情表，优先 THS，备用 EM，失败用缓存。"""
    os.makedirs("data", exist_ok=True)
    if not force_refresh and _cache_fresh(SECTOR_HOT_CACHE):
        try:
            cached = pl.read_parquet(SECTOR_HOT_CACHE)
            if not cached.is_empty():
                get_logger().info("使用板块热度缓存：%s 个行业", len(cached))
                return cached
        except Exception as e:
            log_exception("读取板块热度缓存失败", e)

    try:
        import akshare as ak
        df = ak.stock_board_industry_summary_ths()
        hot = _standardize_hot(df, source="ths")
        if not hot.is_empty():
            hot.write_parquet(SECTOR_HOT_CACHE)
            get_logger().info("同花顺行业热度获取成功：%s 个行业", len(hot))
            return hot
    except Exception as e:
        log_exception("同花顺行业热度获取失败", e)

    try:
        import akshare as ak
        df = ak.stock_board_industry_name_em()
        hot = _standardize_hot(df, source="eastmoney")
        if not hot.is_empty():
            hot.write_parquet(SECTOR_HOT_CACHE)
            get_logger().info("东方财富行业热度获取成功：%s 个行业", len(hot))
            return hot
    except Exception as e:
        log_exception("东方财富行业热度获取失败", e)

    if os.path.exists(SECTOR_HOT_CACHE):
        try:
            cached = pl.read_parquet(SECTOR_HOT_CACHE)
            if not cached.is_empty():
                get_logger().warning("行业热度远程失败，使用旧缓存：%s 个行业", len(cached))
                return cached
        except Exception as e:
            log_exception("读取旧板块热度缓存失败", e)

    return pl.DataFrame()


def score_sector_hot(hot: pl.DataFrame) -> pl.DataFrame:
    """板块热度评分：涨跌幅50、成交额30、换手20；缺字段自动降级。"""
    if hot is None or hot.is_empty():
        return pl.DataFrame()

    df = hot.clone()
    n = max(1, len(df))

    score_exprs = []
    if "pct_chg" in df.columns and df["pct_chg"].drop_nulls().len() > 0:
        df = df.with_columns(pl.col("pct_chg").rank("average", descending=False).alias("pct_rank"))
        score_exprs.append((pl.col("pct_rank") / n * 50).fill_null(0))
    if "amount" in df.columns and df["amount"].drop_nulls().len() > 0:
        df = df.with_columns(pl.col("amount").rank("average", descending=False).alias("amount_rank"))
        score_exprs.append((pl.col("amount_rank") / n * 30).fill_null(0))
    if "turnover" in df.columns and df["turnover"].drop_nulls().len() > 0:
        df = df.with_columns(pl.col("turnover").rank("average", descending=False).alias("turnover_rank"))
        score_exprs.append((pl.col("turnover_rank") / n * 20).fill_null(0))

    if not score_exprs:
        df = df.with_columns(pl.lit(0.0).alias("score"))
    else:
        total = score_exprs[0]
        for expr in score_exprs[1:]:
            total = total + expr
        df = df.with_columns(total.alias("score"))

    keep_cols = [c for c in ["sector", "score", "pct_chg", "amount", "turnover", "source", "updated_at"] if c in df.columns]
    return df.select(keep_cols).sort("score", descending=True)


def _standardize_members(df, sector: str, source: str) -> pl.DataFrame:
    if df is None or len(df) == 0:
        return pl.DataFrame()
    cols = list(df.columns)
    code_col = _pick_col(cols, ["代码", "股票代码", "成分股代码", "证券代码"])
    name_col = _pick_col(cols, ["名称", "股票名称", "成分股名称", "证券简称"])
    if not code_col:
        get_logger().warning("行业成分字段不含代码列 sector=%s source=%s cols=%s", sector, source, cols)
        return pl.DataFrame()
    pldf = pl.from_pandas(df)
    out = pldf.select([
        pl.col(code_col).cast(pl.Utf8).map_elements(normalize_code, return_dtype=pl.Utf8).alias("code"),
        (pl.col(name_col).cast(pl.Utf8) if name_col else pl.lit("")) .alias("name"),
        pl.lit(sector).alias("sector"),
        pl.lit(source).alias("member_source"),
        pl.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("updated_at"),
    ])
    return out.filter(pl.col("code").map_elements(valid_a_share_code, return_dtype=pl.Boolean)).unique(subset=["code", "sector"])


def fetch_sector_members_for(sectors: List[str], sleep_sec: float = 0.8) -> pl.DataFrame:
    """只对强势板块拉成分股。优先 EM 成分接口。"""
    rows: List[pl.DataFrame] = []
    try:
        import akshare as ak
    except Exception as e:
        log_exception("导入 AkShare 失败，无法获取板块成分", e)
        return pl.DataFrame()

    for sector in sectors:
        try:
            df = ak.stock_board_industry_cons_em(symbol=sector)
            part = _standardize_members(df, sector=sector, source="eastmoney_cons")
            if not part.is_empty():
                rows.append(part)
                get_logger().info("板块成分获取成功 sector=%s count=%s", sector, len(part))
            else:
                get_logger().warning("板块成分为空 sector=%s", sector)
        except Exception as e:
            log_exception(f"板块成分获取失败 sector={sector}", e)
        time.sleep(max(0.0, sleep_sec))

    if not rows:
        return pl.DataFrame()
    out = pl.concat(rows, how="vertical_relaxed").unique(subset=["code", "sector"])
    os.makedirs("data", exist_ok=True)
    out.write_parquet(SECTOR_MEMBER_CACHE)
    return out


def get_top_sectors(universe: pl.DataFrame | None = None, top_pct: float = 0.2, max_workers: int = 1, force_refresh: bool = False) -> Tuple[pl.DataFrame, List[str]]:
    """返回板块热度表与强势板块列表。"""
    hot = fetch_sector_hot(force_refresh=force_refresh)
    table = score_sector_hot(hot)
    if table.is_empty():
        get_logger().warning("板块热度表为空，无法计算强势板块")
        return pl.DataFrame(), []

    pct = min(max(float(top_pct or 0.2), 0.01), 1.0)
    top_n = max(1, int(len(table) * pct))
    top = table.head(top_n)
    sectors = top["sector"].to_list()

    members = fetch_sector_members_for(sectors)
    if members.is_empty():
        get_logger().warning("强势板块成分为空，可能是板块名称与成分接口不匹配")
    else:
        get_logger().info("强势板块成分合计：%s 条", len(members))

    print("\n板块过滤报告：")
    print(f"  行业行情源：{top['source'][0] if 'source' in top.columns and len(top) else 'unknown'}")
    print(f"  强势板块数量：{len(sectors)}")
    print(f"  保留比例：{pct:.0%}")
    print("  强势板块Top：")
    print(top.select([c for c in ["sector", "score", "pct_chg", "amount", "turnover"] if c in top.columns]).head(20))

    return table, sectors


def _load_member_cache() -> pl.DataFrame:
    if not os.path.exists(SECTOR_MEMBER_CACHE):
        return pl.DataFrame()
    try:
        df = pl.read_parquet(SECTOR_MEMBER_CACHE)
        if df is not None and not df.is_empty():
            return df.with_columns(pl.col("code").cast(pl.Utf8).map_elements(normalize_code, return_dtype=pl.Utf8).alias("code"))
    except Exception as e:
        log_exception("读取板块成分缓存失败", e)
    return pl.DataFrame()


def filter_universe_by_sectors(universe: pl.DataFrame, top_sectors: List[str]) -> pl.DataFrame:
    """按强势板块成分过滤股票池。"""
    if universe is None or universe.is_empty() or not top_sectors:
        return pl.DataFrame()
    members = _load_member_cache()
    if members.is_empty():
        get_logger().warning("行业成分缓存为空，无法按板块过滤")
        return pl.DataFrame()
    members = members.filter(pl.col("sector").is_in(top_sectors)).select(["code", "sector"]).unique()
    if members.is_empty():
        get_logger().warning("强势板块命中成分为空")
        return pl.DataFrame()

    uni = universe.with_columns(pl.col("code").cast(pl.Utf8).map_elements(normalize_code, return_dtype=pl.Utf8).alias("code"))
    out = uni.join(members, on="code", how="inner")
    print(f"📊 板块过滤后剩余：{len(out)} / {len(universe)}")
    return out
