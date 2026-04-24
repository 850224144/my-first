# -*- coding: utf-8 -*-
"""
core/sector.py

板块/行业强度过滤。
目标：先筛强势行业，再在强势行业里找二买，减少噪音。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Tuple

import polars as pl

from core.data import get_data
from core.feature import compute_features


def _stock_strength(row: Dict[str, Any]) -> Dict[str, Any] | None:
    code = row["code"]
    industry = row.get("industry") or "未知"
    df = get_data(code, bars=120)
    if df is None or len(df) < 80:
        return None
    feat = compute_features(df)
    if feat.is_empty() or len(feat) < 60:
        return None

    try:
        last = feat.tail(1)
        close = float(last.select("close").item())
        ma20 = float(last.select("ma20").item())
        ma60 = float(last.select("ma60").item())
        close_20 = float(feat.tail(21).head(1).select("close").item())
        close_60 = float(feat.tail(61).head(1).select("close").item())
        high60 = float(feat.tail(60).select(pl.max("high")).item())
        ret20 = close / close_20 - 1 if close_20 > 0 else 0
        ret60 = close / close_60 - 1 if close_60 > 0 else 0
        near_high = close >= high60 * 0.95
        return {
            "code": code,
            "industry": industry,
            "ret20": ret20,
            "ret60": ret60,
            "above_ma20": int(close >= ma20),
            "above_ma60": int(close >= ma60),
            "near_high60": int(near_high),
        }
    except Exception:
        return None


def calc_sector_strength(universe: pl.DataFrame, max_workers: int = 8) -> pl.DataFrame:
    """计算行业强度排名。"""
    if universe is None or universe.is_empty():
        return pl.DataFrame()

    rows = universe.to_dicts()
    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_stock_strength, row) for row in rows]
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                res = fut.result()
                if res:
                    results.append(res)
            except Exception:
                pass
            if done % 300 == 0:
                print(f"板块强度计算进度：{done}/{len(rows)}")

    if not results:
        return pl.DataFrame()

    df = pl.DataFrame(results)
    sector = df.group_by("industry").agg(
        [
            pl.count().alias("stock_count"),
            pl.mean("ret20").alias("ret20_mean"),
            pl.mean("ret60").alias("ret60_mean"),
            pl.mean("above_ma20").alias("above_ma20_ratio"),
            pl.mean("above_ma60").alias("above_ma60_ratio"),
            pl.mean("near_high60").alias("near_high60_ratio"),
        ]
    ).filter(pl.col("stock_count") >= 5)

    if sector.is_empty():
        return sector

    # 简单稳健评分，避免过度复杂化
    sector = sector.with_columns(
        (
            pl.col("ret20_mean").rank("average") / pl.count()
            + pl.col("ret60_mean").rank("average") / pl.count()
            + pl.col("above_ma20_ratio")
            + pl.col("above_ma60_ratio")
            + pl.col("near_high60_ratio")
        ).alias("sector_score")
    ).sort("sector_score", descending=True)

    return sector


def get_top_sectors(universe: pl.DataFrame, top_pct: float = 0.20, max_workers: int = 8) -> Tuple[pl.DataFrame, List[str]]:
    """返回强势行业表与行业名称列表。"""
    sector = calc_sector_strength(universe, max_workers=max_workers)
    if sector.is_empty():
        return sector, []
    n = max(1, int(len(sector) * top_pct))
    top = sector.head(n)
    names = top.select("industry").to_series().to_list()
    return top, names


def filter_universe_by_sectors(universe: pl.DataFrame, sectors: List[str]) -> pl.DataFrame:
    if universe is None or universe.is_empty() or not sectors:
        return pl.DataFrame()
    return universe.filter(pl.col("industry").is_in(sectors))
