# -*- coding: utf-8 -*-
"""
core/feature.py

Polars 特征层。
所有策略、模型、回测统一使用这些字段，避免 pandas/polars 混用。
"""

from __future__ import annotations

import polars as pl

FEATURES = [
    "trend",              # ma20 / ma60
    "drawdown20",         # 20日高点回撤
    "vol_ratio",          # 当前量 / 20日均量
    "amount_ratio",       # 当前成交额 / 20日均成交额
    "not_break",          # 10日低点是否高于20日低点
    "close_ma20_ratio",   # close / ma20
    "ma20_slope20",       # 20日均线斜率
    "pct_chg",            # 日涨跌幅
]


def compute_features(df: pl.DataFrame) -> pl.DataFrame:
    """计算通用特征。输入输出均为 Polars DataFrame。"""
    if df is None or len(df) < 120:
        return pl.DataFrame()

    try:
        out = df.clone()
        if "amount" not in out.columns:
            out = out.with_columns((pl.col("close") * pl.col("volume") * 100).cast(pl.Float64).alias("amount"))

        out = out.sort("date")
        out = out.with_columns(
            [
                pl.col("close").pct_change().fill_null(0.0).alias("pct_chg"),
                pl.col("close").rolling_mean(5).alias("ma5"),
                pl.col("close").rolling_mean(10).alias("ma10"),
                pl.col("close").rolling_mean(20).alias("ma20"),
                pl.col("close").rolling_mean(60).alias("ma60"),
                pl.col("close").rolling_mean(120).alias("ma120"),
                pl.col("volume").rolling_mean(5).alias("vol_ma5"),
                pl.col("volume").rolling_mean(20).alias("vol_ma20"),
                pl.col("amount").rolling_mean(20).alias("amount_ma20"),
                pl.col("high").rolling_max(20).alias("high20"),
                pl.col("high").rolling_max(60).alias("high60"),
                pl.col("low").rolling_min(10).alias("low10"),
                pl.col("low").rolling_min(20).alias("low20"),
                pl.col("low").rolling_min(60).alias("low60"),
            ]
        )

        out = out.with_columns(
            [
                (pl.col("ma20") / pl.col("ma60")).alias("trend"),
                ((pl.col("high20") - pl.col("close")) / pl.col("high20")).alias("drawdown20"),
                (pl.col("volume") / pl.col("vol_ma20")).alias("vol_ratio"),
                (pl.col("amount") / pl.col("amount_ma20")).alias("amount_ratio"),
                (pl.col("low10") > pl.col("low20")).cast(pl.Int32).alias("not_break"),
                (pl.col("close") / pl.col("ma20")).alias("close_ma20_ratio"),
                ((pl.col("ma20") / pl.col("ma20").shift(20)) - 1).alias("ma20_slope20"),
                ((pl.col("ma60") / pl.col("ma60").shift(20)) - 1).alias("ma60_slope20"),
                ((pl.col("high") - pl.col("close")) / (pl.col("high") - pl.col("low"))).fill_nan(0).fill_null(0).alias("upper_shadow_ratio"),
                ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low"))).fill_nan(0).fill_null(0).alias("close_position"),
            ]
        )

        out = out.with_columns(
            [
                (pl.col("pct_chg") >= 0.095).cast(pl.Int32).alias("limit_up_like"),
                (
                    (pl.col("pct_chg") >= 0.095)
                    & ((pl.col("high") - pl.col("low")) / pl.col("close") < 0.005)
                ).cast(pl.Int32).alias("one_price_limit_like"),
                (
                    (pl.col("pct_chg") <= -0.05)
                    & (pl.col("volume") > pl.col("vol_ma20") * 1.2)
                ).cast(pl.Int32).alias("big_volume_down"),
            ]
        )

        out = out.drop_nulls(subset=["ma60", "vol_ma20", "amount_ma20", "trend"])
        return out if len(out) >= 60 else pl.DataFrame()
    except Exception as e:
        print(f"⚠️ 特征计算失败: {e}")
        return pl.DataFrame()
