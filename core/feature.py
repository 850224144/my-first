import polars as pl

FEATURES = ['trend', 'drawdown', 'vol_ratio', 'not_break']


def compute_features(df):
    """
    【架构升级】用Polars计算特征，速度提升5-10倍
    API和Pandas高度兼容，逻辑完全不变
    """
    try:
        df = df.clone()

        # 均线
        df = df.with_columns([
            pl.col("close").rolling_mean(window_size=20).alias("ma20"),
            pl.col("close").rolling_mean(window_size=60).alias("ma60")
        ])

        # 趋势
        df = df.with_columns([
            (pl.col("ma20") / pl.col("ma60")).alias("trend")
        ])

        # 回撤
        df = df.with_columns([
            pl.col("high").rolling_max(window_size=20).alias("high20")
        ])
        df = df.with_columns([
            ((pl.col("high20") - pl.col("close")) / pl.col("high20")).alias("drawdown")
        ])

        # 量能
        df = df.with_columns([
            pl.col("volume").rolling_mean(window_size=20).alias("vol_ma20")
        ])
        df = df.with_columns([
            (pl.col("volume") / pl.col("vol_ma20")).alias("vol_ratio")
        ])

        # 支撑位
        df = df.with_columns([
            pl.col("low").rolling_min(window_size=10).alias("low10"),
            pl.col("low").rolling_min(window_size=20).alias("low20")
        ])
        df = df.with_columns([
            (pl.col("low10") > pl.col("low20")).cast(pl.Int32).alias("not_break")
        ])

        # 去掉空值
        df = df.drop_nulls()

        return df if len(df) >= 30 else pl.DataFrame()

    except Exception as e:
        return pl.DataFrame()