import polars as pl
from core.data import get_data


def market_filter(debug=True):
    """大盘环境过滤：上证指数站上20日线才允许选股"""
    df = get_data("sh000001")

    if df is None or len(df) < 60:
        df = get_data("sz399001")

    if df is None or len(df) < 60:
        if debug:
            print("⚠️  大盘数据获取失败，默认放行选股")
        return True

    # 【兼容】Polars计算20日线
    df = df.with_columns(pl.col("close").rolling_mean(window_size=20).alias("ma20"))
    last = df.slice(-1)
    close = last.select(pl.col("close")).item()
    ma20 = last.select(pl.col("ma20")).item()
    last_date = last.select(pl.col("date")).item()

    if debug:
        print("=" * 60)
        print(f"📈 上证指数 最新数据（{last_date}）")
        print(f"   收盘价：{close:.2f}")
        print(f"   20日均线：{ma20:.2f}")
        print(f"   状态：{'✅ 站上20日线，正常放行选股' if close >= ma20 else '❌ 跌破20日线，关闭选股'}")
        print("=" * 60)

    return close >= ma20