import pandas as pd
from core.data import get_data


def market_filter(debug=True):
    """
    大盘环境过滤：上证指数站上20日线才允许选股
    【优化】只在程序启动时调用一次，避免重复计算
    """
    df = get_data("sh000001")

    if df is None or len(df) < 60:
        df = get_data("sz399001")

    if df is None or len(df) < 60:
        if debug:
            print("⚠️  大盘数据获取失败，默认放行选股")
        return True

    df['ma20'] = df['close'].rolling(20).mean()
    last = df.iloc[-1]
    close = last['close']
    ma20 = last['ma20']

    if debug:
        print("=" * 60)
        print(f"📈 上证指数 最新数据（{last['date'].strftime('%Y-%m-%d')}）")
        print(f"   收盘价：{close:.2f}")
        print(f"   20日均线：{ma20:.2f}")
        print(f"   状态：{'✅ 站上20日线，正常放行选股' if close >= ma20 else '❌ 跌破20日线，关闭选股'}")
        print("=" * 60)

    return close >= ma20