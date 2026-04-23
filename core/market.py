import pandas as pd
from core.data import get_data

def market_filter():
    # 上证指数
    df = get_data("000001")  # 上证指数

    if df is None or len(df) < 60:
        return False

    df['ma20'] = df['close'].rolling(20).mean()

    last = df.iloc[-1]

    # 条件：指数站上MA20
    if last['close'] < last['ma20']:
        return False

    return True