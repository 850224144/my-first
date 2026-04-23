import pandas as pd

def compute_features(df):
    df = df.copy()

    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()

    df['trend'] = df['ma20'] / df['ma60']

    df['high20'] = df['high'].rolling(20).max()
    df['drawdown'] = (df['high20'] - df['close']) / df['high20']

    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['vol_ratio'] = df['volume'] / df['vol_ma20']

    df['low10'] = df['low'].rolling(10).min()
    df['low20'] = df['low'].rolling(20).min()
    df['not_break'] = (df['low10'] > df['low20']).astype(int)

    return df.dropna()