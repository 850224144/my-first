def detect_uptrend(df):
    window = df.iloc[-40:-10]
    if len(window) < 20:
        return False
    rise = (window['close'].iloc[-1] - window['close'].iloc[0]) / window['close'].iloc[0]
    return 0.15 < rise < 0.4

def detect_pullback(df):
    window = df.iloc[-15:]
    high = window['high'].max()
    low = window['low'].min()
    drawdown = (high - low) / high
    return 0.05 < drawdown < 0.25

def detect_stabilization(df):
    window = df.iloc[-5:]
    vol = (window['high'].max() - window['low'].min()) / window['low'].min()
    vol_shrink = window['volume'].mean() < df['volume'].rolling(20).mean().iloc[-1]
    return vol < 0.05 and vol_shrink

def is_second_buy(df):
    return detect_uptrend(df) and detect_pullback(df) and detect_stabilization(df)

def volume_confirm(df):
    return df['volume'].iloc[-1] > df['volume'].rolling(20).mean().iloc[-1] * 1.5

def breakout_price(df):
    return df['high'].iloc[-2]

def trade_plan(df, prob):
    bp = breakout_price(df)
    buy = bp * 1.01
    stop = min(df['low'].iloc[-10:].min(), buy * 0.95)

    risk = buy - stop
    pos = min(0.2, 0.01 / (risk / buy))  # 最大20%

    return {
        "buy": round(buy,2),
        "stop": round(stop,2),
        "position": round(pos,2),
        "prob": round(prob,2)
    }