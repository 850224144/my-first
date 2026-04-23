def detect_uptrend(df):
    try:
        # Polars和Pandas的iloc类似，用.slice()
        window = df.slice(-40, 30)  # 最近40天~最近10天
        if len(window) < 20:
            return False
        close_start = window.select(pl.col("close").first()).item()
        close_end = window.select(pl.col("close").last()).item()
        rise = (close_end - close_start) / close_start
        return 0.15 < rise < 0.4
    except:
        return False

def detect_pullback(df):
    try:
        window = df.slice(-15)
        if len(window) < 10:
            return False
        high = window.select(pl.max("high")).item()
        low = window.select(pl.min("low")).item()
        drawdown = (high - low) / high
        return 0.05 < drawdown < 0.25
    except:
        return False

def detect_stabilization(df):
    try:
        window = df.slice(-5)
        if len(window) < 5:
            return False
        high = window.select(pl.max("high")).item()
        low = window.select(pl.min("low")).item()
        vol = (high - low) / low
        vol_mean_window = window.select(pl.mean("volume")).item()
        vol_mean_all = df.select(pl.col("volume").rolling_mean(window_size=20).last()).item()
        vol_shrink = vol_mean_window < vol_mean_all
        return vol < 0.05 and vol_shrink
    except:
        return False

def is_second_buy(df):
    return detect_uptrend(df) and detect_pullback(df) and detect_stabilization(df)

def volume_confirm(df):
    try:
        vol_last = df.select(pl.col("volume").last()).item()
        vol_ma20 = df.select(pl.col("volume").rolling_mean(window_size=20).last()).item()
        return vol_last > vol_ma20 * 1.5
    except:
        return False

def breakout_price(df):
    try:
        return df.select(pl.col("high").slice(-2, 1).last()).item()
    except:
        return df.select(pl.col("close").last()).item()

def trade_plan(df, prob):
    bp = breakout_price(df)
    buy = bp * 1.01
    try:
        low_10 = df.select(pl.col("low").rolling_min(window_size=10).last()).item()
    except:
        low_10 = buy * 0.95
    stop = min(low_10, buy * 0.95)
    risk = buy - stop
    pos = min(0.2, 0.01 / (risk / buy))
    return {
        "buy": round(buy, 2),
        "stop": round(stop, 2),
        "position": round(pos, 2),
        "prob": round(prob, 2)
    }