# ==============================
# 二买概率模型（观察池 + 突破触发版）
# ==============================

import numpy as np
import pandas as pd
import requests
import talib
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# ================= 数据获取 =================
def get_tencent_history(symbol):
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"
    try:
        data = requests.get(url, timeout=10).json()
        klines = data["data"][symbol]["day"]
        df = pd.DataFrame(klines, columns=["date","open","close","high","low","volume","_"])
        df = df[["date","open","close","high","low","volume"]]
        df = df.astype({"open":float,"close":float,"high":float,"low":float,"volume":float})
        return df
    except:
        return None

def get_stock_daily(code):
    symbol = ("sh"+code) if code.startswith("6") else ("sz"+code)
    return get_tencent_history(symbol)

# ================= 特征工程 =================
def compute_features(df):
    df = df.copy()

    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()

    # 趋势
    df['trend'] = df['ma20'] / df['ma60']

    # 回撤
    df['high20'] = df['high'].rolling(20).max()
    df['drawdown'] = (df['high20'] - df['close']) / df['high20']

    # 不破前低
    df['low10'] = df['low'].rolling(10).min()
    df['low20'] = df['low'].rolling(20).min()
    df['not_break'] = (df['low10'] > df['low20']).astype(int)

    # 量
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['vol_ratio'] = df['volume'] / df['vol_ma20']

    # MACD
    macd, macdsignal, _ = talib.MACD(df['close'])
    df['macd'] = macd - macdsignal

    return df.dropna()

# ================= 标签 =================
def add_label(df):
    df = df.copy()
    df['future_max'] = df['high'].rolling(5).max().shift(-5)
    df['ret'] = (df['future_max'] - df['close']) / df['close']
    df['label'] = (df['ret'] > 0.05).astype(int)
    return df.dropna()

# ================= 二买候选（不含突破） =================
def second_buy_candidate(df):
    last = df.iloc[-1]

    cond1 = last['trend'] > 0.98      # 放宽趋势
    cond2 = 0.02 < last['drawdown'] < 0.20  # 放宽回撤
    cond3 = last['not_break'] == 1
    cond4 = last['vol_ratio'] < 1.0   # 放宽缩量

    return cond1 and cond2 and cond3 and cond4

# ================= 模型 =================
FEATURES = ['trend','drawdown','not_break','vol_ratio','macd']

def train(df):
    X = df[FEATURES]
    y = df['label']

    X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2)

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8
    )

    model.fit(X_train,y_train)

    return model, scaler

# ================= 主程序 =================
if __name__ == "__main__":

    # 可以换成你自己的股票池
    STOCK_POOL = ["600519","000858","300750","002594","600036","601318","000333","002415"]

    data = []

    print("加载数据...")

    for code in STOCK_POOL:
        df = get_stock_daily(code)
        if df is None or len(df) < 100:
            continue

        df = compute_features(df)
        df = add_label(df)
        df['code'] = code
        data.append(df)

    full = pd.concat(data)

    print("训练模型...")
    model, scaler = train(full)

    print("\n=== 二买观察池（等待突破） ===")

    results = []

    for code in STOCK_POOL:
        df = get_stock_daily(code)
        if df is None or len(df) < 100:
            continue

        df = compute_features(df)

        # 只筛候选
        if not second_buy_candidate(df):
            continue

        last = df.iloc[-1]

        # 模型评分
        X = last[FEATURES].values.reshape(1, -1)
        X = scaler.transform(X)
        prob = model.predict_proba(X)[0][1]

        break_price = df['high'].iloc[-2]

        results.append((code, prob, break_price))

    # 排序
    results = sorted(results, key=lambda x: x[1], reverse=True)

    for r in results:
        code, prob, bp = r

        if prob >= 0.7:
            level = "A(强)"
        elif prob >= 0.6:
            level = "B(中)"
        else:
            level = "C(弱)"

        print(f"{code} | 概率:{prob:.2f} | 等级:{level} | 突破价:{bp:.2f}")

    if len(results) == 0:
        print("（当前无符合二买结构的标的）")