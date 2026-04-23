# -*- coding: utf-8 -*-
"""
二买概率评分模型（新浪财经接口版）
详细注释版
"""

import requests
import pandas as pd
import numpy as np
import talib  # 技术指标库，依赖 TA-Lib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import xgboost as xgb


# ==================== 一、核心数据获取（新浪实现） ====================
def get_stock_daily_sina(code, start_date="2020-01-01", end_date=None):
    """
    从**新浪财经**获取股票日K线数据 (普通复权,非后复权)。
    """
    # 1. 构造基础请求URL (不加后复权参数)
    if code.startswith('6'):
        symbol = f"sh{code}"
    else:
        symbol = f"sz{code}"

    fmt_url = (f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
               f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=240")

    # 2. 发送请求 (增加Header)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://finance.sina.com.cn"
    }
    try:
        resp = requests.get(fmt_url, headers=headers, timeout=10)
        data = resp.json()  # 新浪返回直接是 JSON 数组

        if not data or len(data) < 2:
            return None

        # 3. 转换为 Pandas DataFrame 并计算复权因子 (模拟前复权)
        #    由于新浪不提供后复权，我们通过回放比例因子模拟前复权
        df = pd.DataFrame(data)
        col_map = {
            'day': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }
        df = df.rename(columns=col_map)

        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        # 4. 计算复权因子 (使用收盘价回滚)
        #    注意：这是模拟前复权，不是后复权
        base = df['close'].iloc[-1]
        df['adj_factor'] = 1.0
        # 从后向前计算复权因子（除权除息导致的自然价格断层）
        for i in range(len(df) - 2, -1, -1):
            # 如果前后价格比例极不合理(> 1.5倍或 < 0.6倍)，认定为除权日
            if df['close'].iloc[i] / df['close'].iloc[i + 1] > 1.5 or df['close'].iloc[i] / df['close'].iloc[
                i + 1] < 0.6:
                # 设定复权因子反转
                df.loc[i, 'adj_factor'] = df.loc[i + 1, 'adj_factor'] * (df['close'].iloc[i + 1] / df['close'].iloc[i])
            else:
                df.loc[i, 'adj_factor'] = df.loc[i + 1, 'adj_factor']

        # 应用前复权
        df['open'] = df['open'] * df['adj_factor']
        df['high'] = df['high'] * df['adj_factor']
        df['low'] = df['low'] * df['adj_factor']
        df['close'] = df['close'] * df['adj_factor']

        return df[['date', 'open', 'high', 'low', 'close', 'volume']]

    except Exception as e:
        print(f"❌ 新浪获取 {code} 失败: {e}")
        return None


# ==================== 二、技术指标特征工程 ====================
def compute_features(df):
    """
    计算模型所需的技术指标特征。
    """
    df = df.copy()

    # 移动平均线和乖离率
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['close_ma20_ratio'] = (df['close'] - df['ma20']) / df['ma20']
    df['close_ma60_ratio'] = (df['close'] - df['ma60']) / df['ma60']
    df['ma20_gt_ma60'] = (df['ma20'] > df['ma60']).astype(int)

    # 创阶段新高
    df['high_20d_max'] = df['high'].rolling(20).max()
    df['is_20d_high'] = (df['high'] == df['high_20d_max']).astype(int)
    df['dist_from_20d_high'] = (df['high_20d_max'] - df['close']) / df['high_20d_max']
    df['drawdown_from_high'] = (df['close'] - df['high_20d_max']) / df['high_20d_max']

    # MACD 指标
    df['macd_d'], df['macd_dea'], df['macd_h'] = talib.MACD(
        df['close'], fastperiod=12, slowperiod=26, signalperiod=9
    )
    df['macd_diff_zero_dist'] = df['macd_d'] / (df['close'] / 100)

    # RSI 与布林带
    df['rsi_14'] = talib.RSI(df['close'], timeperiod=14)
    upper, middle, lower = talib.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2)
    df['boll_position'] = (df['close'] - lower) / (upper - lower)

    # 成交量分析
    df['vol_ma20'] = df['volume'].rolling(20).mean()
    df['vol_ratio'] = df['volume'] / df['vol_ma20']
    df['is_shrink'] = (df['vol_ratio'] < 0.7).astype(int)

    # 收益率特征
    df['ret_5d'] = df['close'].pct_change(5)
    df['ret_20d'] = df['close'].pct_change(20)

    # 删除含有空值的行 (确保所有指标计算完毕)
    df = df.dropna().reset_index(drop=True)
    return df


# ==================== 三、标签生成 ====================
def generate_labels(df, forward_days=5, min_gain=0.03):
    """
    生成二买有效性标签。
    标签 = 1: 未来5日涨幅>=3% 且 期间未创新低。
    """
    df = df.copy()
    df['future_return'] = (df['close'].shift(-forward_days) - df['close']) / df['close']
    future_low = df['low'].rolling(forward_days, min_periods=1).min().shift(-forward_days)
    df['future_break_low'] = future_low < df['low']
    df['label'] = ((df['future_return'] >= min_gain) & (~df['future_break_low'])).astype(int)
    return df


def mark_buy2_candidates(df):
    """
    标记二买候选样本: 用于数据平衡和提升训练效率。
    """
    df = df.copy()
    cond1 = abs(df['close_ma20_ratio']) <= 0.03  # 价格回踩20日线附近
    cond2 = df['is_shrink'] == 1  # 缩量条件
    cond3 = abs(df['macd_diff_zero_dist']) <= 0.5  # MACD 接近0轴
    df['is_candidate'] = (cond1 & cond2 & cond3).astype(int)
    return df


# 特征列表 (模型输入)
FEATURE_COLS = [
    'close_ma20_ratio', 'close_ma60_ratio', 'ma20_gt_ma60',
    'is_20d_high', 'dist_from_20d_high', 'drawdown_from_high',
    'macd_diff_zero_dist', 'rsi_14', 'boll_position',
    'vol_ratio', 'is_shrink', 'ret_5d', 'ret_20d'
]


# ==================== 四、训练模型 ====================
def train_model(data_df):
    """
    训练LightGBM概率模型，并使用xgboost输出二买概率评分
    """
    # 数据准备
    train_data = data_df[data_df['is_candidate'] == 1].copy()
    if len(train_data) < 100:
        print("候选样本过少，使用全部数据集训练")
        train_data = data_df.copy()

    X = train_data[FEATURE_COLS]
    y = train_data['label']

    # 分割测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # 归一化
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # XGBoost 二分类模型
    model = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='auc',
        use_label_encoder=False,
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42
    )
    model.fit(X_train_scaled, y_train, eval_set=[(X_test_scaled, y_test)], verbose=False)

    # 特征重要性排序
    feat_imp = pd.DataFrame({'feature': FEATURE_COLS, 'importance': model.feature_importances_})
    print("\n模型特征重要性 TOP5:")
    print(feat_imp.sort_values('importance', ascending=False).head())

    return model, scaler


def predict_prob(model, scaler, df_latest):
    """对最新数据进行概率预测"""
    if df_latest.empty:
        return None
    X_new = df_latest[FEATURE_COLS].copy()
    X_new_scaled = scaler.transform(X_new)
    return model.predict_proba(X_new_scaled)[:, 1]


# ==================== 五、主函数 ====================
if __name__ == "__main__":
    # 测试股票池 (沪深日线数据可用品种)
    stock_pool = [
        "000858",  # 五粮液
        "600036",  # 招商银行
        "600519"  # 贵州茅台
    ]

    all_data = []
    for code in stock_pool:
        print(f"获取 {code} 数据 (来源: 新浪接口)...")
        df = get_stock_daily_sina(code, start_date="2020-01-01")
        if df is not None and len(df) > 200:
            df['code'] = code
            all_data.append(df)
            print(f"  成功获取 {len(df)} 条数据")
        else:
            print(f"  ❌ {code} 数据不足")

    if not all_data:
        raise SystemExit("没有获取到任何数据，程序终止。")

    full_df = pd.concat(all_data, ignore_index=True)
    full_df = compute_features(full_df)
    full_df = mark_buy2_candidates(full_df)
    full_df = generate_labels(full_df)
    full_df = full_df.dropna(subset=['label'])

    print(f"总有效样本: {len(full_df)} | 正样本比例: {full_df['label'].mean():.3f}")

    # 训练模型
    model, scaler = train_model(full_df)

    # 对最新数据进行评分
    latest = full_df.groupby('code').last().reset_index()
    latest['prob'] = predict_prob(model, scaler, latest)

    print("\n========== 最新二买概率评分 ==========")
    for idx, row in latest.iterrows():
        prob = row['prob']
        grade = "A(强)" if prob >= 0.7 else "B(标准)" if prob >= 0.6 else "C(弱)" if prob >= 0.5 else "D(无效)"
        print(f"{row['code']} : 概率 {prob:.3f} | 等级 {grade}")