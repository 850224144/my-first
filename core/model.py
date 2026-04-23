import pandas as pd
import xgboost as xgb
from sklearn.calibration import CalibratedClassifierCV

FEATURES = ['trend','drawdown','vol_ratio','not_break']

def add_label(df):
    df = df.copy()
    df['future_high'] = df['high'].rolling(20).max().shift(-20)
    df['future_low'] = df['low'].rolling(20).min().shift(-20)

    df['label'] = ((df['future_high'] > df['high']) &
                   (df['future_low'] > df['low'])).astype(int)

    return df.dropna()

def train(df):
    df = add_label(df)

    split = int(len(df) * 0.8)
    train_df = df.iloc[:split]
    test_df = df.iloc[split:]

    X_train = train_df[FEATURES]
    y_train = train_df['label']

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05
    )

    model.fit(X_train, y_train)

    calibrated = CalibratedClassifierCV(model, method='isotonic')
    calibrated.fit(X_train, y_train)

    return calibrated

def predict(model, df):
    X = df[FEATURES].iloc[-1:].values
    return model.predict_proba(X)[0][1]