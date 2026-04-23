import os
import joblib
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import KFold
from sklearn.metrics import roc_auc_score

MODEL_PATH = "model/general_model.pkl"
os.makedirs("model", exist_ok=True)

from core.feature import FEATURES


def add_label(df):
    df = df.copy()
    df['future_high'] = df['high'].rolling(20).max().shift(-20)
    df['future_low'] = df['low'].rolling(20).min().shift(-20)
    df['label'] = ((df['future_high'] > df['high']) &
                   (df['future_low'] > df['low'])).astype(int)
    return df.dropna()


def train_general_model(all_stock_data):
    print("开始训练通用模型...")
    all_data = []
    for code, df in all_stock_data:
        if df is None or len(df) < 200:
            continue
        df_label = add_label(df)
        if df_label.empty:
            continue
        df_label['code'] = code
        all_data.append(df_label)

    if not all_data:
        print("没有足够的数据训练模型")
        return None

    full_df = pd.concat(all_data, ignore_index=True)
    X = full_df[FEATURES]
    y = full_df['label']

    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    auc_scores = []

    for train_idx, test_idx in kf.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            random_state=42,
            use_label_encoder=False,
            eval_metric='logloss'
        )
        model.fit(X_train, y_train)

        y_pred = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred)
        auc_scores.append(auc)

    print(f"交叉验证 AUC: {auc_scores}")
    print(f"平均 AUC: {sum(auc_scores) / len(auc_scores):.4f}")

    final_model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    final_model.fit(X, y)

    joblib.dump(final_model, MODEL_PATH)
    print(f"通用模型已保存至: {MODEL_PATH}")
    return final_model


def load_model():
    if os.path.exists(MODEL_PATH):
        try:
            return joblib.load(MODEL_PATH)
        except:
            return None
    return None


def predict(model, X):
    """【优化】支持批量预测，X可以是单个样本或矩阵"""
    if model is None:
        return 0.0
    return model.predict_proba(X)[:, 1]