# -*- coding: utf-8 -*-
"""
core/model.py

模型层：只排序，不决定买卖。
规则系统先筛出候选股，模型只给候选股排序。
"""

from __future__ import annotations

import os
from typing import List, Dict, Any, Optional

import joblib
import numpy as np
import polars as pl
import xgboost as xgb
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

from core.feature import FEATURES, compute_features

MODEL_PATH = "model/general_model.pkl"
os.makedirs("model", exist_ok=True)


def add_label(df: pl.DataFrame) -> pl.DataFrame:
    """
    简单标签：未来20日最大涨幅 >= 10%，且未来20日最大回撤不超过 8%。
    只用于排序模型，不用于买卖裁决。
    """
    if df is None or len(df) < 160:
        return pl.DataFrame()
    feat = compute_features(df)
    if feat.is_empty():
        return pl.DataFrame()

    out = feat.with_columns(
        [
            pl.col("high").rolling_max(20).shift(-20).alias("future_high20"),
            pl.col("low").rolling_min(20).shift(-20).alias("future_low20"),
        ]
    ).with_columns(
        [
            ((pl.col("future_high20") / pl.col("close") - 1) >= 0.10).alias("future_up_ok"),
            ((pl.col("future_low20") / pl.col("close") - 1) >= -0.08).alias("future_down_ok"),
        ]
    ).with_columns(
        (pl.col("future_up_ok") & pl.col("future_down_ok")).cast(pl.Int32).alias("label")
    )
    return out.drop_nulls(subset=FEATURES + ["label"])


def train_general_model(all_stock_data: List[tuple[str, pl.DataFrame]]) -> Optional[Any]:
    """训练通用排序模型。时间序列切分，避免 shuffle 造成未来泄露。"""
    print("开始训练通用排序模型...")
    parts = []
    for code, df in all_stock_data:
        labeled = add_label(df)
        if labeled.is_empty():
            continue
        parts.append(labeled.with_columns(pl.lit(code).alias("code")))

    if not parts:
        print("没有足够数据训练模型")
        return None

    full = pl.concat(parts, how="diagonal_relaxed").drop_nulls(subset=FEATURES + ["label"])
    X = full.select(FEATURES).to_numpy()
    y = full.select("label").to_numpy().ravel()

    if len(np.unique(y)) < 2:
        print("标签只有单一类别，无法训练")
        return None

    tscv = TimeSeriesSplit(n_splits=5)
    auc_scores = []
    for train_idx, test_idx in tscv.split(X):
        model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            eval_metric="logloss",
        )
        model.fit(X[train_idx], y[train_idx])
        pred = model.predict_proba(X[test_idx])[:, 1]
        auc_scores.append(roc_auc_score(y[test_idx], pred))

    print(f"TimeSeriesSplit AUC: {[round(x, 4) for x in auc_scores]}")
    print(f"平均 AUC: {np.mean(auc_scores):.4f}")

    final = xgb.XGBClassifier(
        n_estimators=240,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="logloss",
    )
    final.fit(X, y)
    joblib.dump(final, MODEL_PATH)
    print(f"模型已保存：{MODEL_PATH}")
    return final


def load_model() -> Optional[Any]:
    if not os.path.exists(MODEL_PATH):
        return None
    try:
        return joblib.load(MODEL_PATH)
    except Exception:
        return None


def predict(model: Any, X: pl.DataFrame | np.ndarray) -> np.ndarray:
    """批量预测，返回概率数组。"""
    if model is None:
        return np.array([])
    if isinstance(X, pl.DataFrame):
        arr = X.select(FEATURES).drop_nulls().to_numpy()
    else:
        arr = X
    if arr is None or len(arr) == 0:
        return np.array([])
    return model.predict_proba(arr)[:, 1]


def predict_latest_prob(model: Any, df_feat: pl.DataFrame) -> float:
    """只取最新一行特征，这是修复点。"""
    if model is None or df_feat is None or df_feat.is_empty():
        return 0.0
    latest = df_feat.tail(1)
    try:
        prob = predict(model, latest)
        return float(prob[0]) if len(prob) else 0.0
    except Exception:
        return 0.0


def rank_candidates_by_model(candidates: List[Dict[str, Any]], model: Optional[Any] = None) -> List[Dict[str, Any]]:
    """模型只排序，不过滤。没有模型则按规则分排序。"""
    if not candidates:
        return []
    if model is None:
        return sorted(candidates, key=lambda x: x.get("score", 0), reverse=True)

    for item in candidates:
        df_feat = item.get("df_feat")
        prob = predict_latest_prob(model, df_feat) if isinstance(df_feat, pl.DataFrame) else 0.0
        item["model_prob"] = round(prob, 4)

    return sorted(candidates, key=lambda x: (x.get("model_prob", 0), x.get("score", 0)), reverse=True)
