# -*- coding: utf-8 -*-

import os
import pandas as pd
import requests

CACHE_DIR = "data/daily"


# =========================
# 腾讯数据（主）
# =========================
def get_tencent(code):

    symbol = "sh" + code if code.startswith("6") else "sz" + code

    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"

    try:
        r = requests.get(url, timeout=5).json()
        data = r["data"][symbol]["day"]

        df = pd.DataFrame(data, columns=[
            "date","open","close","high","low","volume","x"
        ])

        df = df[["date","open","close","high","low","volume"]]
        df.iloc[:, 1:] = df.iloc[:, 1:].astype(float)

        return df

    except:
        return None


# =========================
# 新浪备用（失败兜底）
# =========================
def get_sina(code):

    try:
        symbol = "sh" + code if code.startswith("6") else "sz" + code

        url = f"https://finance.sina.com.cn/realstock/company/{symbol}/hisdata/klc_kl.js"

        r = requests.get(url, timeout=5)

        if len(r.text) < 100:
            return None

        return None  # 这里保留接口结构，避免系统崩

    except:
        return None


# =========================
# 缓存路径
# =========================
def cache_path(code):
    return f"{CACHE_DIR}/{code}.csv"


# =========================
# 读取本地缓存
# =========================
def load_cache(code):

    path = cache_path(code)

    if os.path.exists(path):
        return pd.read_csv(path)

    return None


# =========================
# 保存缓存
# =========================
def save_cache(code, df):

    os.makedirs(CACHE_DIR, exist_ok=True)

    df.to_csv(cache_path(code), index=False)


# =========================
# 判断是否最新
# =========================
def is_latest(old_df, new_df):

    if old_df is None:
        return False

    return new_df["date"].iloc[-1] == old_df["date"].iloc[-1]


# =========================
# 增量更新核心逻辑
# =========================
def get_data(code):

    # 1️⃣ 先读本地
    local = load_cache(code)

    # 2️⃣ 拉腾讯
    new = get_tencent(code)

    # 3️⃣ 腾讯失败 → 新浪
    if new is None:
        new = get_sina(code)

    # 4️⃣ 都失败 → 返回本地
    if new is None:
        return local

    # 5️⃣ 判断是否更新
    if local is not None:

        if is_latest(local, new):
            return local

        # 合并增量
        merged = pd.concat([local, new]).drop_duplicates("date")

        save_cache(code, merged)

        return merged

    # 6️⃣ 首次写入
    save_cache(code, new)

    return new