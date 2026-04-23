# -*- coding: utf-8 -*-

import os
import pandas as pd
import requests
from functools import lru_cache

CACHE_DIR = "data/daily"
CACHE_EXT = ".parquet"          # 使用 Parquet 格式

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36")
}


def get_exchange_prefix(code):
    """
        根据股票代码判断交易所前缀：sh(上海) / sz(深圳) / bj(北交所)
        """
    # 北交所 (现行代码：920)
    if code.startswith('920'):
        return "bj"
    # 上海 (主板 60xxxx, 科创板 688xxx)
    elif code.startswith(('6', '5')):
        return "sh"
    # 深圳 (主板 00xxxx, 中小板 002xxx, 创业板 30xxxx)
    elif code.startswith(('0', '2', '3')):
        return "sz"
    else:
        # 兜底
        return "sz"


# =========================
# 腾讯数据（主）
# =========================
def get_tencent(code):
    prefix = get_exchange_prefix(code)
    symbol = prefix + code
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        r.raise_for_status()
        data = r.json()["data"][symbol]["day"]
        df = pd.DataFrame(data, columns=["date","open","close","high","low","volume","x"])
        df = df[["date","open","close","high","low","volume"]]
        df.iloc[:, 1:] = df.iloc[:, 1:].astype(float)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except:
        return None


# =========================
# 新浪备用（失败兜底）
# =========================
def get_sina(code):
    try:
        prefix = get_exchange_prefix(code)
        symbol = prefix + code
        url = f"https://finance.sina.com.cn/realstock/company/{symbol}/hisdata/klc_kl.js"
        r = requests.get(url, headers=HEADERS, timeout=5)
        if len(r.text) < 100:
            return None
        # 这里保留接口结构，暂不实际解析
        return None
    except:
        return None


# =========================
# 缓存路径
# =========================
def cache_path(code):
    return os.path.join(CACHE_DIR, f"{code}{CACHE_EXT}")


# =========================
# 读取本地缓存（Parquet）
# =========================
def load_cache(code):
    path = cache_path(code)
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None


# =========================
# 保存缓存（Parquet）
# =========================
def save_cache(code, df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.to_parquet(cache_path(code), index=False)


# =========================
# 判断是否最新
# =========================
def is_latest(old_df, new_df):
    if old_df is None or new_df is None or old_df.empty or new_df.empty:
        return False
    if "date" not in old_df.columns or "date" not in new_df.columns:
        return False
    try:
        return new_df["date"].iloc[-1] == old_df["date"].iloc[-1]
    except IndexError:
        return False


# =========================
# 增量合并（只追加新数据）
# =========================
def merge_incremental(old_df, new_df):
    """将 new_df 中日期大于 old_df 最大日期的行追加到 old_df 末尾"""
    max_old_date = old_df["date"].max()
    new_data = new_df[new_df["date"] > max_old_date]
    if new_data.empty:
        return old_df
    merged = pd.concat([old_df, new_data], ignore_index=True)
    return merged


# =========================
# 核心数据获取（加内存缓存）
# =========================
@lru_cache(maxsize=512)
def get_data(code):
    """
    返回该股票的历史日线DataFrame（按日期升序）
    - 优先从本地 Parquet 读取
    - 再尝试腾讯接口、新浪接口
    - 自动合并增量并写回 Parquet
    """
    # 1. 读本地缓存
    local = load_cache(code)

    # 2. 拉腾讯数据
    new = get_tencent(code)

    # 3. 腾讯失败 → 新浪
    if new is None:
        new = get_sina(code)

    # 4. 都失败 → 返回本地数据（如果有）
    if new is None:
        return local

    # 5. 本地有数据，判断是否需要更新
    if local is not None and not local.empty:
        if is_latest(local, new):
            return local
        # 增量合并
        merged = merge_incremental(local, new)
        if merged is not local:      # 确实有新数据才写磁盘
            save_cache(code, merged)
        return merged

    # 6. 首次写入（本地无数据）
    save_cache(code, new)
    return new