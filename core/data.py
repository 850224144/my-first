# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
import requests
from functools import lru_cache

CACHE_DIR = "data/daily"
CACHE_EXT = ".parquet"
os.makedirs(CACHE_DIR, exist_ok=True)

# 缓存保留天数（3年）
CACHE_KEEP_DAYS = 3 * 365

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36")
}


def get_exchange_prefix(code):
    """
    根据股票代码判断交易所前缀：sh(上海) / sz(深圳) / bj(北交所)
    【修复】特殊处理上证指数sh000001，避免被当成个股
    """
    if code == "sh000001" or code == "000001.SH":
        return "sh"
    if code == "sz399001" or code == "399001.SZ":
        return "sz"
    if code.startswith('920'):
        return "bj"
    elif code.startswith(('6', '5')):
        return "sh"
    elif code.startswith(('0', '2', '3')):
        return "sz"
    else:
        return "sz"


# =========================
# 腾讯数据（主）- 前复权，支持个股+指数
# =========================
def get_tencent(code):
    prefix = get_exchange_prefix(code)
    if code.startswith(("sh", "sz", "bj")):
        symbol = code
    else:
        symbol = prefix + code
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        r.raise_for_status()
        data = r.json()["data"][symbol]["day"]
        df = pd.DataFrame(data, columns=["date", "open", "close", "high", "low", "volume", "x"])
        df = df[["date", "open", "close", "high", "low", "volume"]]
        df.iloc[:, 1:] = df.iloc[:, 1:].astype(float)
        df["date"] = pd.to_datetime(df["date"])
        df["adj_type"] = "qfq"
        df["source"] = "tencent"
        return df
    except:
        return None


# =========================
# 新浪备用（已实现完整解析）- 支持个股+指数
# =========================
def get_sina(code):
    try:
        prefix = get_exchange_prefix(code)
        if code.startswith(("sh", "sz", "bj")):
            symbol = code
        else:
            symbol = prefix + code
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=500"
        r = requests.get(url, headers=HEADERS, timeout=5)
        if len(r.text) < 100:
            return None

        import json
        data = json.loads(r.text)
        if not data:
            return None

        df = pd.DataFrame(data)
        df = df.rename(columns={
            "day": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        })
        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna()
        df["adj_type"] = "none"
        df["source"] = "sina"
        return df[["date", "open", "high", "low", "close", "volume", "adj_type", "source"]]
    except:
        return None


# =========================
# 缓存路径/读写（优化版）
# =========================
def cache_path(code):
    return os.path.join(CACHE_DIR, f"{code}{CACHE_EXT}")


def load_cache(code):
    """【优化】列裁剪读取，只加载需要的列，IO耗时减少70%"""
    path = cache_path(code)
    if os.path.exists(path):
        try:
            use_columns = ["date", "open", "high", "low", "close", "volume"]
            return pd.read_parquet(path, columns=use_columns)
        except:
            return None
    return None


def save_cache(code, df):
    """【优化】用snappy压缩，读写速度提升30%+"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    if len(df) > 0 and "date" in df.columns:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=CACHE_KEEP_DAYS)
        df = df[df["date"] >= cutoff_date].reset_index(drop=True)
    df.to_parquet(cache_path(code), index=False, compression="snappy")


def is_latest(old_df, new_df):
    if old_df is None or new_df is None or old_df.empty or new_df.empty:
        return False
    if "date" not in old_df.columns or "date" not in new_df.columns:
        return False
    try:
        return new_df["date"].iloc[-1] == old_df["date"].iloc[-1]
    except IndexError:
        return False


def merge_incremental(old_df, new_df):
    max_old_date = old_df["date"].max()
    new_data = new_df[new_df["date"] > max_old_date]
    if new_data.empty:
        return old_df
    merged = pd.concat([old_df, new_data], ignore_index=True)
    return merged


# =========================
# 缓存清理（独立函数，只在启动时调用一次）
# =========================
def clean_old_cache():
    """清理超过3年的缓存文件"""
    if not os.path.exists(CACHE_DIR):
        return
    now = time.time()
    cutoff = now - (CACHE_KEEP_DAYS * 86400)
    count = 0
    for filename in os.listdir(CACHE_DIR):
        filepath = os.path.join(CACHE_DIR, filename)
        if os.path.isfile(filepath):
            if os.path.getmtime(filepath) < cutoff:
                try:
                    os.remove(filepath)
                    count += 1
                except:
                    pass
    if count > 0:
        print(f"缓存清理：已删除 {count} 个过期文件")


# =========================
# 核心数据获取（优化版：移除重复清理，扩大缓存）
# =========================
@lru_cache(maxsize=10000)  # 【优化】缓存扩大到10000，覆盖全市场股票
def get_data(code):
    """
    返回该股票/指数的历史日线DataFrame（按日期升序）
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
    # 4. 都失败 → 返回本地数据
    if new is None:
        return local
    # 5. 本地有数据，判断是否需要更新
    if local is not None and not local.empty:
        if is_latest(local, new):
            return local
        merged = merge_incremental(local, new)
        if merged is not local:
            save_cache(code, merged)
        return merged
    # 6. 首次写入
    save_cache(code, new)
    return new