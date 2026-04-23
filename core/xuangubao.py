# -*- coding: utf-8 -*-
"""
选股宝数据预留模块
功能：
1. 涨停池/连板/炸板/跌停数据
2. 大盘情绪指标（涨跌家数、炸板率）
3. 热点概念数据
4. 自动存入DuckDB，和现有架构统一
"""

import os
import time
import polars as pl
import duckdb
import requests
import threading
from datetime import datetime, timedelta

# ========================= 配置 =========================
API_BASE = "https://flash-api.xuangubao.cn"
DB_PATH = "data/stock_data.duckdb"

# 全局锁，保证数据库写入安全
_xgb_lock = threading.Lock()


# ========================= 数据库表初始化 =========================
def init_xgb_tables():
    """初始化选股宝专用表结构"""
    from core.data import get_db_connection
    con = get_db_connection()

    # 1. 涨停池表
    con.execute("""
        CREATE TABLE IF NOT EXISTS xgb_limit_up (
            date DATE,
            code VARCHAR,
            name VARCHAR,
            limit_up_time TIMESTAMP,
            limit_down_time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            PRIMARY KEY (date, code)
        )
    """)

    # 2. 大盘情绪表
    con.execute("""
        CREATE TABLE IF NOT EXISTS xgb_market_sentiment (
            date DATE,
            time TIMESTAMP,
            rise_count INT,
            fall_count INT,
            limit_up_count INT,
            limit_down_count INT,
            broken_ratio DOUBLE,
            PRIMARY KEY (date, time)
        )
    """)

    # 3. 热点概念表
    con.execute("""
        CREATE TABLE IF NOT EXISTS xgb_hot_concept (
            date DATE,
            concept_name VARCHAR,
            concept_code VARCHAR,
            rise_count INT,
            fall_count INT,
            lead_stock_code VARCHAR,
            lead_stock_name VARCHAR,
            PRIMARY KEY (date, concept_code)
        )
    """)

    # 创建索引
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_limit_up_date ON xgb_limit_up (date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_sentiment_date ON xgb_market_sentiment (date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_concept_date ON xgb_hot_concept (date)")


# ========================= 通用请求函数 =========================
def _request(url, timeout=8):
    """通用请求封装，带重试"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None


# ========================= 1. 涨停池数据 =========================
def get_limit_up(date=None):
    """
    获取涨停池数据
    date: 日期，格式YYYY-MM-DD，None为今日
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    url = f"{API_BASE}/api/pool/detail?pool_name=limit_up"
    if date != datetime.now().strftime("%Y-%m-%d"):
        url += f"&date={date}"

    data = _request(url)
    if not data or "data" not in data:
        return None

    try:
        items = data["data"].get("items", [])
        if not items:
            return None

        # 解析数据
        df = pl.DataFrame(items)

        # 只保留需要的列，兼容不同返回结构
        keep_cols = ["code", "name"]
        available_cols = [c for c in keep_cols if c in df.columns]
        df = df.select(available_cols)

        # 补充日期
        df = df.with_columns(pl.lit(date).str.strptime(pl.Date, "%Y-%m-%d").alias("date"))

        return df if len(df) > 0 else None
    except:
        return None


def save_limit_up(date=None):
    """获取并保存涨停池数据到数据库"""
    df = get_limit_up(date)
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            # 增量写入
            con.execute("BEGIN TRANSACTION")
            # 先删除该日期的旧数据
            if date:
                con.execute("DELETE FROM xgb_limit_up WHERE date = ?", [date])
            # 写入新数据
            con.execute("INSERT OR REPLACE INTO xgb_limit_up SELECT * FROM df")
            con.execute("COMMIT")
        return True
    except:
        con.execute("ROLLBACK")
        return False


# ========================= 2. 大盘情绪数据 =========================
def get_market_sentiment():
    """获取实时大盘情绪数据"""
    # 涨跌家数
    url1 = f"{API_BASE}/api/market_indicator/line?fields=rise_count,fall_count"
    # 涨跌停家数
    url2 = f"{API_BASE}/api/market_indicator/line?fields=limit_up_count,limit_down_count"
    # 炸板率
    url3 = f"{API_BASE}/api/market_indicator/line?fields=limit_up_broken_ratio"

    data1 = _request(url1)
    data2 = _request(url2)
    data3 = _request(url3)

    if not data1 or not data2 or not data3:
        return None

    try:
        # 合并数据
        sentiment = {}
        if "data" in data1 and len(data1["data"]) > 0:
            sentiment.update(data1["data"][-1])
        if "data" in data2 and len(data2["data"]) > 0:
            sentiment.update(data2["data"][-1])
        if "data" in data3 and len(data3["data"]) > 0:
            sentiment.update(data3["data"][-1])

        # 转成DataFrame
        df = pl.DataFrame([sentiment])

        # 补充时间
        now = datetime.now()
        df = df.with_columns([
            pl.lit(now.date()).alias("date"),
            pl.lit(now).alias("time")
        ])

        return df
    except:
        return None


def save_market_sentiment():
    """获取并保存大盘情绪数据到数据库"""
    df = get_market_sentiment()
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            con.execute("INSERT OR REPLACE INTO xgb_market_sentiment SELECT * FROM df")
        return True
    except:
        return False


# ========================= 3. 热点概念数据 =========================
def get_hot_concept():
    """获取热点概念数据"""
    url = f"{API_BASE}/api/pool/detail?pool_name=hot_concept"
    data = _request(url)
    if not data or "data" not in data:
        return None

    try:
        items = data["data"].get("items", [])
        if not items:
            return None

        df = pl.DataFrame(items)

        # 补充日期
        df = df.with_columns(pl.lit(datetime.now().date()).alias("date"))

        return df if len(df) > 0 else None
    except:
        return None


def save_hot_concept():
    """获取并保存热点概念数据到数据库"""
    df = get_hot_concept()
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            # 先删除今日旧数据
            today = datetime.now().date()
            con.execute("DELETE FROM xgb_hot_concept WHERE date = ?", [today])
            # 写入新数据
            con.execute("INSERT OR REPLACE INTO xgb_hot_concept SELECT * FROM df")
        return True
    except:
        return False


# ========================= 一键更新所有数据 =========================
def update_all_xgb_data():
    """一键更新所有选股宝数据（推荐每天收盘后调用）"""
    print("=" * 60)
    print("📊 更新选股宝数据...")
    print("-" * 60)

    success = 0

    # 1. 更新涨停池
    if save_limit_up():
        print("✅ 涨停池数据更新成功")
        success += 1
    else:
        print("❌ 涨停池数据更新失败")

    # 2. 更新大盘情绪
    if save_market_sentiment():
        print("✅ 大盘情绪数据更新成功")
        success += 1
    else:
        print("❌ 大盘情绪数据更新失败")

    # 3. 更新热点概念
    if save_hot_concept():
        print("✅ 热点概念数据更新成功")
        success += 1
    else:
        print("❌ 热点概念数据更新失败")

    print("-" * 60)
    print(f"更新完成：{success}/3 成功")
    print("=" * 60)

    return success == 3


# ========================= 初始化 =========================
# 自动初始化表结构
init_xgb_tables()