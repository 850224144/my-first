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
from typing import Optional, List

# ========================= 配置 =========================
API_BASE = "https://flash-api.xuangubao.cn"
DB_PATH = "data/stock_data.duckdb"
# 重试次数配置
RETRY_TIMES = 3
RETRY_INTERVAL = 1  # 秒

# 全局锁，保证数据库写入安全
_xgb_lock = threading.Lock()

# 选股宝池类型映射（扩展连板/炸板/跌停）
POOL_TYPES = {
    "limit_up": "涨停池",
    "limit_down": "跌停池",
    "continuous_board": "连板池",
    "broken_board": "炸板池"
}


# ========================= 数据库表初始化 =========================
def init_xgb_tables():
    """初始化选股宝专用表结构（扩展连板/炸板/跌停表）"""
    from core.data import get_db_connection
    con = get_db_connection()

    # 1. 涨停/跌停/连板/炸板 统一池表（整合同类数据，减少表数量）
    con.execute("""
        CREATE TABLE IF NOT EXISTS xgb_stock_pool (
            date DATE,
            pool_type VARCHAR,  -- limit_up/limit_down/continuous_board/broken_board
            code VARCHAR,
            name VARCHAR,
            limit_up_time TIMESTAMP,
            limit_down_time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            continuous_count INT,  -- 连板数（仅连板池有效）
            broken_time TIMESTAMP,  -- 炸板时间（仅炸板池有效）
            PRIMARY KEY (date, pool_type, code)
        )
    """)

    # 2. 大盘情绪表（保持原有）
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

    # 3. 热点概念表（保持原有）
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

    # 创建索引（优化查询）
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_pool_date_type ON xgb_stock_pool (date, pool_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_sentiment_date ON xgb_market_sentiment (date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_xgb_concept_date ON xgb_hot_concept (date)")

    con.close()


# ========================= 通用请求函数（增强重试） =========================
def _request(url: str, timeout: int = 8) -> Optional[dict]:
    """通用请求封装，带重试机制"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://xuangubao.cn",
        "Accept": "application/json, text/plain, */*"
    }

    for _ in range(RETRY_TIMES):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()  # 抛出HTTP错误
            return r.json()
        except (requests.exceptions.RequestException, ValueError):
            time.sleep(RETRY_INTERVAL)
    return None


# ========================= 扩展：通用池数据获取函数 =========================
def get_stock_pool(pool_type: str, date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """
    获取通用股票池数据（支持涨停/跌停/连板/炸板）
    :param pool_type: 池类型（limit_up/limit_down/continuous_board/broken_board）
    :param date: 日期，格式YYYY-MM-DD，None为今日
    :return: Polars DataFrame
    """
    if pool_type not in POOL_TYPES:
        raise ValueError(f"不支持的池类型：{pool_type}，可选：{list(POOL_TYPES.keys())}")

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    # 构造请求URL
    url = f"{API_BASE}/api/pool/detail?pool_name={pool_type}"
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

        # 基础字段过滤（兼容不同池的返回结构）
        base_cols = ["code", "name", "open", "high", "low", "close"]
        available_cols = [c for c in base_cols if c in df.columns]
        df = df.select(available_cols)

        # 补充特有字段
        if pool_type == "continuous_board":
            # 连板数字段
            df = df.with_columns(
                pl.col("continuous_count").cast(pl.Int32).fill_null(0).alias("continuous_count")
            ) if "continuous_count" in df.columns else df.with_columns(pl.lit(0).alias("continuous_count"))
        elif pool_type == "broken_board":
            # 炸板时间字段
            df = df.with_columns(
                pl.col("broken_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").fill_null(None).alias(
                    "broken_time")
            ) if "broken_time" in df.columns else df.with_columns(pl.lit(None).alias("broken_time"))
        else:
            # 涨跌停时间（涨停/跌停池）
            df = df.with_columns([
                pl.col("limit_up_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").fill_null(None).alias(
                    "limit_up_time"),
                pl.col("limit_down_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").fill_null(None).alias(
                    "limit_down_time")
            ]) if "limit_up_time" in df.columns else df.with_columns([
                pl.lit(None).alias("limit_up_time"),
                pl.lit(None).alias("limit_down_time")
            ])

        # 补充公共字段
        df = df.with_columns([
            pl.lit(date).str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
            pl.lit(pool_type).alias("pool_type")
        ])

        # 填充缺失字段（保证表结构一致）
        all_required_cols = [
            "date", "pool_type", "code", "name", "limit_up_time", "limit_down_time",
            "open", "high", "low", "close", "continuous_count", "broken_time"
        ]
        for col in all_required_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        return df.select(all_required_cols) if len(df) > 0 else None
    except Exception as e:
        print(f"解析{POOL_TYPES[pool_type]}数据失败：{e}")
        return None


def save_stock_pool(pool_type: str, date: Optional[str] = None) -> bool:
    """
    保存股票池数据到数据库
    :param pool_type: 池类型（limit_up/limit_down/continuous_board/broken_board）
    :param date: 日期，格式YYYY-MM-DD，None为今日
    :return: 是否成功
    """
    df = get_stock_pool(pool_type, date)
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            con.execute("BEGIN TRANSACTION")
            # 先删除该日期该类型的旧数据
            delete_date = date if date else datetime.now().strftime("%Y-%m-%d")
            con.execute(
                "DELETE FROM xgb_stock_pool WHERE date = ? AND pool_type = ?",
                [delete_date, pool_type]
            )
            # 写入新数据
            con.execute("INSERT OR REPLACE INTO xgb_stock_pool SELECT * FROM df")
            con.execute("COMMIT")
        return True
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"保存{POOL_TYPES[pool_type]}数据失败：{e}")
        return False
    finally:
        con.close()


# ========================= 原有功能适配（兼容新表结构） =========================
def get_limit_up(date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """兼容原有接口：获取涨停池数据"""
    df = get_stock_pool("limit_up", date)
    return df.drop(["pool_type", "continuous_count", "broken_time"]) if df is not None else None


def save_limit_up(date: Optional[str] = None) -> bool:
    """兼容原有接口：保存涨停池数据"""
    return save_stock_pool("limit_up", date)


# ========================= 新增：跌停/连板/炸板 专用函数 =========================
def get_limit_down(date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """获取跌停池数据"""
    df = get_stock_pool("limit_down", date)
    return df.drop(["pool_type", "continuous_count", "broken_time"]) if df is not None else None


def save_limit_down(date: Optional[str] = None) -> bool:
    """保存跌停池数据"""
    return save_stock_pool("limit_down", date)


def get_continuous_board(date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """获取连板池数据"""
    df = get_stock_pool("continuous_board", date)
    return df.drop(["pool_type", "limit_up_time", "limit_down_time", "broken_time"]) if df is not None else None


def save_continuous_board(date: Optional[str] = None) -> bool:
    """保存连板池数据"""
    return save_stock_pool("continuous_board", date)


def get_broken_board(date: Optional[str] = None) -> Optional[pl.DataFrame]:
    """获取炸板池数据"""
    df = get_stock_pool("broken_board", date)
    return df.drop(["pool_type", "limit_up_time", "limit_down_time", "continuous_count"]) if df is not None else None


def save_broken_board(date: Optional[str] = None) -> bool:
    """保存炸板池数据"""
    return save_stock_pool("broken_board", date)


# ========================= 大盘情绪数据（保持原有，增强健壮性） =========================
def get_market_sentiment() -> Optional[pl.DataFrame]:
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

    if not all([data1, data2, data3]):
        print("大盘情绪数据请求不完整")
        return None

    try:
        # 合并最新数据
        sentiment = {}
        for data in [data1, data2, data3]:
            if "data" in data and len(data["data"]) > 0:
                sentiment.update(data["data"][-1])

        # 数据类型转换
        df = pl.DataFrame([sentiment]).with_columns([
            pl.col("rise_count").cast(pl.Int32).fill_null(0),
            pl.col("fall_count").cast(pl.Int32).fill_null(0),
            pl.col("limit_up_count").cast(pl.Int32).fill_null(0),
            pl.col("limit_down_count").cast(pl.Int32).fill_null(0),
            pl.col("limit_up_broken_ratio").cast(pl.Float64).fill_null(0.0).alias("broken_ratio")
        ])

        # 补充时间字段
        now = datetime.now()
        df = df.with_columns([
            pl.lit(now.date()).alias("date"),
            pl.lit(now).alias("time")
        ])

        # 只保留需要的列
        return df.select(
            ["date", "time", "rise_count", "fall_count", "limit_up_count", "limit_down_count", "broken_ratio"])
    except Exception as e:
        print(f"解析大盘情绪数据失败：{e}")
        return None


def save_market_sentiment() -> bool:
    """保存大盘情绪数据到数据库"""
    df = get_market_sentiment()
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            con.execute("INSERT OR REPLACE INTO xgb_market_sentiment SELECT * FROM df")
        return True
    except Exception as e:
        print(f"保存大盘情绪数据失败：{e}")
        return False
    finally:
        con.close()


# ========================= 热点概念数据（保持原有） =========================
def get_hot_concept() -> Optional[pl.DataFrame]:
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

        # 字段过滤和类型转换
        df = df.with_columns([
            pl.col("rise_count").cast(pl.Int32).fill_null(0),
            pl.col("fall_count").cast(pl.Int32).fill_null(0),
            pl.lit(datetime.now().date()).alias("date")
        ])

        # 保证表结构一致
        required_cols = ["date", "concept_name", "concept_code", "rise_count", "fall_count", "lead_stock_code",
                         "lead_stock_name"]
        for col in required_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        return df.select(required_cols) if len(df) > 0 else None
    except Exception as e:
        print(f"解析热点概念数据失败：{e}")
        return None


def save_hot_concept() -> bool:
    """保存热点概念数据到数据库"""
    df = get_hot_concept()
    if df is None:
        return False

    from core.data import get_db_connection
    con = get_db_connection()

    try:
        with _xgb_lock:
            today = datetime.now().date()
            con.execute("DELETE FROM xgb_hot_concept WHERE date = ?", [today])
            con.execute("INSERT OR REPLACE INTO xgb_hot_concept SELECT * FROM df")
        return True
    except Exception as e:
        print(f"保存热点概念数据失败：{e}")
        return False
    finally:
        con.close()


# ========================= 一键更新所有数据（扩展新功能） =========================
def update_all_xgb_data() -> bool:
    """一键更新所有选股宝数据（含涨停/跌停/连板/炸板/情绪/概念）"""
    print("=" * 60)
    print("📊 更新选股宝数据...")
    print("-" * 60)

    success = 0
    total = 6  # 涨停+跌停+连板+炸板+情绪+概念

    # 1. 涨停池
    if save_limit_up():
        print("✅ 涨停池数据更新成功")
        success += 1
    else:
        print("❌ 涨停池数据更新失败")

    # 2. 跌停池
    if save_limit_down():
        print("✅ 跌停池数据更新成功")
        success += 1
    else:
        print("❌ 跌停池数据更新失败")

    # 3. 连板池
    if save_continuous_board():
        print("✅ 连板池数据更新成功")
        success += 1
    else:
        print("❌ 连板池数据更新失败")

    # 4. 炸板池
    if save_broken_board():
        print("✅ 炸板池数据更新成功")
        success += 1
    else:
        print("❌ 炸板池数据更新失败")

    # 5. 大盘情绪
    if save_market_sentiment():
        print("✅ 大盘情绪数据更新成功")
        success += 1
    else:
        print("❌ 大盘情绪数据更新失败")

    # 6. 热点概念
    if save_hot_concept():
        print("✅ 热点概念数据更新成功")
        success += 1
    else:
        print("❌ 热点概念数据更新失败")

    print("-" * 60)
    print(f"更新完成：{success}/{total} 成功")
    print("=" * 60)

    return success == total


# ========================= 初始化 =========================
if __name__ == "__main__":
    # 初始化表结构
    init_xgb_tables()
    # 测试一键更新
    # update_all_xgb_data()

    # 单独测试某类数据
    # save_continuous_board("2025-01-01")  # 测试连板数据
    # save_broken_board()  # 测试炸板数据
    pass
else:
    # 自动初始化表结构
    init_xgb_tables()