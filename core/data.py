# -*- coding: utf-8 -*-
import os
import polars as pl
import duckdb
import requests
from functools import lru_cache

# ========================= 核心配置 =========================
CACHE_DIR = "data/daily"
DB_PATH = "data/stock_data.duckdb"
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_KEEP_DAYS = 3 * 365
SH_INDEX_CODE = "sh000001"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36")
}


# ========================= 数据库连接 =========================
def get_db_connection(read_only=False):
    return duckdb.connect(DB_PATH, read_only=read_only)


# ========================= 数据库&大盘数据初始化 =========================
def init_db():
    con = get_db_connection()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_daily (
                code VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adj_type VARCHAR,
                source VARCHAR,
                PRIMARY KEY (code, date)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_code ON stock_daily (code)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_date ON stock_daily (date)")
    finally:
        con.close()

    print("📈 初始化上证指数数据...")
    df = get_data(SH_INDEX_CODE)
    if df is not None:
        parquet_path = os.path.join(CACHE_DIR, f"{SH_INDEX_CODE}.parquet")
        if os.path.exists(parquet_path):
            try:
                os.remove(parquet_path)
            except:
                pass
        df.write_parquet(parquet_path, compression="snappy")
    print("✅ 上证指数数据初始化完成")


def get_exchange_prefix(code):
    if code.lower() == "sh000001" or code == "000001.SH":
        return "sh"
    if code.lower() == "sz399001" or code == "399001.SZ":
        return "sz"
    if code.startswith('920'):
        return "bj"
    elif code.startswith(('6', '5')):
        return "sh"
    elif code.startswith(('0', '2', '3')):
        return "sz"
    else:
        return "sz"


# ========================= 腾讯接口 =========================
def get_tencent(code):
    prefix = get_exchange_prefix(code)
    symbol = code.lower() if code.lower().startswith(("sh", "sz", "bj")) else prefix + code
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,500,fq"
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        r.raise_for_status()
        data = r.json()["data"][symbol]["day"]

        df = pl.DataFrame(data, schema=["date", "open", "close", "high", "low", "volume", "x"])
        df = df.select(["date", "open", "close", "high", "low", "volume"])

        df = df.with_columns([
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False)
        ])

        df = df.drop_nulls()
        df = df.filter(pl.col("volume") >= 0)

        df = df.with_columns([
            pl.lit("qfq").alias("adj_type"),
            pl.lit("tencent").alias("source")
        ])

        return df if len(df) > 0 else None
    except:
        return None


# ========================= 新浪备用 =========================
def get_sina(code):
    try:
        prefix = get_exchange_prefix(code)
        symbol = code.lower() if code.lower().startswith(("sh", "sz", "bj")) else prefix + code
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=500"
        r = requests.get(url, headers=HEADERS, timeout=5)
        if len(r.text) < 100:
            return None

        import json
        data = json.loads(r.text)
        if not data:
            return None

        df = pl.DataFrame(data)
        df = df.rename({
            "day": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        })

        df = df.with_columns([
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False)
        ])

        df = df.drop_nulls()
        df = df.filter(pl.col("volume") >= 0)

        df = df.with_columns([
            pl.lit("none").alias("adj_type"),
            pl.lit("sina").alias("source")
        ])

        return df.select(["date", "open", "high", "low", "close", "volume", "adj_type", "source"]) if len(
            df) > 0 else None
    except:
        return None


# ========================= DuckDB数据库读写 =========================
def load_from_db(code):
    try:
        con = get_db_connection(read_only=True)
        df = con.execute("""
            SELECT date, open, high, low, close, volume
            FROM stock_daily
            WHERE code = ?
            ORDER BY date
        """, [code]).pl()
        con.close()
        return df if len(df) > 0 else None
    except:
        return None


def save_to_db(code, df):
    try:
        con = get_db_connection()
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("DELETE FROM stock_daily WHERE code = ?", [code])
            df = df.with_columns(pl.lit(code).alias("code"))
            con.execute("INSERT OR REPLACE INTO stock_daily SELECT * FROM df")
            con.execute("COMMIT")
            con.close()
            return True
        except:
            con.execute("ROLLBACK")
            con.close()
            raise
    except:
        return False


# ========================= 【关键修复】加回 clean_old_cache 函数 =========================
def clean_old_cache():
    """清理超过3年的数据库数据"""
    try:
        cutoff_date = pl.date.today().sub(days=CACHE_KEEP_DAYS)
        con = get_db_connection()
        deleted = con.execute("""
            DELETE FROM stock_daily
            WHERE date < ?
        """, [cutoff_date]).rowcount
        con.close()
        if deleted > 0:
            print(f"缓存清理：已删除数据库中{deleted}条过期记录")
    except:
        pass


# ========================= 统一数据获取入口 =========================
@lru_cache(maxsize=10000)
def get_data(code):
    df_db = load_from_db(code)
    df_new = get_tencent(code)
    if df_new is None:
        df_new = get_sina(code)
    if df_new is None:
        return df_db

    if df_db is not None and len(df_db) > 0:
        max_db_date = df_db.select(pl.max("date")).item()
        df_incremental = df_new.filter(pl.col("date") > max_db_date)

        if len(df_incremental) > 0:
            df_merged = pl.concat([df_db, df_incremental]).unique("date").sort("date")
            save_to_db(code, df_merged)
            return df_merged
        else:
            return df_db
    else:
        save_to_db(code, df_new)
        return df_new


# 初始化
init_db()