# -*- coding: utf-8 -*-
"""
core/data.py

方案 D：真实股票基础表 + 新浪实时批量校验 + 历史K分阶段缓存

职责拆分：
1. stock_basic：AkShare 获取真实 A 股代码，本地缓存，默认 7 天刷新一次。
2. realtime_quote：新浪实时行情，单线程批量，400只/批，10秒间隔。
3. stock_daily：日线主表，腾讯历史K主源，efinance/新浪历史K备用，Sina 实时仅在收盘后补当天。
4. stock_daily_raw：原始返回透传，方便排查。

重要原则：
- 任何关键数据源不可用，不再继续跑无意义任务。
- 静态代码号段不进入正式流程。
- 主行情表字段稳定，不做“有几列存几列”。
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import threading
import time
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, Any, Iterable, List, Dict
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import requests

from core.logger import get_logger, log_source_failure, log_exception, log_raw_bad_row, log_reject

# ========================= 基础配置 =========================
CN_TZ = ZoneInfo("Asia/Shanghai")
CACHE_DIR = "data/daily"
DB_PATH = "data/stock_data.duckdb"
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

CACHE_KEEP_DAYS = 3 * 365
DEFAULT_BARS = 520
BASIC_CACHE_DAYS = int(os.getenv("BASIC_CACHE_DAYS", "7"))
SH_INDEX_CODE = "sh000001"
CYB_INDEX_CODE = "sz399006"

SAVE_RAW_ROWS = os.getenv("SAVE_RAW_ROWS", "1") not in {"0", "false", "False", "no", "NO"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}

SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0",
}
SINA_REALTIME_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_BATCH_SIZE = 400
SINA_BATCH_INTERVAL = 10.0
SINA_CACHE_SECONDS = 5.0

STATUS_NO_DATA = "no_data"
STATUS_YESTERDAY_CLOSE = "yesterday_close"
STATUS_INTRADAY = "intraday_unfinished"
STATUS_AFTER_CLOSE = "after_close"

_DB_LOCK = threading.RLock()
_SINA_CACHE: Dict[str, tuple[float, str]] = {}

INDEX_CANONICAL = {
    "sh000001", "sh000016", "sh000300", "sh000905", "sh000852",
    "sz399001", "sz399005", "sz399006",
}

VALID_PREFIXES = (
    "600", "601", "603", "605", "688",
    "000", "001", "002", "003", "300", "301",
    "920",
)

# ========================= 通用工具 =========================

def now_cn() -> datetime:
    return datetime.now(CN_TZ)


def today_cn() -> date:
    return now_cn().date()


def normalize_code(code: str) -> str:
    if code is None:
        return ""
    s = str(code).strip().lower()
    if s.endswith((".sh", ".sz", ".bj")):
        s = s[:-3]
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        s = s[2:]
    return s.zfill(6) if s.isdigit() and len(s) <= 6 else s


def canonical_code(code: str) -> str:
    """
    数据库存储代码：
    - 股票裸代码：000001、600519、920xxx
    - 指数/显式交易所代码：sh000001、sz399006
    裸 000001 必须代表平安银行；上证指数必须用 sh000001 或 000001.SH。
    """
    if code is None:
        return ""
    s = str(code).strip().lower()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[:2] + normalize_code(s)
    if s.endswith((".sh", ".sz", ".bj")):
        return s[-2:] + normalize_code(s)
    return normalize_code(s)


def get_exchange_prefix(code: str) -> str:
    s = str(code).strip().lower()
    if s.startswith("sh") or s.endswith(".sh"):
        return "sh"
    if s.startswith("sz") or s.endswith(".sz"):
        return "sz"
    if s.startswith("bj") or s.endswith(".bj"):
        return "bj"
    c = normalize_code(code)
    if c.startswith(("600", "601", "603", "605", "688")):
        return "sh"
    if c.startswith(("000", "001", "002", "003", "300", "301", "399")):
        return "sz"
    if c.startswith("920"):
        return "bj"
    return "sz"


def to_tencent_symbol(code: str) -> str:
    s = str(code).strip().lower()
    if s.startswith(("sh", "sz", "bj")):
        return s
    if s.endswith(".sh"):
        return "sh" + normalize_code(s)
    if s.endswith(".sz"):
        return "sz" + normalize_code(s)
    if s.endswith(".bj"):
        return "bj" + normalize_code(s)
    return get_exchange_prefix(code) + normalize_code(code)


def to_sina_symbol(code: str) -> str:
    """严格生成新浪实时接口 symbol：sh000001 / sz000001 / bj920xxx。"""
    s = str(code).strip().lower()
    if s.startswith(("sh", "sz", "bj")) and len(s) >= 8:
        return s[:2] + normalize_code(s)
    if s.endswith(".sh"):
        return "sh" + normalize_code(s)
    if s.endswith(".sz"):
        return "sz" + normalize_code(s)
    if s.endswith(".bj"):
        return "bj" + normalize_code(s)
    return get_exchange_prefix(code) + normalize_code(code)


def is_index_code(code: str) -> bool:
    return canonical_code(code) in INDEX_CANONICAL


def valid_a_share_code(code: str) -> bool:
    c = normalize_code(code)
    return len(c) == 6 and c.isdigit() and c.startswith(VALID_PREFIXES)


def classify_market(code: str) -> str:
    c = normalize_code(code)
    if c.startswith(("600", "601", "603", "605", "000", "001", "002", "003")):
        return "main"
    if c.startswith(("300", "301")):
        return "chinext"
    if c.startswith("688"):
        return "star"
    if c.startswith("920"):
        return "bse"
    return "other"


def classify_exchange(code: str) -> str:
    p = get_exchange_prefix(code)
    return {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(p, "")


def infer_board(code: str) -> str:
    c = normalize_code(code)
    if c.startswith(("600", "601", "603", "605")):
        return "上证主板"
    if c.startswith("688"):
        return "科创板"
    if c.startswith(("000", "001", "002", "003")):
        return "深证主板"
    if c.startswith(("300", "301")):
        return "创业板"
    if c.startswith("920"):
        return "北交所"
    return "其他"


def ensure_columns(df: pl.DataFrame) -> pl.DataFrame:
    required = {
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "amount": pl.Float64,
        "adj_type": pl.Utf8,
        "source": pl.Utf8,
    }
    for col, dtype in required.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
    return df.select(list(required.keys()))


def infer_data_status(df: Optional[pl.DataFrame]) -> str:
    if df is None or len(df) == 0 or "date" not in df.columns:
        return STATUS_NO_DATA
    try:
        last_date = df.select(pl.col("date").max()).item()
        if isinstance(last_date, datetime):
            last_date = last_date.date()
        today = today_cn()
        n = now_cn()
        if last_date < today:
            return STATUS_YESTERDAY_CLOSE
        if last_date > today:
            return STATUS_INTRADAY
        close_ready = n.hour > 15 or (n.hour == 15 and n.minute >= 5)
        return STATUS_AFTER_CLOSE if close_ready else STATUS_INTRADAY
    except Exception:
        return STATUS_NO_DATA

# ========================= DuckDB =========================

def get_db_connection(read_only: bool = False):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # 统一普通连接，避免 read_only / write 混用导致 DuckDB file handle 冲突。
    return duckdb.connect(DB_PATH)


def init_db() -> None:
    with _DB_LOCK:
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
                    amount DOUBLE,
                    adj_type VARCHAR,
                    source VARCHAR,
                    updated_at TIMESTAMP,
                    PRIMARY KEY (code, date, adj_type)
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_stock_daily_code ON stock_daily (code)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily (date)")
            con.execute("""
                CREATE TABLE IF NOT EXISTS stock_daily_raw (
                    code VARCHAR,
                    date DATE,
                    source VARCHAR,
                    raw_json VARCHAR,
                    raw_len INTEGER,
                    created_at TIMESTAMP,
                    PRIMARY KEY (code, date, source)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS stock_basic (
                    code VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    name VARCHAR,
                    exchange VARCHAR,
                    board VARCHAR,
                    market VARCHAR,
                    is_st BOOLEAN,
                    is_valid_quote BOOLEAN,
                    source VARCHAR,
                    updated_at TIMESTAMP
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS realtime_quote (
                    symbol VARCHAR PRIMARY KEY,
                    code VARCHAR,
                    name VARCHAR,
                    open DOUBLE,
                    pre_close DOUBLE,
                    price DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    volume BIGINT,
                    amount DOUBLE,
                    date DATE,
                    time VARCHAR,
                    source VARCHAR,
                    raw_text VARCHAR,
                    updated_at TIMESTAMP
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_basic_code ON stock_basic (code)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_quote_code ON realtime_quote (code)")
        finally:
            con.close()


def _fetchdf(sql: str, params: Optional[list[Any]] = None):
    with _DB_LOCK:
        con = get_db_connection()
        try:
            return con.execute(sql, params or []).fetchdf()
        finally:
            con.close()

# ========================= stock_basic =========================

def load_stock_basic() -> pl.DataFrame:
    init_db()
    try:
        pdf = _fetchdf("SELECT * FROM stock_basic ORDER BY code")
        if pdf is None or len(pdf) == 0:
            return pl.DataFrame()
        return pl.from_pandas(pdf)
    except Exception as e:
        log_exception("读取 stock_basic 失败", e)
        return pl.DataFrame()


def stock_basic_is_fresh(cache_days: int = BASIC_CACHE_DAYS) -> bool:
    df = load_stock_basic()
    if df.is_empty() or "updated_at" not in df.columns:
        return False
    try:
        latest = df.select(pl.col("updated_at").max()).item()
        if latest is None:
            return False
        if isinstance(latest, str):
            latest_dt = datetime.fromisoformat(latest)
        else:
            latest_dt = latest
        if latest_dt.tzinfo is not None:
            latest_dt = latest_dt.replace(tzinfo=None)
        return datetime.now().replace(tzinfo=None) - latest_dt <= timedelta(days=cache_days)
    except Exception:
        return False


def save_stock_basic(df: pl.DataFrame) -> bool:
    if df is None or df.is_empty():
        return False
    init_db()
    try:
        now = datetime.now(CN_TZ).replace(tzinfo=None)
        save_df = df.with_columns([
            pl.col("code").cast(pl.Utf8),
            pl.col("symbol").cast(pl.Utf8),
            pl.col("name").cast(pl.Utf8),
            pl.col("exchange").cast(pl.Utf8),
            pl.col("board").cast(pl.Utf8),
            pl.col("market").cast(pl.Utf8),
            pl.col("is_st").cast(pl.Boolean),
            pl.col("is_valid_quote").cast(pl.Boolean),
            pl.col("source").cast(pl.Utf8),
            pl.lit(now).alias("updated_at"),
        ]).select(["code", "symbol", "name", "exchange", "board", "market", "is_st", "is_valid_quote", "source", "updated_at"])
        with _DB_LOCK:
            con = get_db_connection()
            try:
                con.register("tmp_stock_basic", save_df)
                con.execute("INSERT OR REPLACE INTO stock_basic SELECT * FROM tmp_stock_basic")
                return True
            finally:
                con.close()
    except Exception as e:
        log_exception("保存 stock_basic 失败", e)
        return False


def _normalize_basic_df(pdf: Any, source: str) -> pl.DataFrame:
    df = pl.from_pandas(pdf) if not isinstance(pdf, pl.DataFrame) else pdf
    if df.is_empty():
        return pl.DataFrame()
    code_col = None
    name_col = None
    for c in ["code", "证券代码", "股票代码", "代码", "symbol"]:
        if c in df.columns:
            code_col = c
            break
    for c in ["name", "证券简称", "股票简称", "名称", "股票名称"]:
        if c in df.columns:
            name_col = c
            break
    if code_col is None or name_col is None:
        get_logger().warning("stock_basic 字段识别失败 source=%s columns=%s", source, df.columns)
        return pl.DataFrame()
    out = df.select([
        pl.col(code_col).cast(pl.Utf8).map_elements(normalize_code, return_dtype=pl.Utf8).alias("code"),
        pl.col(name_col).cast(pl.Utf8).alias("name"),
    ]).filter(pl.col("code").map_elements(valid_a_share_code, return_dtype=pl.Boolean))
    if out.is_empty():
        return out
    out = out.with_columns([
        pl.col("code").map_elements(to_sina_symbol, return_dtype=pl.Utf8).alias("symbol"),
        pl.col("code").map_elements(classify_exchange, return_dtype=pl.Utf8).alias("exchange"),
        pl.col("code").map_elements(infer_board, return_dtype=pl.Utf8).alias("board"),
        pl.col("code").map_elements(classify_market, return_dtype=pl.Utf8).alias("market"),
        pl.col("name").str.contains(r"ST|退", literal=False).fill_null(False).alias("is_st"),
        pl.lit(True).alias("is_valid_quote"),  # 初始默认 True，validate-basic 后更新
        pl.lit(source).alias("source"),
    ]).unique("code").sort("code")
    # 剔除 ST
    out = out.filter(~pl.col("is_st"))
    return out


def fetch_stock_basic_akshare() -> pl.DataFrame:
    try:
        import akshare as ak  # type: ignore
        pdf = ak.stock_info_a_code_name()
        df = _normalize_basic_df(pdf, "akshare")
        if not df.is_empty():
            get_logger().info("AkShare 股票基础表获取成功：%s 只", len(df))
        return df
    except Exception as e:
        log_exception("AkShare 股票基础表获取失败", e)
        return pl.DataFrame()


def build_stock_basic(force_refresh: bool = False, cache_days: int = BASIC_CACHE_DAYS) -> pl.DataFrame:
    init_db()
    cached = load_stock_basic()
    if not force_refresh and not cached.is_empty() and stock_basic_is_fresh(cache_days):
        get_logger().info("使用 stock_basic 缓存：%s 只，缓存未过期", len(cached))
        return cached
    df = fetch_stock_basic_akshare()
    if not df.is_empty():
        save_stock_basic(df)
        return load_stock_basic()
    if not cached.is_empty():
        get_logger().warning("AkShare 更新失败，使用旧 stock_basic 缓存：%s 只", len(cached))
        return cached
    get_logger().error("AkShare 获取股票基础表失败，且无 stock_basic 缓存，停止。")
    return pl.DataFrame()


def update_stock_basic_validity(valid_map: Dict[str, bool]) -> None:
    if not valid_map:
        return
    init_db()
    rows = [{"code": normalize_code(k), "is_valid_quote": bool(v)} for k, v in valid_map.items()]
    df = pl.DataFrame(rows)
    with _DB_LOCK:
        con = get_db_connection()
        try:
            con.register("tmp_valid", df)
            con.execute("""
                UPDATE stock_basic
                SET is_valid_quote = tmp_valid.is_valid_quote
                FROM tmp_valid
                WHERE stock_basic.code = tmp_valid.code
            """)
        finally:
            con.close()

# ========================= 新浪实时 =========================

def _request_sina_realtime(symbols: List[str], retries: List[int] = [1, 2, 5]) -> str:
    symbols_key = ",".join(symbols)
    now_ts = time.time()
    cached = _SINA_CACHE.get(symbols_key)
    if cached and now_ts - cached[0] <= SINA_CACHE_SECONDS:
        return cached[1]

    url = SINA_REALTIME_URL.format(symbols=symbols_key)
    last_err: Any = None
    for i, delay in enumerate([0] + retries):
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(url, headers=SINA_HEADERS, timeout=8)
            if r.status_code == 429:
                raise RuntimeError("HTTP 429 Too Many Requests")
            text = r.text or ""
            if text.strip() == "":
                raise RuntimeError("empty_response")
            _SINA_CACHE[symbols_key] = (time.time(), text)
            return text
        except Exception as e:
            last_err = e
            get_logger().warning("新浪实时请求失败 batch=%s attempt=%s err=%s", len(symbols), i + 1, e)
    raise RuntimeError(f"sina_realtime_failed: {last_err}")


def _parse_sina_line(line: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'var\s+hq_str_([a-z]{2}\d{6})="(.*)";?', line.strip())
    if not m:
        return None
    symbol = m.group(1)
    raw = m.group(2)
    if raw == "":
        return None
    parts = raw.split(",")
    if len(parts) < 10:
        return None
    code = normalize_code(symbol)
    # 常规个股完整字段：name,open,pre_close,price,high,low,bid,ask,volume,amount,...,date,time
    name = parts[0] if len(parts) > 0 else ""
    def f(idx: int) -> Optional[float]:
        try:
            return float(parts[idx]) if idx < len(parts) and parts[idx] not in {"", "None"} else None
        except Exception:
            return None
    def i(idx: int) -> Optional[int]:
        val = f(idx)
        return int(val) if val is not None else None

    d = parts[30] if len(parts) > 30 else None
    t = parts[31] if len(parts) > 31 else None
    # 部分指数接口字段较短，日期时间可能不存在；保守不补 daily，只保留 quote。
    try:
        parsed_date = datetime.strptime(d, "%Y-%m-%d").date() if d else None
    except Exception:
        parsed_date = None
    return {
        "symbol": symbol,
        "code": code,
        "name": name,
        "open": f(1),
        "pre_close": f(2),
        "price": f(3),
        "high": f(4),
        "low": f(5),
        "volume": i(8),
        "amount": f(9),
        "date": parsed_date,
        "time": t,
        "source": "sina_realtime",
        "raw_text": raw,
        "updated_at": datetime.now(CN_TZ).replace(tzinfo=None),
    }


def parse_sina_realtime_text(text: str) -> pl.DataFrame:
    rows: List[Dict[str, Any]] = []
    for line in text.splitlines():
        rec = _parse_sina_line(line)
        if rec and rec.get("price") and float(rec["price"] or 0) > 0:
            rows.append(rec)
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame(rows).with_columns([
        pl.col("date").cast(pl.Date, strict=False),
        pl.col("volume").cast(pl.Int64, strict=False),
    ])
    return df.unique("symbol")


def save_realtime_quotes(df: pl.DataFrame) -> bool:
    if df is None or df.is_empty():
        return False
    init_db()
    cols = ["symbol", "code", "name", "open", "pre_close", "price", "high", "low", "volume", "amount", "date", "time", "source", "raw_text", "updated_at"]
    save_df = df.select(cols)
    try:
        with _DB_LOCK:
            con = get_db_connection()
            try:
                con.register("tmp_realtime_quote", save_df)
                con.execute("INSERT OR REPLACE INTO realtime_quote SELECT * FROM tmp_realtime_quote")
            finally:
                con.close()
        return True
    except Exception as e:
        log_exception("保存 realtime_quote 失败", e)
        return False


def fetch_sina_realtime_batch(
    codes: Iterable[str],
    batch_size: int = SINA_BATCH_SIZE,
    batch_interval: float = SINA_BATCH_INTERVAL,
    save: bool = True,
) -> pl.DataFrame:
    """
    单线程批量请求新浪实时行情。
    - 默认 400 只/批
    - 每批间隔 10 秒
    - 不并发
    """
    symbols = [to_sina_symbol(c) for c in codes if c]
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        return pl.DataFrame()
    all_df: List[pl.DataFrame] = []
    empty_failures = 0
    for start in range(0, len(symbols), batch_size):
        batch = symbols[start:start + batch_size]
        try:
            text = _request_sina_realtime(batch)
            df = parse_sina_realtime_text(text)
            if df.is_empty():
                empty_failures += 1
                get_logger().warning("新浪实时批量返回无有效记录 batch=%s", len(batch))
                if empty_failures >= 3:
                    raise RuntimeError("新浪实时连续空响应 >= 3，熔断停止")
            else:
                empty_failures = 0
                all_df.append(df)
                if save:
                    save_realtime_quotes(df)
            done = min(start + batch_size, len(symbols))
            get_logger().info("新浪实时进度：%s/%s，本批有效=%s", done, len(symbols), len(df) if not df.is_empty() else 0)
        except Exception as e:
            log_exception("新浪实时批量请求异常", e)
            raise
        if start + batch_size < len(symbols):
            time.sleep(batch_interval)
    if not all_df:
        return pl.DataFrame()
    return pl.concat(all_df, how="diagonal_relaxed").unique("symbol")


def validate_stock_basic_with_sina(batch_size: int = SINA_BATCH_SIZE, batch_interval: float = SINA_BATCH_INTERVAL) -> pl.DataFrame:
    basic = load_stock_basic()
    if basic.is_empty():
        get_logger().error("stock_basic 为空，无法进行新浪实时校验")
        return pl.DataFrame()
    codes = basic.select("code").to_series().to_list()
    quote = fetch_sina_realtime_batch(codes, batch_size=batch_size, batch_interval=batch_interval, save=True)
    if quote.is_empty():
        get_logger().error("新浪实时校验失败：无有效 quote")
        return quote
    valid_codes = set(quote.select("code").to_series().to_list())
    update_stock_basic_validity({c: (c in valid_codes) for c in codes})
    get_logger().info("新浪实时校验完成：有效=%s / 基础表=%s", len(valid_codes), len(codes))
    return quote


def _quote_to_daily(df: pl.DataFrame) -> pl.DataFrame:
    if df is None or df.is_empty():
        return pl.DataFrame()
    return df.filter(pl.col("date").is_not_null()).select([
        pl.col("code"),
        pl.col("date"),
        pl.col("open"),
        pl.col("high"),
        pl.col("low"),
        pl.col("price").alias("close"),
        pl.col("volume"),
        pl.col("amount"),
        pl.lit("none").alias("adj_type"),
        pl.lit("sina_realtime").alias("source"),
    ]).drop_nulls(subset=["date", "open", "high", "low", "close"])


def realtime_can_patch_daily(q: Dict[str, Any]) -> bool:
    d = q.get("date")
    t = q.get("time") or ""
    if not d:
        return False
    n = now_cn()
    # 允许非交易日/晚上用接口返回日期自身；但必须时间已收盘，或本地时间已过 15:05。
    return (str(t) >= "15:00:00") or (n.hour > 15 or (n.hour == 15 and n.minute >= 5))


def patch_daily_from_sina_realtime(codes: Optional[Iterable[str]] = None, batch_size: int = SINA_BATCH_SIZE, batch_interval: float = SINA_BATCH_INTERVAL) -> int:
    if codes is None:
        basic = load_stock_basic()
        if basic.is_empty():
            return 0
        codes = basic.filter(pl.col("is_valid_quote") == True).select("code").to_series().to_list()
    quote = fetch_sina_realtime_batch(codes, batch_size=batch_size, batch_interval=batch_interval, save=True)
    if quote.is_empty():
        return 0
    # 逐行判断是否可补 daily
    rows = []
    for rec in quote.to_dicts():
        if realtime_can_patch_daily(rec):
            rows.append(rec)
    if not rows:
        get_logger().info("新浪实时可用，但当前非收盘完成状态，不补 stock_daily")
        return 0
    daily_df = _quote_to_daily(pl.DataFrame(rows))
    if daily_df.is_empty():
        return 0
    count = 0
    for code, g in daily_df.group_by("code"):
        c = code[0] if isinstance(code, tuple) else code
        if save_to_db(c, g.select(["date", "open", "high", "low", "close", "volume", "amount", "adj_type", "source"])):
            count += len(g)
    get_logger().info("新浪实时补日线完成：%s 条", count)
    return count

# ========================= 日线数据库 =========================

def load_from_db(code: str, adj_type: str = "qfq") -> Optional[pl.DataFrame]:
    init_db()
    c = canonical_code(code)
    try:
        pdf = _fetchdf("""
            SELECT date, open, high, low, close, volume, amount, adj_type, source
            FROM stock_daily
            WHERE code = ? AND adj_type = ?
            ORDER BY date
        """, [c, adj_type])
        if pdf is None or len(pdf) == 0:
            return None
        return ensure_columns(pl.from_pandas(pdf))
    except Exception as e:
        log_exception(f"读取数据库失败 code={c}", e)
        return None


def save_to_db(code: str, df: pl.DataFrame) -> bool:
    if df is None or len(df) == 0:
        return False
    init_db()
    c = canonical_code(code)
    df = ensure_columns(df).drop_nulls(subset=["date", "open", "high", "low", "close"])
    if df.is_empty():
        return False
    try:
        save_df = df.with_columns([
            pl.lit(c).alias("code"),
            pl.lit(datetime.now(CN_TZ).replace(tzinfo=None)).alias("updated_at"),
        ]).select(["code", "date", "open", "high", "low", "close", "volume", "amount", "adj_type", "source", "updated_at"])
        with _DB_LOCK:
            con = get_db_connection()
            try:
                con.register("tmp_stock_daily", save_df)
                con.execute("INSERT OR REPLACE INTO stock_daily SELECT * FROM tmp_stock_daily")
                return True
            finally:
                con.close()
    except Exception as e:
        log_exception(f"保存数据库失败 code={c}", e)
        return False


def save_raw_rows(code: str, source: str, rows: list[Any]) -> None:
    if not SAVE_RAW_ROWS or not rows:
        return
    init_db()
    c = canonical_code(code)
    raw_records = []
    created_at = datetime.now(CN_TZ).replace(tzinfo=None)
    for row in rows:
        try:
            if not isinstance(row, (list, tuple)) or len(row) < 1:
                continue
            raw_records.append({
                "code": c,
                "date": str(row[0]),
                "source": source,
                "raw_json": json.dumps(list(row), ensure_ascii=False),
                "raw_len": int(len(row)),
                "created_at": created_at,
            })
        except Exception:
            continue
    if not raw_records:
        return
    try:
        df = pl.DataFrame(raw_records).with_columns(
            pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        ).drop_nulls(subset=["date"])
        if df.is_empty():
            return
        with _DB_LOCK:
            con = get_db_connection()
            try:
                con.register("tmp_stock_daily_raw", df)
                con.execute("INSERT OR REPLACE INTO stock_daily_raw SELECT * FROM tmp_stock_daily_raw")
            finally:
                con.close()
    except Exception as e:
        log_exception(f"保存 raw 数据失败 code={c} source={source}", e)

# ========================= 历史行情源 =========================

def _parse_tencent_rows(code: str, rows: list[Any]) -> Optional[pl.DataFrame]:
    parsed: list[dict[str, Any]] = []
    c = canonical_code(code)
    for row in rows:
        if not isinstance(row, (list, tuple)):
            log_raw_bad_row(c, "tencent", row, "row_not_list")
            continue
        if len(row) < 6:
            log_raw_bad_row(c, "tencent", row, f"raw_len={len(row)} < 6")
            continue
        parsed.append({"date": row[0], "open": row[1], "close": row[2], "high": row[3], "low": row[4], "volume": row[5]})
    if not parsed:
        return None
    df = pl.DataFrame(parsed).select(["date", "open", "high", "low", "close", "volume"]).with_columns([
        pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Int64, strict=False),
    ]).drop_nulls(subset=["date", "open", "high", "low", "close"])
    if df.is_empty():
        return None
    df = df.with_columns([
        (pl.col("close") * pl.col("volume") * 100).cast(pl.Float64).alias("amount"),
        pl.lit("qfq").alias("adj_type"),
        pl.lit("tencent").alias("source"),
    ]).filter((pl.col("volume") >= 0) & (pl.col("close") > 0)).unique("date").sort("date")
    return ensure_columns(df) if len(df) > 0 else None


def get_tencent(code: str, bars: int = DEFAULT_BARS) -> Optional[pl.DataFrame]:
    symbol = to_tencent_symbol(code)
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,{bars},fq"
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", {}).get(symbol, {})
        rows = data.get("day") or data.get("qfqday") or []
        if not rows:
            return None
        save_raw_rows(code, "tencent", rows)
        return _parse_tencent_rows(code, rows)
    except Exception as e:
        log_source_failure(canonical_code(code), "tencent", "request_or_parse_failed", e)
        return None


def get_ef_stock_api():
    try:
        import efinance.stock as ef_stock  # type: ignore
        return ef_stock
    except Exception:
        pass
    try:
        import efinance as ef  # type: ignore
        return getattr(ef, "stock", None)
    except Exception:
        return None


def get_efinance(code: str, bars: int = DEFAULT_BARS) -> Optional[pl.DataFrame]:
    if is_index_code(code):
        return None
    ef_stock = get_ef_stock_api()
    if ef_stock is None:
        return None
    c = normalize_code(code)
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            pdf = ef_stock.get_quote_history(c, beg="19900101", end="20500101", klt=101, fqt=1)
        noise = buf.getvalue().strip()
        if noise:
            get_logger().debug("efinance 输出 | code=%s | %s", c, noise)
        if pdf is None or len(pdf) == 0:
            return None
        df = pl.from_pandas(pdf)
        rename_map = {"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"}
        for cn, en in rename_map.items():
            if cn in df.columns:
                df = df.rename({cn: en})
        needed = ["date", "open", "high", "low", "close", "volume", "amount"]
        if not all(x in df.columns for x in needed[:6]):
            return None
        if "amount" not in df.columns:
            df = df.with_columns((pl.col("close") * pl.col("volume") * 100).alias("amount"))
        df = df.select(needed).with_columns([
            pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
            pl.lit("qfq").alias("adj_type"),
            pl.lit("efinance").alias("source"),
        ]).drop_nulls(subset=["date", "open", "high", "low", "close"])
        df = df.filter((pl.col("volume") >= 0) & (pl.col("close") > 0)).unique("date").sort("date")
        return ensure_columns(df.tail(bars)) if len(df) > 0 else None
    except Exception as e:
        log_source_failure(canonical_code(code), "efinance", "request_or_parse_failed", e)
        return None


def get_sina_history(code: str, bars: int = DEFAULT_BARS) -> Optional[pl.DataFrame]:
    try:
        symbol = to_tencent_symbol(code)
        url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/" + f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={bars}"
        r = requests.get(url, headers=HEADERS, timeout=8)
        if not r.text or len(r.text) < 50:
            return None
        rows = json.loads(r.text)
        if not rows:
            return None
        df = pl.DataFrame(rows)
        if "day" in df.columns:
            df = df.rename({"day": "date"})
        df = df.select(["date", "open", "high", "low", "close", "volume"]).with_columns([
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Int64, strict=False),
        ]).drop_nulls(subset=["date", "open", "high", "low", "close"])
        df = df.with_columns([
            (pl.col("close") * pl.col("volume") * 100).cast(pl.Float64).alias("amount"),
            pl.lit("none").alias("adj_type"),
            pl.lit("sina_history").alias("source"),
        ]).filter((pl.col("volume") >= 0) & (pl.col("close") > 0)).unique("date").sort("date")
        return ensure_columns(df) if len(df) > 0 else None
    except Exception as e:
        log_source_failure(canonical_code(code), "sina_history", "request_or_parse_failed", e)
        return None


def _merge_cache_and_new(df_db: Optional[pl.DataFrame], df_new: pl.DataFrame) -> pl.DataFrame:
    if df_db is None or len(df_db) == 0:
        return df_new.unique("date").sort("date")
    return pl.concat([df_db, df_new], how="diagonal_relaxed").unique("date", keep="last").sort("date")


def fetch_remote_data(code: str, bars: int = DEFAULT_BARS, allow_sina: bool = True) -> Optional[pl.DataFrame]:
    for fn in (get_tencent, get_efinance):
        df = fn(code, bars=bars)
        if df is not None and len(df) > 0:
            return df
        time.sleep(0.05)
    if allow_sina:
        df = get_sina_history(code, bars=bars)
        if df is not None and len(df) > 0:
            return df
    get_logger().debug("远程行情全部失败 code=%s", canonical_code(code))
    return None


def get_data_with_status(code: str, bars: int = DEFAULT_BARS, force_refresh: bool = False, allow_sina_fallback: bool = True) -> Tuple[Optional[pl.DataFrame], str]:
    c = canonical_code(code)
    df_db = None if force_refresh else load_from_db(c, adj_type="qfq")
    df_new = fetch_remote_data(c, bars=bars, allow_sina=allow_sina_fallback)
    if df_new is None or len(df_new) == 0:
        if df_db is not None and len(df_db) > 0:
            return df_db.tail(bars), infer_data_status(df_db)
        log_source_failure(c, "all", "no_remote_and_no_cache", "所有行情源失败且无本地缓存")
        return None, STATUS_NO_DATA
    new_adj = df_new.select(pl.col("adj_type").last()).item()
    if new_adj != "qfq" and df_db is not None and len(df_db) > 0:
        return df_db.tail(bars), infer_data_status(df_db)
    merged = _merge_cache_and_new(df_db, df_new) if new_adj == "qfq" else df_new
    status = infer_data_status(merged)
    if new_adj == "qfq" and status != STATUS_INTRADAY:
        save_to_db(c, merged)
    return merged.tail(bars), status


def get_data(code: str, bars: int = DEFAULT_BARS, force_refresh: bool = False) -> Optional[pl.DataFrame]:
    df, _ = get_data_with_status(code, bars=bars, force_refresh=force_refresh)
    return df

# ========================= 分阶段缓存 / 预检 =========================

def preflight_data_sources() -> Dict[str, bool]:
    result = {"sina_index": False, "sina_stock": False, "stock_basic": False, "tencent_history": False, "duckdb": False}
    try:
        init_db()
        result["duckdb"] = True
    except Exception:
        pass
    try:
        q = fetch_sina_realtime_batch(["sh000001", "sz000001"], batch_size=400, batch_interval=0, save=True)
        result["sina_index"] = not q.filter(pl.col("symbol") == "sh000001").is_empty() if not q.is_empty() else False
        result["sina_stock"] = not q.filter(pl.col("symbol") == "sz000001").is_empty() if not q.is_empty() else False
    except Exception:
        pass
    try:
        result["stock_basic"] = not build_stock_basic(force_refresh=False).is_empty()
    except Exception:
        pass
    try:
        result["tencent_history"] = get_tencent("000001", bars=20) is not None
    except Exception:
        pass
    get_logger().info("数据源预检：%s", result)
    return result


def build_daily_cache(
    limit: Optional[int] = None,
    bars: int = DEFAULT_BARS,
    workers: int = 1,
    only_valid_quote: bool = True,
    stop_consecutive_failures: int = 20,
    stop_failure_rate: float = 0.50,
) -> Dict[str, int]:
    """历史K分阶段缓存。默认 workers=1，避免免费接口高频失败。"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    basic = load_stock_basic()
    if basic.is_empty():
        get_logger().error("stock_basic 为空，无法构建历史K缓存")
        return {"processed": 0, "success": 0, "failed": 0}
    if only_valid_quote and "is_valid_quote" in basic.columns:
        basic = basic.filter(pl.col("is_valid_quote") == True)
    codes = basic.select("code").to_series().to_list()
    if limit:
        codes = codes[:limit]
    if not codes:
        return {"processed": 0, "success": 0, "failed": 0}

    processed = success = failed = consecutive_fail = 0

    def one(c: str) -> bool:
        df, status = get_data_with_status(c, bars=bars)
        ok = df is not None and len(df) >= 250 and status != STATUS_NO_DATA
        if not ok:
            log_reject(c, "daily_cache", "no_kline_data", f"status={status}, rows={len(df) if df is not None else 0}")
        return ok

    mw = max(1, int(workers or 1))
    with ThreadPoolExecutor(max_workers=mw) as ex:
        futures = {ex.submit(one, c): c for c in codes}
        for fut in as_completed(futures):
            processed += 1
            try:
                ok = fut.result()
            except Exception as e:
                ok = False
                log_exception(f"历史K缓存线程异常 code={futures[fut]}", e)
            if ok:
                success += 1
                consecutive_fail = 0
            else:
                failed += 1
                consecutive_fail += 1
            if processed % 50 == 0:
                get_logger().info("历史K缓存进度：%s/%s 成功=%s 失败=%s", processed, len(codes), success, failed)
            if consecutive_fail >= stop_consecutive_failures:
                get_logger().error("历史K连续失败 %s 次，熔断停止。", consecutive_fail)
                break
            if processed >= 30 and failed / processed > stop_failure_rate:
                get_logger().error("历史K失败率 %.2f 超过阈值 %.2f，熔断停止。", failed / processed, stop_failure_rate)
                break
    return {"processed": processed, "success": success, "failed": failed}


if __name__ == "__main__":
    init_db()
    print(preflight_data_sources())
