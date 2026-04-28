# core/intraday.py
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional
import re
import time

import polars as pl
import requests


SINA_REALTIME_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0",
}


def _safe_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _safe_str(v, default=""):
    if v is None:
        return default
    return str(v)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_date(v) -> str:
    if v is None or v == "":
        return _today_str()

    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d")

    s = str(v)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    return _today_str()


def _get_db_connection():
    from core.data import get_db_connection
    return get_db_connection()


def to_sina_symbol(code: str) -> str:
    code = str(code).zfill(6)

    if code.startswith(("600", "601", "603", "605", "688")):
        return "sh" + code

    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return "sz" + code

    if code.startswith("920"):
        return "bj" + code

    return "sz" + code


def _parse_sina_line(line: str) -> Optional[Dict[str, Any]]:
    if not line or "=" not in line:
        return None

    m_symbol = re.search(r"hq_str_([a-z]{2}\d{6})", line)
    if not m_symbol:
        return None

    sina_symbol = m_symbol.group(1)
    code = sina_symbol[-6:]

    m = re.search(r'="(.*)"', line)
    if not m:
        return None

    payload = m.group(1).strip()
    if not payload:
        return None

    fields = payload.split(",")

    if len(fields) < 10:
        return None

    name = fields[0].strip()
    open_price = _safe_float(fields[1], None)
    pre_close = _safe_float(fields[2], None)
    price = _safe_float(fields[3], None)
    high = _safe_float(fields[4], None)
    low = _safe_float(fields[5], None)
    volume = _safe_float(fields[8], 0.0)
    amount = _safe_float(fields[9], None)

    if price is None or price <= 0:
        return None

    if open_price is None or open_price <= 0:
        open_price = price
    if high is None or high <= 0:
        high = max(open_price, price)
    if low is None or low <= 0:
        low = min(open_price, price)
    if amount is None:
        amount = price * (volume or 0)

    q_date = _today_str()
    q_time = ""

    for f in fields:
        f = f.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", f):
            q_date = f
        elif re.match(r"^\d{2}:\d{2}:\d{2}$", f):
            q_time = f

    return {
        "code": code,
        "name": name,
        "price": price,
        "open": open_price,
        "pre_close": pre_close,
        "high": high,
        "low": low,
        "volume": volume,
        "amount": amount,
        "date": q_date,
        "time": q_time,
        "source": "sina_realtime",
        "updated_at": _now_str(),
    }


def fetch_sina_realtime_quotes(
    codes: List[str],
    batch_size: int = 400,
    interval_seconds: int = 10,
) -> pl.DataFrame:
    unique_codes = []
    seen = set()
    for c in codes:
        c = str(c).zfill(6)
        if c not in seen:
            unique_codes.append(c)
            seen.add(c)

    if not unique_codes:
        return pl.DataFrame()

    rows: List[Dict[str, Any]] = []
    total_batches = (len(unique_codes) + batch_size - 1) // batch_size

    for i in range(0, len(unique_codes), batch_size):
        batch = unique_codes[i:i + batch_size]
        batch_no = i // batch_size + 1

        symbols = ",".join([to_sina_symbol(c) for c in batch])
        url = SINA_REALTIME_URL.format(symbols=symbols)

        try:
            resp = requests.get(url, headers=SINA_HEADERS, timeout=12)
            text = resp.text.strip()

            for line in text.splitlines():
                item = _parse_sina_line(line)
                if item:
                    rows.append(item)

        except Exception:
            pass

        if batch_no < total_batches:
            time.sleep(interval_seconds)

    if not rows:
        return pl.DataFrame()

    return pl.DataFrame(rows)


def save_realtime_quotes(df: pl.DataFrame) -> int:
    if df is None or df.is_empty():
        return 0

    work = df.clone()

    required = {
        "code": pl.Utf8,
        "name": pl.Utf8,
        "price": pl.Float64,
        "open": pl.Float64,
        "pre_close": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "volume": pl.Float64,
        "amount": pl.Float64,
        "date": pl.Utf8,
        "time": pl.Utf8,
        "source": pl.Utf8,
        "updated_at": pl.Utf8,
    }

    for col, dtype in required.items():
        if col not in work.columns:
            work = work.with_columns(pl.lit(None).cast(dtype).alias(col))
        else:
            work = work.with_columns(pl.col(col).cast(dtype, strict=False))

    work = work.select(list(required.keys()))

    codes = work["code"].cast(pl.Utf8).unique().to_list()

    con = _get_db_connection()
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS realtime_quote (
            code VARCHAR,
            name VARCHAR,
            price DOUBLE,
            open DOUBLE,
            pre_close DOUBLE,
            high DOUBLE,
            low DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            date VARCHAR,
            time VARCHAR,
            source VARCHAR,
            updated_at VARCHAR
        )
        """)

        for col, sql_type in [
            ("name", "VARCHAR"),
            ("open", "DOUBLE"),
            ("pre_close", "DOUBLE"),
            ("high", "DOUBLE"),
            ("low", "DOUBLE"),
            ("source", "VARCHAR"),
            ("updated_at", "VARCHAR"),
        ]:
            try:
                con.execute(f"ALTER TABLE realtime_quote ADD COLUMN {col} {sql_type}")
            except Exception:
                pass

        if codes:
            placeholders = ",".join(["?"] * len(codes))
            con.execute(f"DELETE FROM realtime_quote WHERE code IN ({placeholders})", codes)

        con.register("_tmp_realtime_quote", work)

        con.execute("""
        INSERT INTO realtime_quote (
            code, name, price, open, pre_close, high, low,
            volume, amount, date, time, source, updated_at
        )
        SELECT
            code, name, price, open, pre_close, high, low,
            volume, amount, date, time, source, updated_at
        FROM _tmp_realtime_quote
        """)

        con.unregister("_tmp_realtime_quote")

    finally:
        con.close()

    return len(codes)


def refresh_realtime_quotes(
    codes: List[str],
    batch_size: int = 400,
    interval_seconds: int = 10,
) -> int:
    df = fetch_sina_realtime_quotes(
        codes=codes,
        batch_size=batch_size,
        interval_seconds=interval_seconds,
    )
    return save_realtime_quotes(df)


def load_realtime_quote(code: str) -> Dict[str, Any]:
    code = str(code).zfill(6)

    try:
        con = _get_db_connection()

        rows = con.execute(
            """
            SELECT *
            FROM realtime_quote
            WHERE code = ?
            LIMIT 1
            """,
            [code],
        ).fetchall()

        cols = [x[0] for x in con.description]
        con.close()

        if not rows:
            return {}

        row = dict(zip(cols, rows[0]))

    except Exception:
        return {}

    def pick(*names, default=None):
        for name in names:
            if name in row and row.get(name) not in [None, ""]:
                return row.get(name)
        return default

    price = _safe_float(pick("price", "close", "current", "latest", "last_price"), None)

    if price is None or price <= 0:
        return {}

    open_price = _safe_float(pick("open", "open_price", "today_open"), price)
    high = _safe_float(pick("high", "high_price"), max(open_price, price))
    low = _safe_float(pick("low", "low_price"), min(open_price, price))
    volume = _safe_float(pick("volume", "vol"), 0.0)
    amount = _safe_float(pick("amount", "turnover", "money"), None)

    if amount is None:
        amount = price * (volume or 0)

    q_date = _parse_date(pick("date", "trade_date", "day"))
    q_time = _safe_str(pick("time", "trade_time", "updated_time"), "")

    return {
        "code": code,
        "date": q_date,
        "time": q_time,
        "open": open_price,
        "high": high,
        "low": low,
        "close": price,
        "volume": volume,
        "amount": amount,
        "adj_type": "intraday",
        "source": "realtime_quote",
    }


def append_realtime_bar(df: pl.DataFrame, code: str, mode: str = "observe") -> pl.DataFrame:
    if mode not in {"observe", "tail_confirm", "watchlist_refresh"}:
        return df

    if df is None or df.is_empty():
        return df

    quote = load_realtime_quote(code)
    if not quote:
        return df

    q_date = quote["date"]

    work = df.clone()

    required_cols = [
        "date", "open", "high", "low", "close",
        "volume", "amount", "adj_type", "source"
    ]

    for col in required_cols:
        if col not in work.columns:
            if col in {"adj_type", "source"}:
                work = work.with_columns(pl.lit("").alias(col))
            else:
                work = work.with_columns(pl.lit(None).alias(col))

    work = work.with_columns(
        [
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
            pl.col("adj_type").cast(pl.Utf8),
            pl.col("source").cast(pl.Utf8),
        ]
    )

    last_date = str(work["date"][-1])[:10]

    if q_date < last_date:
        return work

    row = {
        "date": q_date,
        "open": quote["open"],
        "high": quote["high"],
        "low": quote["low"],
        "close": quote["close"],
        "volume": quote["volume"],
        "amount": quote["amount"],
        "adj_type": "intraday",
        "source": "realtime_quote",
    }

    row_df = pl.DataFrame([row]).with_columns(
        [
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
            pl.col("adj_type").cast(pl.Utf8),
            pl.col("source").cast(pl.Utf8),
        ]
    )

    work = work.filter(pl.col("date").cast(pl.Utf8).str.slice(0, 10) != q_date)

    return pl.concat([work, row_df], how="diagonal_relaxed").sort("date")
