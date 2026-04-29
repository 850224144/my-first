# core/realtime_guard.py
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Tuple
import os
import time
import re

import requests


SINA_REALTIME_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0",
}

DEFAULT_BATCH_SIZE = 400
DEFAULT_BATCH_INTERVAL = 10


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_float(v: Any, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_code(code: Any) -> str:
    s = str(code).strip()
    if "." in s:
        s = s.split(".")[0]
    return s.zfill(6)


def to_sina_symbol(code: str) -> str:
    code = _to_code(code)
    if code.startswith(("600", "601", "603", "605", "688")):
        return f"sh{code}"
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz{code}"
    if code.startswith("920"):
        return f"bj{code}"
    # 保守兜底：不要生成错误代码；返回空让上层跳过
    return ""


def from_sina_symbol(symbol: str) -> str:
    return str(symbol)[2:]


def _split_batches(items: List[str], batch_size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def _extract_lines(text: str) -> List[Tuple[str, str]]:
    """返回 [(sina_symbol, payload), ...]。"""
    if not text:
        return []
    rows: List[Tuple[str, str]] = []
    for m in re.finditer(r'var\s+hq_str_([a-z]{2}\d{6})="(.*?)";', text, flags=re.S):
        rows.append((m.group(1), m.group(2)))
    return rows


def _parse_stock_payload(symbol: str, payload: str) -> Dict[str, Any]:
    fields = payload.split(",")
    if len(fields) < 32:
        return {}

    name = fields[0].strip()
    if not name:
        return {}

    open_price = _safe_float(fields[1], None)
    pre_close = _safe_float(fields[2], None)
    price = _safe_float(fields[3], None)
    high = _safe_float(fields[4], None)
    low = _safe_float(fields[5], None)
    volume = _safe_float(fields[8], 0.0)
    amount = _safe_float(fields[9], None)
    q_date = fields[30].strip() if len(fields) > 30 else _today_str()
    q_time = fields[31].strip() if len(fields) > 31 else ""

    if price is None or price <= 0:
        return {}

    if open_price is None or open_price <= 0:
        open_price = price
    if high is None or high <= 0:
        high = max(open_price, price)
    if low is None or low <= 0:
        low = min(open_price, price)
    if amount is None:
        amount = price * (volume or 0.0)

    pct_chg = None
    if pre_close and pre_close > 0:
        pct_chg = (price / pre_close - 1) * 100

    return {
        "code": from_sina_symbol(symbol),
        "symbol": symbol,
        "name": name,
        "price": float(price),
        "open": float(open_price),
        "pre_close": float(pre_close) if pre_close else None,
        "high": float(high),
        "low": float(low),
        "volume": float(volume or 0.0),
        "amount": float(amount or 0.0),
        "pct_chg": float(pct_chg) if pct_chg is not None else None,
        "date": q_date or _today_str(),
        "time": q_time,
        "source": "sina_realtime",
        "updated_at": _now_str(),
    }


def _get_db_connection():
    from core.data import get_db_connection
    return get_db_connection()


def _ensure_realtime_table(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS realtime_quote (
        code VARCHAR,
        symbol VARCHAR,
        name VARCHAR,
        price DOUBLE,
        open DOUBLE,
        pre_close DOUBLE,
        high DOUBLE,
        low DOUBLE,
        volume DOUBLE,
        amount DOUBLE,
        pct_chg DOUBLE,
        date VARCHAR,
        time VARCHAR,
        source VARCHAR,
        updated_at VARCHAR
    )
    """)

    for col, typ in [
        ("symbol", "VARCHAR"),
        ("name", "VARCHAR"),
        ("open", "DOUBLE"),
        ("pre_close", "DOUBLE"),
        ("high", "DOUBLE"),
        ("low", "DOUBLE"),
        ("volume", "DOUBLE"),
        ("amount", "DOUBLE"),
        ("pct_chg", "DOUBLE"),
        ("date", "VARCHAR"),
        ("time", "VARCHAR"),
        ("source", "VARCHAR"),
        ("updated_at", "VARCHAR"),
    ]:
        try:
            con.execute(f"ALTER TABLE realtime_quote ADD COLUMN {col} {typ}")
        except Exception:
            pass


def _write_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return

    import polars as pl

    con = _get_db_connection()
    try:
        _ensure_realtime_table(con)
        codes = [r["code"] for r in rows]
        placeholders = ",".join(["?"] * len(codes))
        con.execute(f"DELETE FROM realtime_quote WHERE code IN ({placeholders})", codes)

        df = pl.DataFrame(rows).select([
            "code", "symbol", "name", "price", "open", "pre_close", "high", "low",
            "volume", "amount", "pct_chg", "date", "time", "source", "updated_at"
        ])
        con.register("_tmp_realtime_quote", df)
        con.execute("""
        INSERT INTO realtime_quote (
            code, symbol, name, price, open, pre_close, high, low,
            volume, amount, pct_chg, date, time, source, updated_at
        )
        SELECT
            code, symbol, name, price, open, pre_close, high, low,
            volume, amount, pct_chg, date, time, source, updated_at
        FROM _tmp_realtime_quote
        """)
        con.unregister("_tmp_realtime_quote")
    finally:
        con.close()


def refresh_realtime_quotes(
    codes: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_interval: int = DEFAULT_BATCH_INTERVAL,
    min_success_rate: float = 0.7,
    fail_fast: bool = False,
) -> Dict[str, Any]:
    """
    刷新浪实时行情并写入 realtime_quote。
    返回刷新统计。不会兜底旧数据。
    """
    codes = [_to_code(c) for c in codes if str(c).strip()]
    codes = list(dict.fromkeys(codes))

    symbols = [to_sina_symbol(c) for c in codes]
    symbols = [s for s in symbols if s]

    requested = len(symbols)
    success_rows: List[Dict[str, Any]] = []
    failed: List[str] = []
    newest_dt = ""

    if requested == 0:
        return {
            "requested": 0,
            "success": 0,
            "failed": 0,
            "success_rate": 0.0,
            "newest_quote_time": "",
            "ok": False,
            "message": "没有可刷新的股票代码",
        }

    for idx, batch in enumerate(_split_batches(symbols, max(1, batch_size))):
        if idx > 0 and batch_interval > 0:
            time.sleep(batch_interval)

        url = SINA_REALTIME_URL.format(symbols=",".join(batch))
        try:
            resp = requests.get(url, headers=SINA_HEADERS, timeout=10)
            text = resp.text.strip()
        except Exception:
            failed.extend(batch)
            if fail_fast:
                break
            continue

        parsed_symbols = set()
        for symbol, payload in _extract_lines(text):
            parsed_symbols.add(symbol)
            row = _parse_stock_payload(symbol, payload)
            if row:
                success_rows.append(row)
                qdt = f"{row.get('date', '')} {row.get('time', '')}".strip()
                if qdt > newest_dt:
                    newest_dt = qdt
            else:
                failed.append(symbol)

        for symbol in batch:
            if symbol not in parsed_symbols:
                failed.append(symbol)

    _write_rows(success_rows)

    success = len(success_rows)
    success_rate = success / requested if requested else 0.0
    ok = success_rate >= min_success_rate

    return {
        "requested": requested,
        "success": success,
        "failed": requested - success,
        "success_rate": success_rate,
        "newest_quote_time": newest_dt,
        "ok": ok,
        "message": f"实时行情刷新结果：请求={requested} 成功={success} 成功率={success_rate:.2%}",
    }


def get_realtime_quote_summary(codes: List[str], max_age_minutes: int = 20) -> Dict[str, Any]:
    codes = [_to_code(c) for c in codes if str(c).strip()]
    if not codes:
        return {"requested": 0, "fresh": 0, "stale": 0, "newest_quote_time": "", "ok": False}

    con = _get_db_connection()
    try:
        placeholders = ",".join(["?"] * len(codes))
        rows = con.execute(f"""
            SELECT code, price, date, time, updated_at
            FROM realtime_quote
            WHERE code IN ({placeholders})
        """, codes).fetchall()
    except Exception:
        rows = []
    finally:
        con.close()

    today = _today_str()
    newest = ""
    fresh = 0
    stale = 0

    for code, price, q_date, q_time, updated_at in rows:
        q_date = str(q_date)[:10] if q_date is not None else ""
        q_time = str(q_time) if q_time is not None else ""
        qdt = f"{q_date} {q_time}".strip()
        if qdt > newest:
            newest = qdt
        if q_date == today and _safe_float(price, 0) and _safe_float(price, 0) > 0:
            fresh += 1
        else:
            stale += 1

    missing = len(codes) - len(rows)
    stale += max(0, missing)

    return {
        "requested": len(codes),
        "fresh": fresh,
        "stale": stale,
        "missing": missing,
        "fresh_rate": fresh / len(codes) if codes else 0.0,
        "newest_quote_time": newest,
        "ok": fresh > 0 and stale == 0,
    }


def refresh_and_validate_realtime(
    codes: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_interval: int = DEFAULT_BATCH_INTERVAL,
    min_success_rate: float = 0.7,
    max_age_minutes: int = 20,
) -> Dict[str, Any]:
    refresh = refresh_realtime_quotes(
        codes=codes,
        batch_size=batch_size,
        batch_interval=batch_interval,
        min_success_rate=min_success_rate,
    )
    summary = get_realtime_quote_summary(codes, max_age_minutes=max_age_minutes)

    ok = bool(refresh.get("ok")) and summary.get("fresh", 0) > 0

    return {
        **refresh,
        "fresh": summary.get("fresh", 0),
        "stale": summary.get("stale", 0),
        "missing": summary.get("missing", 0),
        "fresh_rate": summary.get("fresh_rate", 0.0),
        "newest_quote_time": summary.get("newest_quote_time") or refresh.get("newest_quote_time", ""),
        "ok": ok,
        "message": (
            f"实时行情刷新结果：请求={refresh.get('requested', 0)} "
            f"成功={refresh.get('success', 0)} "
            f"成功率={refresh.get('success_rate', 0.0):.2%} "
            f"fresh={summary.get('fresh', 0)} stale={summary.get('stale', 0)} "
            f"最新={summary.get('newest_quote_time') or refresh.get('newest_quote_time', '')}"
        ),
    }


def should_require_realtime(mode: str) -> bool:
    return mode in {"observe", "tail_confirm", "watchlist_refresh"}
