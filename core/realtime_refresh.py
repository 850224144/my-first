# core/realtime_refresh.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import os
import time

import polars as pl
import requests


SINA_REALTIME_URL = "http://hq.sinajs.cn/list={symbols}"
SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0",
}

DEFAULT_BATCH_SIZE = 400
DEFAULT_BATCH_INTERVAL = 10.0
DEFAULT_BACKOFFS = [1, 2, 5]


@dataclass
class RefreshStats:
    requested: int = 0
    success: int = 0
    failed: int = 0
    batches: int = 0
    empty_response: int = 0
    parse_failed: int = 0
    written: int = 0

    @property
    def success_rate(self) -> float:
        if self.requested <= 0:
            return 0.0
        return self.success / self.requested

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requested": self.requested,
            "success": self.success,
            "failed": self.failed,
            "batches": self.batches,
            "empty_response": self.empty_response,
            "parse_failed": self.parse_failed,
            "written": self.written,
            "success_rate": round(self.success_rate, 4),
        }


def _get_logger():
    try:
        from core.logger import get_logger

        return get_logger()
    except Exception:
        import logging

        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("a_stock")


logger = _get_logger()


def _get_db_connection():
    from core.data import get_db_connection

    return get_db_connection()


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def to_sina_symbol(code: str) -> Optional[str]:
    """
    A 股代码转新浪实时接口 symbol。
    必须使用：
    - 上证：sh600xxx / sh601xxx / sh603xxx / sh605xxx / sh688xxx
    - 深证：sz000xxx / sz001xxx / sz002xxx / sz003xxx / sz300xxx / sz301xxx
    - 北交所：bj920xxx
    """
    c = str(code).strip().zfill(6)

    if c.startswith(("600", "601", "603", "605", "688")):
        return f"sh{c}"

    if c.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz{c}"

    if c.startswith("920"):
        return f"bj{c}"

    return None


def _code_from_sina_symbol(symbol: str) -> str:
    s = str(symbol).strip()
    return s[-6:]


def _bad_rows_file() -> str:
    os.makedirs("logs", exist_ok=True)
    return os.path.join("logs", f"realtime_bad_rows_{datetime.now().strftime('%Y%m%d')}.jsonl")


def _log_bad_row(symbol: str, reason: str, raw: str = ""):
    try:
        with open(_bad_rows_file(), "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "ts": _now_str(),
                        "symbol": symbol,
                        "reason": reason,
                        "raw": raw[:500],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        pass


def _parse_sina_line(line: str) -> Optional[Dict[str, Any]]:
    """
    解析新浪实时行情行：
    var hq_str_sz000001="平安银行,11.79,11.88,11.83,...,2026-04-28,10:30:00,00";
    """
    if not line or "hq_str_" not in line:
        return None

    try:
        left, right = line.split("=", 1)
        symbol = left.split("hq_str_", 1)[1].strip()
        payload = right.strip().strip(";").strip()

        if payload.startswith('"') and payload.endswith('"'):
            payload = payload[1:-1]

        if not payload:
            _log_bad_row(symbol, "empty_payload", line)
            return None

        fields = payload.split(",")

        # 股票实时接口常见字段至少 32 个；但柔性处理，少字段也尽量解析。
        if len(fields) < 10:
            _log_bad_row(symbol, f"fields_too_short_{len(fields)}", line)
            return None

        name = fields[0].strip()
        open_price = _safe_float(fields[1], None)
        pre_close = _safe_float(fields[2], None)
        price = _safe_float(fields[3], None)
        high = _safe_float(fields[4], None)
        low = _safe_float(fields[5], None)
        volume = _safe_float(fields[8], 0.0)
        amount = _safe_float(fields[9], 0.0)

        # 日期和时间通常在 30/31 位；柔性查找。
        date_str = ""
        time_str = ""
        for item in fields:
            item = item.strip()
            if len(item) == 10 and item[4] == "-" and item[7] == "-":
                date_str = item
            if len(item) == 8 and item[2] == ":" and item[5] == ":":
                time_str = item

        if not date_str:
            date_str = _today_str()

        if price is None or price <= 0:
            _log_bad_row(symbol, "invalid_price", line)
            return None

        if open_price is None or open_price <= 0:
            open_price = price
        if high is None or high <= 0:
            high = max(open_price, price)
        if low is None or low <= 0:
            low = min(open_price, price)

        code = _code_from_sina_symbol(symbol)
        pct_chg = None
        if pre_close and pre_close > 0:
            pct_chg = (price / pre_close - 1) * 100

        return {
            "code": code,
            "symbol": symbol,
            "name": name,
            "price": float(price),
            "open": float(open_price),
            "pre_close": float(pre_close) if pre_close is not None else None,
            "high": float(high),
            "low": float(low),
            "volume": float(volume or 0),
            "amount": float(amount or 0),
            "pct_chg": float(pct_chg) if pct_chg is not None else None,
            "date": date_str,
            "time": time_str,
            "source": "sina_realtime",
            "updated_at": _now_str(),
            "raw": payload[:1000],
        }

    except Exception as e:
        _log_bad_row("unknown", f"parse_exception:{e}", line)
        return None


def _request_sina_batch(symbols: List[str]) -> str:
    url = SINA_REALTIME_URL.format(symbols=",".join(symbols))
    resp = requests.get(url, headers=SINA_HEADERS, timeout=10)
    # 新浪接口常见 GBK/GB18030，requests 可能猜错，手动兜底
    if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
        resp.encoding = "gbk"
    return resp.text.strip()


def _chunks(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def load_stock_basic_codes(limit: Optional[int] = None) -> List[str]:
    try:
        con = _get_db_connection()
        sql = """
        SELECT code
        FROM stock_basic
        WHERE code IS NOT NULL
        ORDER BY code
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = con.execute(sql).fetchall()
        con.close()
        return [str(x[0]).zfill(6) for x in rows if x and x[0]]
    except Exception:
        return []


def _ensure_realtime_table(con):
    con.execute(
        """
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
            updated_at VARCHAR,
            raw VARCHAR
        )
        """
    )

    # 兼容旧表缺字段
    cols = {r[1] for r in con.execute("PRAGMA table_info('realtime_quote')").fetchall()}
    add_cols = {
        "symbol": "VARCHAR",
        "name": "VARCHAR",
        "open": "DOUBLE",
        "pre_close": "DOUBLE",
        "high": "DOUBLE",
        "low": "DOUBLE",
        "pct_chg": "DOUBLE",
        "source": "VARCHAR",
        "updated_at": "VARCHAR",
        "raw": "VARCHAR",
    }
    for col, typ in add_cols.items():
        if col not in cols:
            try:
                con.execute(f"ALTER TABLE realtime_quote ADD COLUMN {col} {typ}")
            except Exception:
                pass


def _write_realtime_quotes(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    df = pl.DataFrame(rows)
    df = df.with_columns(
        [
            pl.col("code").cast(pl.Utf8),
            pl.col("symbol").cast(pl.Utf8),
            pl.col("name").cast(pl.Utf8),
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("pre_close").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
            pl.col("pct_chg").cast(pl.Float64, strict=False),
            pl.col("date").cast(pl.Utf8),
            pl.col("time").cast(pl.Utf8),
            pl.col("source").cast(pl.Utf8),
            pl.col("updated_at").cast(pl.Utf8),
            pl.col("raw").cast(pl.Utf8),
        ]
    )

    con = _get_db_connection()
    try:
        _ensure_realtime_table(con)
        codes = df["code"].cast(pl.Utf8).unique().to_list()
        placeholders = ",".join(["?"] * len(codes))
        con.execute(f"DELETE FROM realtime_quote WHERE code IN ({placeholders})", codes)

        con.register("_tmp_realtime_quote", df)
        con.execute(
            """
            INSERT INTO realtime_quote (
                code, symbol, name, price, open, pre_close, high, low,
                volume, amount, pct_chg, date, time, source, updated_at, raw
            )
            SELECT
                code, symbol, name, price, open, pre_close, high, low,
                volume, amount, pct_chg, date, time, source, updated_at, raw
            FROM _tmp_realtime_quote
            """
        )
        con.unregister("_tmp_realtime_quote")
        return len(rows)
    finally:
        con.close()


def refresh_realtime_quotes(
    codes: Optional[List[str]] = None,
    *,
    limit: Optional[int] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    batch_interval: float = DEFAULT_BATCH_INTERVAL,
    backoffs: Optional[List[float]] = None,
    min_success_rate: float = 0.5,
) -> Dict[str, Any]:
    """
    刷新新浪实时行情并写入 realtime_quote。

    默认单线程，400只/批，批间隔10秒，符合你前面定的规则。
    """
    if backoffs is None:
        backoffs = DEFAULT_BACKOFFS

    if codes is None:
        codes = load_stock_basic_codes(limit=limit)
    else:
        codes = [str(c).zfill(6) for c in codes]
        if limit:
            codes = codes[: int(limit)]

    symbols = []
    bad_codes = []
    for code in codes:
        s = to_sina_symbol(code)
        if s:
            symbols.append(s)
        else:
            bad_codes.append(code)

    stats = RefreshStats(requested=len(symbols))

    if not symbols:
        logger.warning("实时行情刷新：没有有效 symbol")
        return stats.to_dict()

    all_rows: List[Dict[str, Any]] = []
    batches = list(_chunks(symbols, max(1, int(batch_size))))
    stats.batches = len(batches)

    for bi, batch in enumerate(batches, start=1):
        text = ""
        ok = False
        for attempt, wait_seconds in enumerate([0] + backoffs, start=1):
            if wait_seconds:
                time.sleep(wait_seconds)
            try:
                text = _request_sina_batch(batch)
                if text:
                    ok = True
                    break
                stats.empty_response += 1
            except Exception as e:
                logger.warning(f"新浪实时批次失败 batch={bi}/{len(batches)} attempt={attempt}: {e}")

        if not ok or not text:
            stats.failed += len(batch)
            logger.warning(f"新浪实时批次为空 batch={bi}/{len(batches)} size={len(batch)}")
        else:
            rows = []
            for line in text.splitlines():
                row = _parse_sina_line(line.strip())
                if row:
                    rows.append(row)
                else:
                    stats.parse_failed += 1
            got_codes = {r["code"] for r in rows}
            batch_codes = {_code_from_sina_symbol(s) for s in batch}
            missing = batch_codes - got_codes
            stats.success += len(rows)
            stats.failed += len(missing)
            all_rows.extend(rows)
            logger.info(
                f"新浪实时批次 {bi}/{len(batches)} 完成：请求 {len(batch)}，有效 {len(rows)}，缺失 {len(missing)}"
            )

        if bi < len(batches):
            time.sleep(batch_interval)

    written = _write_realtime_quotes(all_rows)
    stats.written = written

    summary = stats.to_dict()
    if summary["success_rate"] < min_success_rate:
        logger.warning(f"实时行情刷新成功率偏低：{summary}")
    else:
        logger.info(f"实时行情刷新完成：{summary}")

    return summary


__all__ = [
    "refresh_realtime_quotes",
    "load_stock_basic_codes",
    "to_sina_symbol",
]
