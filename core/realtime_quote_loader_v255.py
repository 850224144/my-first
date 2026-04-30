"""
v2.5.5 DuckDB realtime_quote 加载器。

合并 v2.5.4.1 修复：
- date/datetime/time 全部安全转字符串，避免 json.dumps 报错

新增：
- latest_date 报告
- 如果当天无数据，可读取最新数据，但 fresh_quote=False
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt

try:
    from .data_normalizer import normalize_symbol
except Exception:
    def normalize_symbol(x): return str(x)


def _connect(db_path: str | Path):
    try:
        import duckdb
    except Exception as exc:
        raise RuntimeError("未安装 duckdb，无法读取 realtime_quote") from exc
    return duckdb.connect(str(db_path), read_only=True)


def safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    return str(value)


def parse_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    s = s[:10].replace("/", "-")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return dt.date.fromisoformat(s).isoformat()
    except Exception:
        return None


def inspect_realtime_quote_table(db_path: str | Path = "data/stock_data.duckdb") -> Dict[str, Any]:
    p = Path(db_path)
    if not p.exists():
        return {"exists": False, "path": str(p), "table_exists": False}

    try:
        con = _connect(p)
        tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]
        if "realtime_quote" not in tables:
            con.close()
            return {"exists": True, "path": str(p), "table_exists": False, "tables": tables}

        cols = [x[1] for x in con.execute("PRAGMA table_info('realtime_quote')").fetchall()]
        count = int(con.execute("SELECT COUNT(*) FROM realtime_quote").fetchone()[0])

        recent_dates = []
        latest_date = None
        if "date" in cols:
            raw_dates = [x[0] for x in con.execute("SELECT DISTINCT date FROM realtime_quote ORDER BY date DESC LIMIT 10").fetchall()]
            recent_dates = [safe_str(x) for x in raw_dates]
            latest = con.execute("SELECT MAX(date) FROM realtime_quote").fetchone()[0]
            latest_date = parse_date(latest)

        con.close()
        return {
            "exists": True,
            "path": str(p),
            "table_exists": True,
            "columns": cols,
            "rows": count,
            "latest_date": latest_date,
            "recent_dates": recent_dates,
        }
    except Exception as exc:
        return {"exists": True, "path": str(p), "table_exists": False, "error": str(exc)}


def load_realtime_quote_map_from_duckdb(
    *,
    db_path: str | Path = "data/stock_data.duckdb",
    trade_date: Optional[str] = None,
    only_symbols: Optional[List[str]] = None,
    allow_latest_if_date_missing: bool = True,
) -> Dict[str, Dict[str, Any]]:
    p = Path(db_path)
    if not p.exists():
        return {}

    target_date = trade_date or dt.date.today().isoformat()

    std_filter = None
    if only_symbols:
        std_filter = set()
        for s in only_symbols:
            try:
                std = normalize_symbol(s)
                code = std.split(".")[0]
                std_filter.update({std, code, std.replace(".", ""), "sh" + code, "sz" + code, "bj" + code})
            except Exception:
                std_filter.add(str(s))

    try:
        con = _connect(p)
        cols = [x[1] for x in con.execute("PRAGMA table_info('realtime_quote')").fetchall()]
        has_date = "date" in cols

        used_date = None
        if has_date:
            rows = con.execute("SELECT * FROM realtime_quote WHERE date = ?", (target_date,)).fetchdf()
            used_date = target_date
            if len(rows) == 0 and allow_latest_if_date_missing:
                latest = con.execute("SELECT MAX(date) FROM realtime_quote").fetchone()[0]
                used_date = parse_date(latest)
                rows = con.execute("SELECT * FROM realtime_quote WHERE date = ?", (latest,)).fetchdf()
        else:
            rows = con.execute("SELECT * FROM realtime_quote").fetchdf()
            used_date = None

        con.close()

        if rows is None or len(rows) == 0:
            return {}

        rows = rows.where(rows.notna(), None)
        out: Dict[str, Dict[str, Any]] = {}

        for r in rows.to_dict("records"):
            raw_symbol = r.get("symbol") or r.get("code")
            if not raw_symbol:
                continue

            try:
                std = normalize_symbol(raw_symbol)
            except Exception:
                try:
                    std = normalize_symbol(r.get("code"))
                except Exception:
                    continue

            code = std.split(".")[0]
            keys = {std, code, std.replace(".", ""), "sh" + code, "sz" + code, "bj" + code}
            if std_filter and not (keys & std_filter):
                continue

            q_date = parse_date(r.get("date")) or used_date
            fresh = bool(q_date == target_date)

            price = r.get("price")
            quote = {
                "symbol": std,
                "code": code,
                "name": r.get("name"),
                "open": r.get("open"),
                "pre_close": r.get("pre_close"),
                "price": price,
                "current_price": price,
                "high": r.get("high"),
                "low": r.get("low"),
                "volume": r.get("volume"),
                "amount": r.get("amount"),
                "pct_chg": r.get("pct_chg"),
                "change_percent": r.get("pct_chg"),
                "quote_date": q_date,
                "quote_time": safe_str(r.get("time")),
                "source": r.get("source"),
                "quote_source": r.get("source"),
                "updated_at": safe_str(r.get("updated_at")),
                "fresh_quote": fresh,
                "is_fresh": fresh,
                "used_quote_date": used_date,
            }

            out[std] = quote
            out[code] = quote

        return out

    except Exception:
        return {}


def quote_map_report(quote_map: Dict[str, Dict[str, Any]], symbols: List[str]) -> Dict[str, Any]:
    total = len(symbols)
    matched = 0
    fresh = 0
    missing = []
    dates = set()
    sources = set()

    for s in symbols:
        try:
            std = normalize_symbol(s)
            code = std.split(".")[0]
        except Exception:
            std = str(s)
            code = str(s)

        q = quote_map.get(std) or quote_map.get(code)
        if q:
            matched += 1
            if q.get("fresh_quote"):
                fresh += 1
            if q.get("quote_date"):
                dates.add(str(q.get("quote_date")))
            if q.get("source"):
                sources.add(str(q.get("source")))
        else:
            missing.append(std)

    return {
        "total_symbols": total,
        "matched": matched,
        "matched_ratio": round(matched / max(total, 1), 4),
        "fresh": fresh,
        "fresh_ratio": round(fresh / max(total, 1), 4),
        "quote_dates": sorted(dates, reverse=True)[:5],
        "sources": sorted(sources),
        "missing_sample": missing[:20],
    }
