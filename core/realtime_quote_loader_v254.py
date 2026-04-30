"""
v2.5.4 DuckDB realtime_quote 加载器。

目标：
- 从 data/stock_data.duckdb 的 realtime_quote 表读取实时价
- 构造 quote_map，供 watchlist candidate 填 current_price / fresh_quote
- fresh_quote 根据 date 字段判断是否为目标交易日
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


def _parse_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    # 支持 20260430 / 2026-04-30 / 2026/04/30
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
        count = con.execute("SELECT COUNT(*) FROM realtime_quote").fetchone()[0]
        dates = []
        if "date" in cols:
            dates = [x[0] for x in con.execute("SELECT DISTINCT date FROM realtime_quote ORDER BY date DESC LIMIT 5").fetchall()]
        con.close()
        return {
            "exists": True,
            "path": str(p),
            "table_exists": True,
            "columns": cols,
            "rows": count,
            "recent_dates": dates,
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
    """
    返回：
    {
      "688183.SH": {
          "symbol": "688183.SH",
          "price": 120.7,
          "current_price": 120.7,
          "open": ...,
          "high": ...,
          "low": ...,
          "fresh_quote": true/false,
          "quote_date": "2026-04-30",
          "quote_time": "14:50:00",
          "source": "sina/tencent/..."
      }
    }
    """
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
                std_filter.add(std)
                std_filter.add(code)
                std_filter.add(std.replace(".", ""))
                std_filter.add("sh" + code)
                std_filter.add("sz" + code)
                std_filter.add("bj" + code)
            except Exception:
                std_filter.add(str(s))

    try:
        con = _connect(p)
        cols = [x[1] for x in con.execute("PRAGMA table_info('realtime_quote')").fetchall()]
        has_date = "date" in cols

        if has_date:
            rows = con.execute(
                """
                SELECT *
                FROM realtime_quote
                WHERE date = ?
                """,
                (target_date,),
            ).fetchdf()

            if len(rows) == 0 and allow_latest_if_date_missing:
                latest = con.execute("SELECT MAX(date) FROM realtime_quote").fetchone()[0]
                rows = con.execute("SELECT * FROM realtime_quote WHERE date = ?", (latest,)).fetchdf()
        else:
            rows = con.execute("SELECT * FROM realtime_quote").fetchdf()

        con.close()

        if rows is None or len(rows) == 0:
            return {}

        rows = rows.where(rows.notna(), None)
        out: Dict[str, Dict[str, Any]] = {}

        for r in rows.to_dict("records"):
            raw_symbol = r.get("symbol") or r.get("code")
            if not raw_symbol:
                continue

            # realtime_quote 里可能 symbol 已是 sh600519 或 600519.SH
            try:
                std = normalize_symbol(raw_symbol)
            except Exception:
                try:
                    std = normalize_symbol(r.get("code"))
                except Exception:
                    continue

            code = std.split(".")[0]
            if std_filter and not ({std, code, std.replace(".", ""), "sh"+code, "sz"+code, "bj"+code} & std_filter):
                continue

            q_date = _parse_date(r.get("date"))
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
                "quote_time": r.get("time"),
                "source": r.get("source"),
                "quote_source": r.get("source"),
                "updated_at": r.get("updated_at"),
                "fresh_quote": fresh,
                "is_fresh": fresh,
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
        else:
            missing.append(std)
    return {
        "total_symbols": total,
        "matched": matched,
        "matched_ratio": round(matched / max(total, 1), 4),
        "fresh": fresh,
        "fresh_ratio": round(fresh / max(total, 1), 4),
        "missing_sample": missing[:20],
    }
