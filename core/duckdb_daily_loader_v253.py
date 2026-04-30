"""
v2.5.3 DuckDB 日 K 加载器。

目标：
- 尝试从 data/stock_data.duckdb 加载个股日 K
- 不强依赖具体表名；先 inspect，再尝试常见表/字段
- 失败时返回空，不阻塞主流程
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from .data_normalizer import normalize_symbol, to_plain_code
except Exception:
    def normalize_symbol(x): return str(x)
    def to_plain_code(x): return str(x).split(".")[0]


COMMON_TABLES = [
    "daily_kline",
    "stock_daily",
    "kline_daily",
    "daily",
    "bars",
    "a_stock_daily",
    "stock_bars",
]


def _connect(db_path: str | Path):
    try:
        import duckdb
    except Exception as exc:
        raise RuntimeError("未安装 duckdb，无法读取 stock_data.duckdb") from exc
    return duckdb.connect(str(db_path), read_only=True)


def inspect_duckdb(db_path: str | Path = "data/stock_data.duckdb") -> Dict[str, Any]:
    p = Path(db_path)
    if not p.exists():
        return {"exists": False, "path": str(p), "tables": []}

    try:
        con = _connect(p)
        rows = con.execute("SHOW TABLES").fetchall()
        tables = [x[0] for x in rows]
        info = {"exists": True, "path": str(p), "tables": tables, "columns": {}}
        for t in tables:
            try:
                cols = con.execute(f"PRAGMA table_info('{t}')").fetchall()
                info["columns"][t] = [x[1] for x in cols]
            except Exception:
                info["columns"][t] = []
        con.close()
        return info
    except Exception as exc:
        return {"exists": True, "path": str(p), "tables": [], "error": str(exc)}


def _pick_table(info: Dict[str, Any]) -> Optional[str]:
    tables = info.get("tables") or []
    for t in COMMON_TABLES:
        if t in tables:
            return t
    # 挑一个包含 code/symbol + close 的表
    for t, cols in (info.get("columns") or {}).items():
        lower = {c.lower() for c in cols}
        if ("code" in lower or "symbol" in lower or "ts_code" in lower) and "close" in lower:
            return t
    return tables[0] if tables else None


def load_daily_bars_from_duckdb(
    symbol: str,
    *,
    db_path: str | Path = "data/stock_data.duckdb",
    limit: int = 260,
    table: Optional[str] = None,
) -> List[Dict[str, Any]]:
    p = Path(db_path)
    if not p.exists():
        return []

    info = inspect_duckdb(p)
    if info.get("error"):
        return []

    t = table or _pick_table(info)
    if not t:
        return []

    cols = info.get("columns", {}).get(t, [])
    lower_map = {c.lower(): c for c in cols}

    code_col = None
    for k in ["symbol", "code", "ts_code", "stock_code"]:
        if k in lower_map:
            code_col = lower_map[k]
            break

    date_col = None
    for k in ["trade_date", "date", "datetime", "dt"]:
        if k in lower_map:
            date_col = lower_map[k]
            break

    if not code_col or not date_col:
        return []

    std = normalize_symbol(symbol)
    plain = to_plain_code(std)
    candidates = [std, plain, std.replace(".", ""), plain + ".SH", plain + ".SZ", "sh" + plain, "sz" + plain]

    try:
        con = _connect(p)
        placeholders = ",".join(["?"] * len(candidates))
        sql = f"""
            SELECT *
            FROM {t}
            WHERE {code_col} IN ({placeholders})
            ORDER BY {date_col} DESC
            LIMIT ?
        """
        rows = con.execute(sql, [*candidates, int(limit)]).fetchdf()
        con.close()
        if rows is None or len(rows) == 0:
            return []
        rows = rows.sort_values(by=date_col)
        rows = rows.where(rows.notna(), None)
        return rows.to_dict("records")
    except Exception:
        return []
