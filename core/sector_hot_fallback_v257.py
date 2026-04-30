"""
v2.5.7 本地 sector_hot.parquet 板块兜底。

目标：
- 如果 XGB core_pools 为空，则从 data/sector_hot.parquet 尝试给候选补 theme_name / sector_score
- 只做板块兜底，不伪造 leader_score / yuanjun_score
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import re

try:
    from .data_normalizer import normalize_symbol
except Exception:
    def normalize_symbol(x): return str(x)


THEME_COLS = ["theme_name", "sector_name", "sector", "name", "板块", "概念", "industry", "行业"]
SCORE_COLS = ["theme_heat_score", "sector_score", "hot_score", "score", "rank_score", "heat", "涨幅", "pct_chg"]
SYMBOL_COLS = ["symbol", "code", "stock_code", "ts_code"]
MEMBER_COLS = ["symbols", "codes", "stocks", "members", "constituents", "成分股", "成分"]


def _import_pandas():
    import pandas as pd
    return pd


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def inspect_sector_hot(path: str | Path = "data/sector_hot.parquet", sample_rows: int = 5) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"exists": False, "path": str(p)}
    pd = _import_pandas()
    df = pd.read_parquet(p)
    sample = df.head(sample_rows).where(df.head(sample_rows).notna(), None).to_dict("records")
    return {
        "exists": True,
        "path": str(p),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "sample": sample,
        "guessed_theme_cols": [c for c in df.columns if c in THEME_COLS],
        "guessed_score_cols": [c for c in df.columns if c in SCORE_COLS],
        "guessed_symbol_cols": [c for c in df.columns if c in SYMBOL_COLS],
        "guessed_member_cols": [c for c in df.columns if c in MEMBER_COLS],
    }


def _contains_symbol(member_value: Any, symbol: str) -> bool:
    if member_value is None:
        return False
    try:
        std = normalize_symbol(symbol)
        code = std.split(".")[0]
    except Exception:
        std = str(symbol)
        code = str(symbol)

    if isinstance(member_value, list):
        text = ",".join(str(x) for x in member_value)
    elif isinstance(member_value, dict):
        text = json.dumps(member_value, ensure_ascii=False)
    else:
        text = str(member_value)

    return std in text or code in text or std.replace(".", "") in text


def _row_theme(row: Dict[str, Any]) -> Optional[str]:
    for c in THEME_COLS:
        if row.get(c):
            return str(row.get(c))
    return None


def _row_score(row: Dict[str, Any]) -> Optional[float]:
    for c in SCORE_COLS:
        if row.get(c) is not None:
            v = _f(row.get(c))
            if v is not None:
                # 如果像涨幅 3.2，则转成 50+涨幅*5 的热度，封顶 85
                if c in {"涨幅", "pct_chg"}:
                    return max(45, min(85, 50 + v * 5))
                # 如果已经是 0-100
                if 0 <= v <= 100:
                    return v
                # 如果排名分/热度较大，压缩
                return max(45, min(85, v))
    return None


def _direct_symbol_match(row: Dict[str, Any], symbol: str) -> bool:
    try:
        std = normalize_symbol(symbol)
        code = std.split(".")[0]
    except Exception:
        std = str(symbol)
        code = str(symbol)

    for c in SYMBOL_COLS:
        if row.get(c):
            try:
                if normalize_symbol(row.get(c)) == std:
                    return True
            except Exception:
                if str(row.get(c)) in {std, code}:
                    return True
    return False


def lookup_sector_hot_for_symbol(
    symbol: str,
    *,
    path: str | Path = "data/sector_hot.parquet",
) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {
            "matched": False,
            "sector_score": 50.0,
            "theme_name": None,
            "sector_flags": ["本地 sector_hot 不存在(sector_hot_missing)"],
        }

    pd = _import_pandas()
    df = pd.read_parquet(p)
    if df.empty:
        return {
            "matched": False,
            "sector_score": 50.0,
            "theme_name": None,
            "sector_flags": ["本地 sector_hot 为空(sector_hot_empty)"],
        }

    df = df.where(df.notna(), None)
    matches: List[Dict[str, Any]] = []

    for row in df.to_dict("records"):
        matched = _direct_symbol_match(row, symbol)
        if not matched:
            for c in MEMBER_COLS:
                if c in row and _contains_symbol(row.get(c), symbol):
                    matched = True
                    break
        if matched:
            score = _row_score(row)
            theme = _row_theme(row)
            matches.append({
                "theme_name": theme,
                "sector_score": score if score is not None else 55.0,
                "row": row,
            })

    if not matches:
        return {
            "matched": False,
            "sector_score": 50.0,
            "theme_name": None,
            "sector_flags": ["本地 sector_hot 未匹配到该股票(sector_hot_no_match)"],
        }

    best = sorted(matches, key=lambda x: float(x.get("sector_score") or 50), reverse=True)[0]
    return {
        "matched": True,
        "sector_score": float(best.get("sector_score") or 55),
        "theme_name": best.get("theme_name"),
        "sector_state": "sector_hot_fallback",
        "sector_flags": ["使用本地 sector_hot 兜底(sector_hot_fallback)"],
        "sector_reasons": [f"本地 sector_hot 匹配：{best.get('theme_name') or '-'}"],
    }


def apply_sector_hot_fallback(
    candidate: Dict[str, Any],
    *,
    path: str | Path = "data/sector_hot.parquet",
) -> Dict[str, Any]:
    out = dict(candidate)
    # 只有 sector 默认/缺失时才兜底
    if out.get("sector_score") not in (None, 50, 50.0):
        return out

    res = lookup_sector_hot_for_symbol(out.get("symbol") or out.get("code"), path=path)
    if res.get("matched"):
        out["sector_score"] = res.get("sector_score")
        out["theme_name"] = out.get("theme_name") or res.get("theme_name")
        out["sector_state"] = res.get("sector_state")
        out.setdefault("leader_score", 50.0)
        out.setdefault("leader_type", "unknown")
        # 不伪造 leader/yuanjun
        flags = list(out.get("risk_flags") or [])
        flags.extend(res.get("sector_flags") or [])
        out["risk_flags"] = list(dict.fromkeys(flags))
        reasons = list(out.get("sector_reasons") or [])
        reasons.extend(res.get("sector_reasons") or [])
        out["sector_reasons"] = list(dict.fromkeys(reasons))
    else:
        flags = list(out.get("risk_flags") or [])
        flags.extend(res.get("sector_flags") or [])
        out["risk_flags"] = list(dict.fromkeys(flags))

    return out
