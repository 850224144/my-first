"""
v2.6.1 Tail Focus 加载器。
tail_confirm 后续可优先读取 data/watchlist_tail_focus.parquet。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


def _pd():
    import pandas as pd
    return pd


def load_tail_focus_v261(path: str | Path = "data/watchlist_tail_focus.parquet") -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    pd = _pd()
    df = pd.read_parquet(p)
    if df.empty:
        return []
    df = df.where(df.notna(), None)
    return df.to_dict("records")


def tail_focus_symbols_v261(path: str | Path = "data/watchlist_tail_focus.parquet") -> List[str]:
    rows = load_tail_focus_v261(path)
    out = []
    for r in rows:
        s = r.get("symbol") or r.get("code")
        if s:
            out.append(str(s))
    return out


def filter_candidates_by_tail_focus_v261(
    candidates: List[Dict[str, Any]],
    *,
    tail_focus_path: str | Path = "data/watchlist_tail_focus.parquet",
) -> List[Dict[str, Any]]:
    focus = set(tail_focus_symbols_v261(tail_focus_path))
    if not focus:
        return []
    out = []
    for c in candidates:
        s = c.get("symbol") or c.get("code")
        if s and str(s) in focus:
            out.append(c)
    return out
