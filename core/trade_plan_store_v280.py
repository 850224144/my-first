"""
v2.8.0 Parquet 存储工具：追加 + 去重。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json

from .parquet_safe_writer_v263 import write_parquet_safe_v263, read_parquet_safe_v263


def dedupe_rows_v280(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        k = tuple(str(r.get(x) or "") for x in keys)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def append_parquet_dedup_v280(
    *,
    path: str | Path,
    new_rows: List[Dict[str, Any]],
    dedupe_keys: List[str],
) -> List[Dict[str, Any]]:
    existing = read_parquet_safe_v263(path)
    all_rows = existing + (new_rows or [])
    final_rows = dedupe_rows_v280(all_rows, dedupe_keys)
    write_parquet_safe_v263(final_rows, path)
    return final_rows
