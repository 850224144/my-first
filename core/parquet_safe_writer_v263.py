"""
v2.6.3 Parquet 安全写入工具。

解决：
pyarrow.lib.ArrowInvalid: cannot mix list and non-list, non-null values

策略：
- 写 parquet 前统一字段类型
- list/dict/tuple 全部 JSON 字符串化
- numpy/pandas scalar 转 Python 基础类型
- datetime/date/time 转 isoformat
- None 保持 None
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import datetime as dt
import json
import math


COMPLEX_FIELD_HINTS = {
    "risk_flags",
    "blocking_flags",
    "downgrade_flags",
    "signal_reasons",
    "upgrade_reasons",
    "sector_reasons",
    "yuanjun_reasons",
    "weekly_flags",
    "weekly_reasons",
    "sector_flags",
    "yuanjun_flags",
    "risk_reasons",
    "xgb_pools",
    "raw_json",
    "paper_trade_record",
    "open_recheck_plan",
    "stop_loss_diagnosis",
    "compressible_proposals",
    "proposals",
}


def to_builtin_v263(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()

    # numpy scalar
    try:
        if hasattr(value, "item") and callable(value.item):
            return to_builtin_v263(value.item())
    except Exception:
        pass

    # numpy array / pandas array
    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            return to_builtin_v263(value.tolist())
    except Exception:
        pass

    if isinstance(value, tuple):
        return [to_builtin_v263(x) for x in value]

    if isinstance(value, list):
        return [to_builtin_v263(x) for x in value]

    if isinstance(value, dict):
        return {str(k): to_builtin_v263(v) for k, v in value.items()}

    return str(value)


def json_string_v263(value: Any) -> str | None:
    value = to_builtin_v263(value)
    if value is None:
        return None

    # 字符串如果看起来已经是普通文本，就直接保留；
    # 如果是 list/dict 的字符串，不强行解析，避免误处理。
    if isinstance(value, str):
        return value

    return json.dumps(value, ensure_ascii=False, default=str)


def normalize_record_for_parquet_v263(record: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (record or {}).items():
        key = str(k)
        b = to_builtin_v263(v)

        # 明确复杂字段全部 JSON 字符串化
        if key in COMPLEX_FIELD_HINTS:
            out[key] = json_string_v263(b)
            continue

        # 其他字段如果是 list/dict，也统一 JSON 字符串化
        if isinstance(b, (list, dict, tuple)):
            out[key] = json_string_v263(b)
            continue

        out[key] = b

    return out


def normalize_records_for_parquet_v263(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [normalize_record_for_parquet_v263(x) for x in (records or [])]


def write_parquet_safe_v263(records: List[Dict[str, Any]], path: str | Path) -> None:
    import pandas as pd

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    safe_records = normalize_records_for_parquet_v263(records)
    df = pd.DataFrame(safe_records)

    # 对 object 列再做一层兜底：
    # 只要发现 list/dict，整列转 JSON/字符串，避免 pyarrow 混列。
    for col in df.columns:
        if str(df[col].dtype) == "object":
            has_complex = False
            for v in df[col].dropna().head(200).tolist():
                if isinstance(v, (list, dict, tuple)):
                    has_complex = True
                    break
            if has_complex:
                df[col] = df[col].apply(json_string_v263)

    df.to_parquet(p, index=False)


def read_parquet_safe_v263(path: str | Path) -> List[Dict[str, Any]]:
    import pandas as pd

    p = Path(path)
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    if df.empty:
        return []
    df = df.where(df.notna(), None)
    return df.to_dict("records")
