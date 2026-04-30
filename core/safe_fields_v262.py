"""
v2.6.2 安全字段清洗。

解决：
- parquet 中 list/ndarray 字段显示成 ['A' 'B']
- 企业微信/日报原因字段难读
- JSON/parquet 写入时遇到 numpy/pandas 类型不稳定
"""

from __future__ import annotations

from typing import Any, Dict, List
import json
import datetime as dt


def to_builtin(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()

    # numpy scalar
    try:
        if hasattr(value, "item") and callable(value.item):
            return value.item()
    except Exception:
        pass

    # numpy array / pandas array
    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            return to_builtin(value.tolist())
    except Exception:
        pass

    if isinstance(value, tuple):
        return [to_builtin(x) for x in value]

    if isinstance(value, list):
        return [to_builtin(x) for x in value]

    if isinstance(value, dict):
        return {str(k): to_builtin(v) for k, v in value.items()}

    return str(value)


def normalize_list_field(value: Any) -> List[str]:
    value = to_builtin(value)

    if value is None:
        return []

    if isinstance(value, list):
        out = []
        for x in value:
            if x is None:
                continue
            s = str(x).strip()
            if s and s.lower() not in {"nan", "none", "null", "[]"}:
                out.append(s)
        return list(dict.fromkeys(out))

    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in {"nan", "none", "null", "[]"}:
            return []

        # 尝试 JSON
        try:
            obj = json.loads(s)
            return normalize_list_field(obj)
        except Exception:
            pass

        # 处理 numpy array 字符串：['A' 'B']
        s2 = s.strip("[]")
        if "'" in s2 or '"' in s2:
            parts = []
            cur = ""
            in_quote = False
            quote = ""
            for ch in s2:
                if ch in {"'", '"'}:
                    if not in_quote:
                        in_quote = True
                        quote = ch
                        cur = ""
                    elif ch == quote:
                        in_quote = False
                        if cur.strip():
                            parts.append(cur.strip())
                        cur = ""
                elif in_quote:
                    cur += ch
            if parts:
                return list(dict.fromkeys(parts))

        # 普通分隔
        for sep in ["；", ";", "，", ",", "|"]:
            if sep in s:
                return list(dict.fromkeys(x.strip() for x in s.split(sep) if x.strip()))

        return [s]

    return [str(value)]


def clean_record_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    out = {str(k): to_builtin(v) for k, v in (record or {}).items()}

    list_keys = [
        "risk_reasons",
        "risk_flags",
        "blocking_flags",
        "downgrade_flags",
        "signal_reasons",
        "upgrade_reasons",
        "sector_reasons",
        "yuanjun_reasons",
        "xgb_pools",
    ]
    for k in list_keys:
        if k in out:
            out[k] = normalize_list_field(out.get(k))

    return out


def stringify_list(value: Any, sep: str = "，") -> str:
    return sep.join(normalize_list_field(value))


def json_dumps_safe(obj: Any) -> str:
    return json.dumps(to_builtin(obj), ensure_ascii=False, indent=2, default=str)
