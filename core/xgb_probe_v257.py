"""
v2.5.7 选股宝 core_pools 探测工具。

目的：
- 不是直接替代 xgb_client
- 是诊断为什么 /api/pool/detail 返回空
- 尝试不同日期格式、参数名、pool 名称
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import datetime as dt
import requests


BASE_URL = "https://flash-api.xuangubao.cn"

POOL_NAMES = [
    "limit_up",
    "continuous_limit_up",
    "strong_stock",
    "yesterday_limit_up",
    "limit_up_broken",
    "limit_down",
]

PATHS = [
    "/api/pool/detail",
]

PARAM_KEYS = [
    "pool_name",
    "pool",
    "type",
]


def _date_formats(date_str: str) -> List[Optional[str]]:
    d = date_str
    compact = d.replace("-", "")
    return [d, compact, None]


def _extract_data(payload: Any) -> Any:
    if isinstance(payload, dict):
        if "data" in payload:
            return payload["data"]
        return payload
    return payload


def _count_data(data: Any) -> int:
    if data is None:
        return 0
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        for key in ["items", "list", "data", "rows", "pool"]:
            v = data.get(key)
            if isinstance(v, list):
                return len(v)
        return len(data)
    return 0


def _shape(data: Any) -> Dict[str, Any]:
    if data is None:
        return {"type": "None"}
    if isinstance(data, list):
        first = data[0] if data else None
        return {
            "type": "list",
            "len": len(data),
            "first_keys": list(first.keys())[:20] if isinstance(first, dict) else None,
            "first_sample": first if isinstance(first, (str, int, float)) else None,
        }
    if isinstance(data, dict):
        return {
            "type": "dict",
            "keys": list(data.keys())[:30],
        }
    return {
        "type": type(data).__name__,
        "repr": str(data)[:300],
    }


def request_xgb(path: str, params: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
    url = BASE_URL.rstrip("/") + path
    headers = {
        "User-Agent": "Mozilla/5.0 AStockSecondBuyAssistant/2.5.7",
        "Accept": "application/json,text/plain,*/*",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        status_code = r.status_code
        text = r.text[:1000]
        try:
            payload = r.json()
        except Exception:
            payload = None

        data = _extract_data(payload)
        return {
            "ok": status_code == 200,
            "status_code": status_code,
            "url": r.url,
            "code": payload.get("code") if isinstance(payload, dict) else None,
            "msg": payload.get("msg") if isinstance(payload, dict) else None,
            "count": _count_data(data),
            "shape": _shape(data),
            "text_sample": text,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "path": path,
            "params": params,
            "count": 0,
        }


def probe_xgb_pool_variants(
    *,
    trade_date: Optional[str] = None,
    pools: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    trade_date = trade_date or dt.date.today().isoformat()
    pools = pools or POOL_NAMES

    results: List[Dict[str, Any]] = []
    for pool_name in pools:
        for path in PATHS:
            for param_key in PARAM_KEYS:
                for date_value in _date_formats(trade_date):
                    params = {param_key: pool_name}
                    if date_value is not None:
                        params["date"] = date_value

                    res = request_xgb(path, params)
                    res.update({
                        "pool_name": pool_name,
                        "path": path,
                        "param_key": param_key,
                        "date_value": date_value,
                    })
                    results.append(res)
    return results


def summarize_probe(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    non_empty = [r for r in results if int(r.get("count") or 0) > 0]
    errors = [r for r in results if r.get("error") or not r.get("ok")]
    best = sorted(non_empty, key=lambda x: int(x.get("count") or 0), reverse=True)[:10]
    return {
        "total_requests": len(results),
        "non_empty_count": len(non_empty),
        "error_count": len(errors),
        "best": best,
    }
