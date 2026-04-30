"""
v2.5.8 选股宝 live pool 获取器。

关键修复：
- /api/pool/detail 不传 date 参数
- 之前带 date 会返回 0
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt
import json
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


def _safe_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _safe_json_read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def extract_data(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
    else:
        data = payload
    return data if isinstance(data, list) else []


def fetch_xgb_live_pool(pool_name: str, *, param_key: str = "pool_name", timeout: int = 12) -> Dict[str, Any]:
    """
    不传 date。
    """
    url = BASE_URL.rstrip("/") + "/api/pool/detail"
    params = {param_key: pool_name}
    headers = {
        "User-Agent": "Mozilla/5.0 AStockSecondBuyAssistant/2.5.8",
        "Accept": "application/json,text/plain,*/*",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        try:
            payload = r.json()
        except Exception:
            payload = None
        data = extract_data(payload)
        return {
            "ok": r.status_code == 200 and isinstance(payload, dict),
            "status_code": r.status_code,
            "url": r.url,
            "pool_name": pool_name,
            "param_key": param_key,
            "code": payload.get("code") if isinstance(payload, dict) else None,
            "message": payload.get("message") if isinstance(payload, dict) else None,
            "count": len(data),
            "data": data,
            "raw": payload,
            "error": None,
            "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "pool_name": pool_name,
            "param_key": param_key,
            "code": None,
            "message": None,
            "count": 0,
            "data": [],
            "raw": None,
            "error": str(exc),
            "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
        }


def fetch_and_cache_xgb_live_pools_v258(
    *,
    trade_date: Optional[str] = None,
    cache_root: str | Path = "data/xgb",
    param_key: str = "pool_name",
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    root = Path(cache_root) / "live_pools" / trade_date
    root.mkdir(parents=True, exist_ok=True)

    pools: Dict[str, Any] = {}
    summary: Dict[str, Any] = {}

    for pool in POOL_NAMES:
        res = fetch_xgb_live_pool(pool, param_key=param_key)
        pools[pool] = res.get("data") or []
        summary[pool] = {
            "count": res.get("count", 0),
            "ok": res.get("ok"),
            "code": res.get("code"),
            "error": res.get("error"),
            "url": res.get("url"),
        }
        _safe_json_write(root / f"{pool}.json", {
            "meta": summary[pool],
            "data": pools[pool],
        })

    meta = {
        "trade_date": trade_date,
        "cache_root": str(root),
        "param_key": param_key,
        "fetched_at": dt.datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
    }
    _safe_json_write(root / "_meta.json", meta)

    pools["_meta"] = meta
    return pools


def load_xgb_live_pools_v258(
    *,
    trade_date: Optional[str] = None,
    cache_root: str | Path = "data/xgb",
    fetch_if_empty: bool = False,
    param_key: str = "pool_name",
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    root = Path(cache_root) / "live_pools" / trade_date

    pools: Dict[str, Any] = {}
    loaded = {}
    missing = []

    for pool in POOL_NAMES:
        p = root / f"{pool}.json"
        if p.exists():
            try:
                payload = _safe_json_read(p)
                data = payload.get("data") if isinstance(payload, dict) else payload
                data = data if isinstance(data, list) else []
                pools[pool] = data
                if data:
                    loaded[pool] = len(data)
                else:
                    missing.append(pool)
            except Exception:
                pools[pool] = []
                missing.append(pool)
        else:
            pools[pool] = []
            missing.append(pool)

    if fetch_if_empty and not any(pools.get(p) for p in POOL_NAMES):
        return fetch_and_cache_xgb_live_pools_v258(
            trade_date=trade_date,
            cache_root=cache_root,
            param_key=param_key,
        )

    meta_path = root / "_meta.json"
    if meta_path.exists():
        try:
            meta = _safe_json_read(meta_path)
        except Exception:
            meta = {}
    else:
        meta = {}

    pools["_meta"] = {
        "trade_date": trade_date,
        "cache_root": str(root),
        "loaded_pools": loaded,
        "missing_pools": missing,
        "has_any_pool_data": any(bool(pools.get(p)) for p in POOL_NAMES),
        "cached_meta": meta,
    }
    return pools


def live_pools_report(pools: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not pools:
        return {
            "exists": False,
            "has_any_pool_data": False,
            "loaded_pools": {},
            "missing_pools": POOL_NAMES,
        }
    loaded = {}
    missing = []
    for pool in POOL_NAMES:
        data = pools.get(pool) or []
        if data:
            loaded[pool] = len(data)
        else:
            missing.append(pool)
    meta = pools.get("_meta") or {}
    return {
        "exists": True,
        "trade_date": meta.get("trade_date"),
        "has_any_pool_data": any(loaded.values()),
        "loaded_pools": loaded,
        "missing_pools": missing,
    }
