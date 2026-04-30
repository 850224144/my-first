"""
v2.5.5 选股宝 core_pools 加载入口。

优先读取本地缓存：
data/xgb/pools/<trade_date>/<pool_name>.json

如果本地没有缓存，默认不联网抓取，避免 preview 阶段慢和不稳定。
需要联网时，显式 allow_fetch=True，并确保 core.xgb_client / core.xgb_cache 可用。

核心股池：
- limit_up
- continuous_limit_up
- strong_stock
- yesterday_limit_up
- limit_up_broken
- limit_down
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt
import json

CORE_POOLS = [
    "limit_up",
    "continuous_limit_up",
    "strong_stock",
    "yesterday_limit_up",
    "limit_up_broken",
    "limit_down",
]


def _read_cached_pool(root: str | Path, trade_date: str, pool_name: str) -> List[Dict[str, Any]]:
    p = Path(root) / "pools" / trade_date / f"{pool_name}.json"
    if not p.exists():
        return []
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "data" in payload:
            data = payload["data"]
        else:
            data = payload
        return data if isinstance(data, list) else []
    except Exception:
        return []


def load_xgb_core_pools_v255(
    *,
    trade_date: Optional[str] = None,
    cache_root: str | Path = "data/xgb",
    allow_fetch: bool = False,
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    out: Dict[str, Any] = {}
    loaded = {}
    missing = []

    for pool in CORE_POOLS:
        data = _read_cached_pool(cache_root, trade_date, pool)
        if data:
            out[pool] = data
            loaded[pool] = len(data)
        else:
            out[pool] = []
            missing.append(pool)

    if allow_fetch and missing:
        try:
            from .xgb_client import XGBClient
            from .xgb_cache import XGBCache
            client = XGBClient()
            cache = XGBCache(root=cache_root)
            fetched = cache.get_core_pools(client, trade_date=trade_date, allow_stale=True)
            for pool in CORE_POOLS:
                item = fetched.get(pool) or {}
                data = item.get("data") or []
                out[pool] = data
                loaded[pool] = len(data)
            missing = [p for p in CORE_POOLS if not out.get(p)]
        except Exception:
            pass

    out["_meta"] = {
        "trade_date": trade_date,
        "cache_root": str(cache_root),
        "loaded_pools": loaded,
        "missing_pools": missing,
        "has_any_pool_data": any(bool(out.get(p)) for p in CORE_POOLS),
    }
    return out


def core_pools_report(core_pools: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not core_pools:
        return {
            "exists": False,
            "has_any_pool_data": False,
            "loaded_pools": {},
            "missing_pools": CORE_POOLS,
        }

    meta = core_pools.get("_meta", {})
    loaded = {}
    missing = []
    for p in CORE_POOLS:
        data = core_pools.get(p) or []
        if data:
            loaded[p] = len(data)
        else:
            missing.append(p)

    return {
        "exists": True,
        "trade_date": meta.get("trade_date"),
        "has_any_pool_data": any(loaded.values()),
        "loaded_pools": loaded,
        "missing_pools": missing,
    }
