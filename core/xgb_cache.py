"""
选股宝文件缓存。

目标：
- 请求成功：写入 data/xgb/
- 请求失败：读取最近缓存，并标记 stale=True
- 不阻塞主流程
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import datetime as dt
import json

from .xgb_client import XGBClient


def today_str() -> str:
    return dt.date.today().isoformat()


class XGBCache:
    def __init__(self, root: str | Path = "data/xgb"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _write_json(self, path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
            "data": data,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_json(self, path: Path) -> Optional[Any]:
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and "data" in payload:
                return payload["data"]
            return payload
        except Exception:
            return None

    def market_indicator_path(self, trade_date: str) -> Path:
        return self.root / "market_indicator" / f"{trade_date}.json"

    def pool_path(self, trade_date: str, pool_name: str) -> Path:
        return self.root / "pools" / trade_date / f"{pool_name}.json"

    def stock_data_path(self, trade_date: str, key: str) -> Path:
        return self.root / "stock_data" / trade_date / f"{key}.json"

    def get_or_fetch(
        self,
        path: Path,
        fetcher: Callable[[], Any],
        *,
        allow_stale: bool = True,
    ) -> Tuple[Any, bool, Optional[str]]:
        """
        返回：data, stale, error
        """
        try:
            data = fetcher()
            self._write_json(path, data)
            return data, False, None
        except Exception as exc:
            if allow_stale:
                cached = self._read_json(path)
                if cached is not None:
                    return cached, True, str(exc)
            return None, False, str(exc)

    def get_market_indicator(
        self,
        client: XGBClient,
        trade_date: Optional[str] = None,
        *,
        allow_stale: bool = True,
    ) -> Tuple[Any, bool, Optional[str]]:
        d = trade_date or today_str()
        return self.get_or_fetch(
            self.market_indicator_path(d),
            lambda: client.get_market_indicator(date=d),
            allow_stale=allow_stale,
        )

    def get_pool(
        self,
        client: XGBClient,
        pool_name: str,
        trade_date: Optional[str] = None,
        *,
        allow_stale: bool = True,
    ) -> Tuple[List[Dict[str, Any]], bool, Optional[str]]:
        d = trade_date or today_str()
        data, stale, err = self.get_or_fetch(
            self.pool_path(d, pool_name),
            lambda: client.get_pool(pool_name, date=d),
            allow_stale=allow_stale,
        )
        return (data or []), stale, err

    def get_core_pools(
        self,
        client: XGBClient,
        trade_date: Optional[str] = None,
        *,
        allow_stale: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        返回结构：
        {
          "limit_up": {"data": [...], "stale": false, "error": null},
          ...
        }
        """
        pools = [
            "limit_up",
            "continuous_limit_up",
            "strong_stock",
            "yesterday_limit_up",
            "limit_up_broken",
            "limit_down",
        ]
        out: Dict[str, Dict[str, Any]] = {}
        for p in pools:
            data, stale, err = self.get_pool(client, p, trade_date=trade_date, allow_stale=allow_stale)
            out[p] = {"data": data, "stale": stale, "error": err}
        return out
