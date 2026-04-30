"""
选股宝 HTTP 客户端。

职责：
- 只负责请求选股宝接口
- 不负责策略判断
- 不混用 AkShare / 腾讯 / 新浪

失败时抛异常或返回空列表，由 xgb_cache 决定是否读缓存降级。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional
import time
import requests


BASE_URL = "https://flash-api.xuangubao.cn"


class XGBError(RuntimeError):
    pass


@dataclass
class XGBClient:
    base_url: str = BASE_URL
    timeout: int = 10
    retries: int = 3
    backoff_seconds: List[float] = field(default_factory=lambda: [1, 2, 5])
    session: Optional[requests.Session] = None

    def _session(self) -> requests.Session:
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 AStockSecondBuyAssistant/2.4.0",
                "Accept": "application/json,text/plain,*/*",
            })
        return self.session

    def request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        last_exc: Optional[Exception] = None

        for idx in range(max(1, self.retries)):
            try:
                resp = self._session().get(url, params=params or {}, timeout=self.timeout)
                resp.raise_for_status()
                payload = resp.json()
                if isinstance(payload, dict):
                    code = payload.get("code")
                    # 选股宝常见 code=20000
                    if code not in (None, 20000):
                        raise XGBError(f"XGB code={code}, msg={payload.get('msg')}, url={url}")
                    return payload.get("data", [])
                return payload
            except Exception as exc:
                last_exc = exc
                if idx < self.retries - 1:
                    delay = self.backoff_seconds[min(idx, len(self.backoff_seconds) - 1)]
                    time.sleep(delay)

        raise XGBError(f"XGB request failed: {url}, params={params}, err={last_exc}")

    def get_market_indicator(self, date: Optional[str] = None) -> Any:
        fields = ",".join([
            "rise_count",
            "fall_count",
            "flat_count",
            "limit_up_count",
            "limit_down_count",
            "limit_up_broken_count",
            "limit_up_broken_ratio",
        ])
        params: Dict[str, Any] = {"fields": fields}
        if date:
            params["date"] = date
        return self.request("/api/market_indicator/line", params=params)

    def get_pool(self, pool_name: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"pool_name": pool_name}
        if date:
            params["date"] = date
        data = self.request("/api/pool/detail", params=params)
        return data if isinstance(data, list) else []

    def get_limit_up(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("limit_up", date=date)

    def get_continuous_limit_up(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("continuous_limit_up", date=date)

    def get_strong_stock(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("strong_stock", date=date)

    def get_yesterday_limit_up(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("yesterday_limit_up", date=date)

    def get_limit_up_broken(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("limit_up_broken", date=date)

    def get_limit_down(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.get_pool("limit_down", date=date)

    def get_stock_data(self, symbols: Iterable[str]) -> List[Dict[str, Any]]:
        symbols_list = list(symbols)
        out: List[Dict[str, Any]] = []
        fields = ",".join([
            "symbol",
            "stock_chi_name",
            "price",
            "change_percent",
            "open",
            "high",
            "low",
            "volume",
            "amount",
            "turnover_ratio",
            "circulating_market_cap",
            "total_market_cap",
        ])

        for i in range(0, len(symbols_list), 20):
            batch = symbols_list[i:i + 20]
            if not batch:
                continue
            params = {"symbols": ",".join(batch), "fields": fields}
            data = self.request("/api/stock/data", params=params)
            if isinstance(data, list):
                out.extend(data)
        return out
