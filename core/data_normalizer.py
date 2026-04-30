"""
v2.4.0 数据标准化模块

内部统一标准：

- symbol: 600519.SH / 000001.SZ / 834765.BJ
- volume: 手
- amount: 元
- market_cap: 元
- turnover_ratio: 百分比数值，例如 12.5 表示 12.5%
- change_percent: 百分比数值，例如 5.2 表示 5.2%
- price/open/high/low/close: 元

注意：
选股宝接口文档里 stock/data 的 amount 通常是“万元”，pool 里的 circulating_market_cap 通常是“亿元”。
入库前必须转成元。
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import json
import math
import re


class NormalizeError(ValueError):
    pass


def _to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null", "--", "-"}:
        return default
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return default


def _to_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    f = _to_float(value, None)
    if f is None:
        return default
    return int(round(f))


def normalize_symbol(raw: Any) -> str:
    """
    统一股票代码格式。

    支持：
    - 600519
    - sh600519
    - 600519.SH
    - sz000001
    - 000001.SZ
    - 834765.BJ
    """
    if raw is None:
        raise NormalizeError("symbol is None")

    s = str(raw).strip().upper()
    if not s:
        raise NormalizeError("symbol is empty")

    s = s.replace("_", ".").replace("-", ".")
    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", s):
        return s

    m = re.fullmatch(r"(SH|SZ|BJ)(\d{6})", s)
    if m:
        return f"{m.group(2)}.{m.group(1)}"

    m = re.fullmatch(r"(\d{6})(SH|SZ|BJ)", s)
    if m:
        return f"{m.group(1)}.{m.group(2)}"

    m = re.fullmatch(r"\d{6}", s)
    if m:
        code = s
        if code.startswith(("60", "68", "90")):
            return f"{code}.SH"
        if code.startswith(("00", "30", "20")):
            return f"{code}.SZ"
        if code.startswith(("8", "4")):
            return f"{code}.BJ"
        return f"{code}.SZ"

    raise NormalizeError(f"unsupported symbol: {raw}")


def to_plain_code(symbol: str) -> str:
    return normalize_symbol(symbol).split(".")[0]


def to_sina_symbol(symbol: str) -> str:
    std = normalize_symbol(symbol)
    code, ex = std.split(".")
    return f"{ex.lower()}{code}"


def to_tencent_symbol(symbol: str) -> str:
    # 腾讯常用 sh600519 / sz000001
    return to_sina_symbol(symbol)


def normalize_percent(value: Any, *, input_type: str = "auto") -> Optional[float]:
    """
    返回百分比数值。
    例如：
    - 输入 5.2 -> 5.2
    - 输入 0.052 且 input_type='ratio' -> 5.2
    - input_type='auto' 时：绝对值 <= 1 且非 0，按比例处理。
    """
    v = _to_float(value, None)
    if v is None:
        return None
    if input_type == "ratio":
        return v * 100.0
    if input_type == "percent":
        return v
    if input_type == "auto":
        if 0 < abs(v) <= 1:
            return v * 100.0
        return v
    raise NormalizeError(f"unsupported percent input_type: {input_type}")


def normalize_volume_to_shou(value: Any, *, unit: str = "shou", source: Optional[str] = None) -> Optional[float]:
    """
    统一成交量为“手”。

    unit:
    - shou: 已经是手
    - gu/share: 股，除以 100
    - lot: 手
    - auto: 按 source 粗略判断

    source 默认规则：
    - xgb: 手
    - akshare: 通常为手
    - sina: 实时行情常见为股
    - tencent: 项目日 K 通常按手处理；如你本地腾讯解析为股，请显式传 unit='gu'
    """
    v = _to_float(value, None)
    if v is None:
        return None

    u = (unit or "shou").lower()
    src = (source or "").lower()

    if u == "auto":
        if src == "sina":
            u = "gu"
        elif src in {"xgb", "akshare", "tencent"}:
            u = "shou"
        else:
            u = "shou"

    if u in {"shou", "lot", "lots"}:
        return v
    if u in {"gu", "share", "shares"}:
        return v / 100.0

    raise NormalizeError(f"unsupported volume unit: {unit}")


def normalize_amount_to_yuan(value: Any, *, unit: str = "yuan", source: Optional[str] = None) -> Optional[float]:
    """
    统一成交额为“元”。

    unit:
    - yuan: 元
    - wan_yuan: 万元
    - yi_yuan: 亿元
    - auto: 按 source 粗略判断

    source 默认规则：
    - xgb: 万元
    - sina: 元
    - akshare: 多数行情函数为元
    - tencent: 通常按元处理；如本地解析不同，请显式指定
    """
    v = _to_float(value, None)
    if v is None:
        return None

    u = (unit or "yuan").lower()
    src = (source or "").lower()

    if u == "auto":
        if src == "xgb":
            u = "wan_yuan"
        else:
            u = "yuan"

    if u in {"yuan", "元"}:
        return v
    if u in {"wan_yuan", "万元", "wan"}:
        return v * 10_000.0
    if u in {"yi_yuan", "亿元", "yi"}:
        return v * 100_000_000.0

    raise NormalizeError(f"unsupported amount unit: {unit}")


def normalize_market_cap_to_yuan(value: Any, *, unit: str = "yuan", source: Optional[str] = None) -> Optional[float]:
    """
    统一市值为“元”。

    选股宝 pool/detail 中 circulating_market_cap 文档按“亿元”。
    """
    v = _to_float(value, None)
    if v is None:
        return None

    u = (unit or "yuan").lower()
    src = (source or "").lower()
    if u == "auto":
        if src == "xgb":
            u = "yi_yuan"
        else:
            u = "yuan"

    if u in {"yuan", "元"}:
        return v
    if u in {"wan_yuan", "万元", "wan"}:
        return v * 10_000.0
    if u in {"yi_yuan", "亿元", "yi"}:
        return v * 100_000_000.0

    raise NormalizeError(f"unsupported market cap unit: {unit}")


@dataclass
class StandardQuote:
    symbol: str
    stock_name: Optional[str] = None
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None       # 手
    amount: Optional[float] = None       # 元
    turnover_ratio: Optional[float] = None
    change_percent: Optional[float] = None
    circulating_market_cap: Optional[float] = None
    total_market_cap: Optional[float] = None
    source: Optional[str] = None
    raw_json: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def standardize_xgb_stock_data(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    标准化选股宝 /api/stock/data 返回。
    """
    raw = dict(item or {})
    symbol = normalize_symbol(raw.get("symbol"))
    quote = StandardQuote(
        symbol=symbol,
        stock_name=raw.get("stock_chi_name") or raw.get("stock_name") or raw.get("name"),
        price=_to_float(raw.get("price")),
        open=_to_float(raw.get("open")),
        high=_to_float(raw.get("high")),
        low=_to_float(raw.get("low")),
        close=_to_float(raw.get("close") or raw.get("price")),
        volume=normalize_volume_to_shou(raw.get("volume"), unit="shou", source="xgb"),
        amount=normalize_amount_to_yuan(raw.get("amount"), unit="wan_yuan", source="xgb"),
        turnover_ratio=normalize_percent(raw.get("turnover_ratio"), input_type="auto"),
        change_percent=normalize_percent(raw.get("change_percent"), input_type="auto"),
        circulating_market_cap=normalize_market_cap_to_yuan(raw.get("circulating_market_cap"), unit="yi_yuan", source="xgb"),
        total_market_cap=normalize_market_cap_to_yuan(raw.get("total_market_cap"), unit="yi_yuan", source="xgb"),
        source="xgb",
        raw_json=json.dumps(raw, ensure_ascii=False),
    )
    return quote.to_dict()


def standardize_xgb_pool_item(item: Dict[str, Any], *, pool_name: Optional[str] = None, trade_date: Optional[str] = None) -> Dict[str, Any]:
    """
    标准化选股宝 pool/detail 返回。
    """
    raw = dict(item or {})
    symbol = normalize_symbol(raw.get("symbol"))
    out = standardize_xgb_stock_data(raw)
    out.update({
        "trade_date": trade_date,
        "pool_name": pool_name,
        "limit_up_days": _to_int(raw.get("limit_up_days"), 0),
        "first_limit_up_time": _to_int(raw.get("first_limit_up_time")),
        "last_limit_up_time": _to_int(raw.get("last_limit_up_time")),
        "surge_reason": raw.get("surge_reason"),
        "raw_json": json.dumps(raw, ensure_ascii=False),
    })
    return out


def format_yuan(value: Optional[float]) -> str:
    if value is None:
        return "-"
    v = float(value)
    if abs(v) >= 100_000_000:
        return f"{v / 100_000_000:.2f}亿"
    if abs(v) >= 10_000:
        return f"{v / 10_000:.2f}万"
    return f"{v:.2f}元"


def format_volume_shou(value: Optional[float]) -> str:
    if value is None:
        return "-"
    v = float(value)
    if abs(v) >= 10_000:
        return f"{v / 10_000:.2f}万手"
    return f"{v:.0f}手"


def clamp_score(value: float, low: float = 0, high: float = 100) -> float:
    return max(low, min(high, float(value)))


def pct_change(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old in (None, 0):
        return None
    return (float(new) / float(old) - 1.0) * 100.0
