"""
v2.5.3 watchlist 行 -> final_signal candidate 映射。

你的 watchlist 当前字段：
['code', 'name', 'first_seen_at', 'last_seen_at', 'date', 'mode', 'market_state',
 'signal', 'total_score', 'trend_score', 'pullback_score', 'stabilize_score',
 'confirm_score', 'risk_pct', 'risk_level', 'action', 'entry_type', 'entry_price',
 'trigger_price', 'stop_loss', 'take_profit_1', 'take_profit_2',
 'position_suggestion', 'warnings', 'veto', 'veto_reasons', 'invalid_condition',
 'note', 'status']

本模块将其转成：
- symbol
- stock_name
- daily_2buy_score
- current_price
- trigger_price
- risk_pct
- fresh_quote
- stop_loss
- target_1 / target_2
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import math

try:
    from .data_normalizer import normalize_symbol
except Exception:
    def normalize_symbol(x): return str(x)


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        s = str(value).strip()
        if s == "" or s.lower() in {"nan", "none", "null", "--"}:
            return default
        v = float(s.replace(",", ""))
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _b(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "是"}:
        return True
    if s in {"0", "false", "no", "n", "否"}:
        return False
    return bool(value)


def _s(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def parse_warning_flags(row: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    for key in ["warnings", "veto_reasons", "invalid_condition", "note"]:
        v = row.get(key)
        if v is None:
            continue
        if isinstance(v, list):
            flags.extend(str(x) for x in v if str(x))
        else:
            text = str(v).strip()
            if text and text.lower() not in {"nan", "none", "null", "[]"}:
                # 粗略切分
                parts = text.replace("；", ";").replace("，", ",").replace("|", ",").split(",")
                flags.extend(x.strip() for x in parts if x.strip())
    return list(dict.fromkeys(flags))


def normalize_quote(symbol: str, quote: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    quote_map 支持任意字段：
    - price / current_price / close
    - open/high/low
    - fresh_quote / is_fresh
    """
    if not quote:
        return {}

    price = _f(quote.get("price") or quote.get("current_price") or quote.get("close"))
    out = {
        "current_price": price,
        "price": price,
        "open": _f(quote.get("open")),
        "high": _f(quote.get("high")),
        "low": _f(quote.get("low")),
        "fresh_quote": _b(quote.get("fresh_quote", quote.get("is_fresh")), True),
        "quote_source": quote.get("source") or quote.get("quote_source"),
        "quote_time": quote.get("quote_time") or quote.get("datetime") or quote.get("time"),
    }
    return {k: v for k, v in out.items() if v is not None}


def build_candidate_from_watchlist_row(
    row: Dict[str, Any],
    *,
    quote: Optional[Dict[str, Any]] = None,
    allow_stale_fallback_price: bool = True,
) -> Dict[str, Any]:
    symbol_raw = row.get("symbol") or row.get("code")
    symbol = normalize_symbol(symbol_raw) if symbol_raw else None

    stock_name = row.get("stock_name") or row.get("name")
    total_score = _f(row.get("daily_2buy_score", row.get("total_score")), 0) or 0
    risk_pct = _f(row.get("risk_pct"), 999) or 999
    trigger_price = _f(row.get("trigger_price"))
    entry_price = _f(row.get("entry_price"))
    stop_loss = _f(row.get("stop_loss"))

    quote_std = normalize_quote(symbol or "", quote)
    current_price = quote_std.get("current_price")

    # 没有实时价时，只用兜底价做展示，但 fresh_quote=False，防止误买
    fresh_quote = quote_std.get("fresh_quote")
    if current_price is None and allow_stale_fallback_price:
        current_price = entry_price or trigger_price
        fresh_quote = False

    warnings = parse_warning_flags(row)

    # 旧字段 veto=True 直接加入风险旗标
    if _b(row.get("veto"), False):
        warnings.append("旧策略 veto=True(veto_from_watchlist)")

    candidate = {
        "symbol": symbol,
        "code": row.get("code"),
        "stock_name": stock_name,
        "name": stock_name,

        "trade_date": row.get("date"),
        "market_state": row.get("market_state"),
        "source_signal": row.get("signal"),
        "source_status": row.get("status"),
        "source_mode": row.get("mode"),

        "daily_2buy_score": total_score,
        "total_score": total_score,
        "trend_score": _f(row.get("trend_score")),
        "pullback_score": _f(row.get("pullback_score")),
        "stabilize_score": _f(row.get("stabilize_score")),
        "confirm_score": _f(row.get("confirm_score")),

        "risk_pct": risk_pct,
        "risk_level": row.get("risk_level"),

        "current_price": current_price,
        "price": current_price,
        "trigger_price": trigger_price,
        "planned_buy_price": entry_price or trigger_price,
        "entry_price": entry_price,

        "stop_loss": stop_loss,
        "yj_stop_loss": stop_loss,
        "target_1": _f(row.get("take_profit_1")),
        "target_2": _f(row.get("take_profit_2")),

        "fresh_quote": bool(fresh_quote) if fresh_quote is not None else False,
        "risk_flags": warnings,

        "position_suggestion": row.get("position_suggestion"),
        "action": row.get("action"),
        "entry_type": row.get("entry_type"),

        "is_demo": False,
        "data_mode": "real",
    }

    candidate.update(quote_std)

    # 缺字段提示
    missing: List[str] = []
    for k in ["symbol", "daily_2buy_score", "risk_pct", "current_price", "trigger_price", "fresh_quote"]:
        if candidate.get(k) is None:
            missing.append(k)
    candidate["candidate_missing_fields"] = missing

    return candidate


def candidate_ready_report(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    required = [
        "symbol",
        "daily_2buy_score",
        "risk_pct",
        "current_price",
        "trigger_price",
        "fresh_quote",
        "weekly_score",
        "sector_score",
        "leader_score",
        "yuanjun_score",
    ]
    coverage = {}
    for k in required:
        count = sum(1 for c in candidates if c.get(k) is not None)
        coverage[k] = {"non_null": count, "ratio": round(count / max(len(candidates), 1), 4)}
    return {
        "rows": len(candidates),
        "coverage": coverage,
        "missing_all": [k for k, v in coverage.items() if v["non_null"] == 0],
    }
