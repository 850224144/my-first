"""
v2.6.0 撤军线/结构止损诊断。

只做诊断，不自动改 stop_loss。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import math


def _f(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        v = float(value)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _bars_to_list(daily_bars: Any) -> List[Dict[str, Any]]:
    if daily_bars is None:
        return []
    if isinstance(daily_bars, list):
        return [x for x in daily_bars if isinstance(x, dict)]
    try:
        # pandas DataFrame
        df = daily_bars
        df = df.where(df.notna(), None)
        return df.to_dict("records")
    except Exception:
        return []


def _col(row: Dict[str, Any], *names: str) -> Optional[float]:
    for n in names:
        if n in row:
            v = _f(row.get(n))
            if v is not None:
                return v
    return None


def _atr14(bars: List[Dict[str, Any]]) -> Optional[float]:
    if len(bars) < 15:
        return None
    trs = []
    prev_close = None
    for row in bars[-30:]:
        high = _col(row, "high", "HIGH")
        low = _col(row, "low", "LOW")
        close = _col(row, "close", "CLOSE")
        if high is None or low is None or close is None:
            continue
        if prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < 14:
        return None
    return sum(trs[-14:]) / 14


def _recent_low(bars: List[Dict[str, Any]], n: int) -> Optional[float]:
    lows = []
    for row in bars[-n:]:
        low = _col(row, "low", "LOW")
        if low is not None:
            lows.append(low)
    return min(lows) if lows else None


def risk_pct(entry_price: float, stop_loss: float) -> Optional[float]:
    if not entry_price or not stop_loss or entry_price <= 0 or stop_loss <= 0:
        return None
    if stop_loss >= entry_price:
        return None
    return round((entry_price - stop_loss) / entry_price * 100, 2)


def diagnose_stop_loss_for_candidate(
    candidate: Dict[str, Any],
    *,
    daily_bars: Any = None,
    max_allowed_risk_pct: float = 8.0,
) -> Dict[str, Any]:
    entry = _f(candidate.get("current_price") or candidate.get("price") or candidate.get("trigger_price"))
    current_stop = _f(candidate.get("stop_loss") or candidate.get("yj_stop_loss"))
    current_risk = _f(candidate.get("risk_pct"))

    bars = _bars_to_list(daily_bars)
    proposals: List[Dict[str, Any]] = []

    if entry:
        max_allowed_stop = entry * (1 - max_allowed_risk_pct / 100)
        proposals.append({
            "source": "max_allowed_8pct_stop",
            "stop_loss": round(max_allowed_stop, 3),
            "risk_pct": max_allowed_risk_pct,
            "note": "这是最大允许风险线，不代表结构止损，不能自动采用。",
        })

        if current_stop:
            proposals.append({
                "source": "current_structure_stop",
                "stop_loss": current_stop,
                "risk_pct": current_risk if current_risk is not None else risk_pct(entry, current_stop),
                "note": "当前系统使用的结构止损。",
            })

        if bars:
            low5 = _recent_low(bars, 5)
            low10 = _recent_low(bars, 10)
            atr = _atr14(bars)

            if low5:
                stop = low5 * 0.995
                proposals.append({
                    "source": "recent_5_low_minus_0.5pct",
                    "stop_loss": round(stop, 3),
                    "risk_pct": risk_pct(entry, stop),
                    "note": "近5日低点下方0.5%，较敏感。",
                })
            if low10:
                stop = low10 * 0.99
                proposals.append({
                    "source": "recent_10_low_minus_1pct",
                    "stop_loss": round(stop, 3),
                    "risk_pct": risk_pct(entry, stop),
                    "note": "近10日低点下方1%，偏结构。",
                })
            if atr:
                stop = entry - 1.5 * atr
                proposals.append({
                    "source": "atr14_1_5x",
                    "stop_loss": round(stop, 3),
                    "risk_pct": risk_pct(entry, stop),
                    "note": "ATR 1.5倍止损，适合波动口径参考。",
                })

    valid = [p for p in proposals if p.get("risk_pct") is not None]
    compressible = [
        p for p in valid
        if p["source"] != "max_allowed_8pct_stop"
        and p["risk_pct"] <= max_allowed_risk_pct
        and p["stop_loss"] < entry
    ] if entry else []

    if current_risk is not None and current_risk <= max_allowed_risk_pct:
        conclusion = "current_stop_ok"
    elif compressible:
        conclusion = "possible_stop_compression"
    else:
        conclusion = "structure_stop_too_far"

    return {
        "symbol": candidate.get("symbol") or candidate.get("code"),
        "stock_name": candidate.get("stock_name") or candidate.get("name"),
        "entry_price": entry,
        "current_stop_loss": current_stop,
        "current_risk_pct": current_risk,
        "conclusion": conclusion,
        "proposals": valid,
        "compressible_proposals": compressible,
    }


def summarize_stop_loss_diagnostics(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    from collections import Counter
    c = Counter(x.get("conclusion") for x in results)
    compressible = [x for x in results if x.get("conclusion") == "possible_stop_compression"]
    return {
        "total": len(results),
        "conclusion_counter": dict(c),
        "compressible_count": len(compressible),
        "compressible_sample": compressible[:10],
    }
