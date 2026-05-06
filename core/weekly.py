"""
周线评分模块。

v2.9.6 稳定性修复：
- 兼容 strict / **kwargs
- 支持 list[dict] / dict(data=...) / pandas.DataFrame / polars.DataFrame
- 支持 pandas.Series / polars.Series / 列字典
- 避免 DataFrame / Series 参与布尔判断
- 原有周线评分逻辑保持不变
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
import datetime as dt
import math

try:
    from .data_normalizer import clamp_score
except Exception:
    def clamp_score(value: float, low: float = 0, high: float = 100) -> float:
        return max(low, min(high, float(value)))


@dataclass
class WeeklyScore:
    weekly_score: float
    weekly_state: str
    weekly_flags: List[str]
    weekly_reasons: List[str]
    close: Optional[float] = None
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, dt.date, dt.datetime)):
        return value

    try:
        if hasattr(value, "item") and callable(value.item):
            return value.item()
    except Exception:
        pass

    try:
        if hasattr(value, "to_list") and callable(value.to_list):
            arr = value.to_list()
            return arr[0] if arr else None
    except Exception:
        pass

    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            arr = value.tolist()
            if isinstance(arr, list):
                return arr[0] if arr else None
            return arr
    except Exception:
        pass

    if isinstance(value, (list, tuple)):
        return value[0] if value else None

    return value


def _value_to_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        return [value]

    try:
        if hasattr(value, "to_list") and callable(value.to_list):
            arr = value.to_list()
            return list(arr) if isinstance(arr, (list, tuple)) else [arr]
    except Exception:
        pass

    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            arr = value.tolist()
            return list(arr) if isinstance(arr, (list, tuple)) else [arr]
    except Exception:
        pass

    if isinstance(value, (list, tuple)):
        return list(value)

    return [value]


def _dict_to_records(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    if "data" in d:
        return _to_records(d.get("data"))

    values = {k: _value_to_list(v) for k, v in d.items()}
    max_len = max((len(v) for v in values.values()), default=0)

    if max_len > 1:
        rows: List[Dict[str, Any]] = []
        for i in range(max_len):
            row = {}
            for k, arr in values.items():
                row[k] = arr[i] if i < len(arr) else None
            rows.append(row)
        return rows

    return [{k: _scalar(v) for k, v in d.items()}]


def _to_records(value: Any) -> List[Dict[str, Any]]:
    """
    安全转 records。
    重点：绝不对 DataFrame/Series 使用 if value / value or []。
    """
    if value is None:
        return []

    # polars DataFrame 优先
    if hasattr(value, "to_dicts") and callable(value.to_dicts):
        try:
            records = value.to_dicts()
            if isinstance(records, list):
                return [dict(x) for x in records if isinstance(x, dict)]
        except Exception:
            pass

    # pandas DataFrame
    if hasattr(value, "to_dict") and callable(value.to_dict) and not isinstance(value, dict):
        try:
            records = value.to_dict("records")
            if isinstance(records, list):
                return [dict(x) for x in records if isinstance(x, dict)]
        except TypeError:
            try:
                d = value.to_dict()
                if isinstance(d, dict):
                    return _dict_to_records(d)
            except Exception:
                pass
        except Exception:
            pass

    if isinstance(value, dict):
        return _dict_to_records(value)

    if isinstance(value, (list, tuple)):
        out: List[Dict[str, Any]] = []
        for x in value:
            if isinstance(x, dict):
                out.extend(_dict_to_records(x))
            elif hasattr(x, "to_dicts") or hasattr(x, "to_dict"):
                out.extend(_to_records(x))
        return out

    return []


def _pick_bars(item: Dict[str, Any]) -> Any:
    """
    安全选择 daily_bars / bars。
    不使用 daily_bars or bars，避免 DataFrame/Series 布尔判断。
    """
    if not isinstance(item, dict):
        return []

    if "daily_bars" in item:
        v = item.get("daily_bars")
        if v is not None:
            return v

    if "bars" in item:
        v = item.get("bars")
        if v is not None:
            return v

    return []


def _parse_date(value: Any) -> Optional[dt.date]:
    value = _scalar(value)
    if value is None:
        return None
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    s = str(value)[:10].replace("/", "-")
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def _float(value: Any) -> Optional[float]:
    value = _scalar(value)
    if value is None:
        return None
    try:
        v = float(str(value).replace(",", ""))
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def resample_daily_to_weekly(daily_bars: Any) -> List[Dict[str, Any]]:
    records = _to_records(daily_bars)
    cleaned: List[Dict[str, Any]] = []
    for r in records:
        d = _parse_date(r.get("trade_date") or r.get("date") or r.get("datetime"))
        c = _float(r.get("close") or r.get("price"))
        if d is None or c is None:
            continue
        cleaned.append({
            "date": d,
            "open": _float(r.get("open")) or c,
            "high": _float(r.get("high")) or c,
            "low": _float(r.get("low")) or c,
            "close": c,
            "volume": _float(r.get("volume")) or 0.0,
            "amount": _float(r.get("amount")) or 0.0,
        })

    cleaned.sort(key=lambda x: x["date"])
    weeks: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in cleaned:
        iso = r["date"].isocalendar()
        key = (iso.year, iso.week)
        weeks.setdefault(key, []).append(r)

    weekly: List[Dict[str, Any]] = []
    for key in sorted(weeks.keys()):
        rows = weeks[key]
        weekly.append({
            "week": f"{key[0]}-W{key[1]:02d}",
            "date": rows[-1]["date"].isoformat(),
            "open": rows[0]["open"],
            "high": max(x["high"] for x in rows),
            "low": min(x["low"] for x in rows),
            "close": rows[-1]["close"],
            "volume": sum(x["volume"] for x in rows),
            "amount": sum(x["amount"] for x in rows),
        })
    return weekly


def _ma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def score_weekly_trend(daily_bars: Any, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = config if isinstance(config, dict) else {}
    weekly = resample_daily_to_weekly(daily_bars)
    flags: List[str] = []
    reasons: List[str] = []

    if len(weekly) < 10:
        return WeeklyScore(
            weekly_score=50,
            weekly_state="weekly_unknown",
            weekly_flags=["周线数据不足(weekly_data_insufficient)"],
            weekly_reasons=["周线样本少于10周，暂不硬过滤，只降级提醒"],
        ).to_dict()

    closes = [float(x["close"]) for x in weekly]
    highs = [float(x["high"]) for x in weekly]
    lows = [float(x["low"]) for x in weekly]
    close = closes[-1]

    ma5 = _ma(closes, 5)
    ma10 = _ma(closes, 10)
    ma20 = _ma(closes, 20)

    score = 50.0

    if ma5 is not None and close >= ma5:
        score += 8
        reasons.append("周收盘价站上5周线")
    else:
        score -= 5
        flags.append("周线未站上5周线(weekly_below_ma5)")

    if ma10 is not None and close >= ma10:
        score += 8
        reasons.append("周收盘价站上10周线")
    else:
        score -= 5
        flags.append("周线未站上10周线(weekly_below_ma10)")

    if ma20 is not None:
        if close >= ma20:
            score += 10
            reasons.append("周收盘价站上20周线")
        else:
            score -= 12
            flags.append("周线跌破20周线(weekly_below_ma20)")

        ma20_prev = sum(closes[-25:-5]) / 20 if len(closes) >= 25 else None
        if ma20_prev is not None:
            ma20_slope_pct = (ma20 / ma20_prev - 1) * 100
            if ma20_slope_pct > 0:
                score += 12
                reasons.append("20周线向上")
            else:
                score -= 12
                flags.append("20周线向下(weekly_ma20_down)")

            if close < ma20 and ma20_slope_pct < 0:
                flags.append("周线下降趋势(weekly_downtrend)")
                reasons.append("价格低于20周线且20周线向下")

        distance_ma20 = (close / ma20 - 1) * 100
        too_hot_pct = float(cfg.get("too_hot_distance_from_ma20_pct", 35))
        if distance_ma20 > too_hot_pct:
            score -= 18
            flags.append("周线过热(weekly_too_hot)")
            reasons.append(f"价格距离20周线过远：{distance_ma20:.2f}%")

    if len(highs) >= 6:
        recent_high_ok = highs[-1] >= max(highs[-6:-1]) * 0.98
        recent_low_ok = lows[-1] >= min(lows[-6:-1]) * 0.98
        if recent_high_ok and recent_low_ok:
            score += 10
            reasons.append("周线高低点结构未破坏")
        elif not recent_low_ok:
            score -= 12
            flags.append("周线结构破坏(weekly_structure_broken)")

    if len(closes) >= 4:
        mom4 = (closes[-1] / closes[-4] - 1) * 100
        if mom4 > 5:
            score += 8
            reasons.append(f"近4周动量较强：{mom4:.2f}%")
        elif mom4 < -8:
            score -= 10
            flags.append("周线动量偏弱(weekly_momentum_weak)")

    last = weekly[-1]
    high = float(last["high"])
    low = float(last["low"])
    if high > low:
        upper_shadow_ratio = (high - close) / (high - low)
        if upper_shadow_ratio > 0.55:
            score -= 8
            flags.append("周线长上影(weekly_long_upper_shadow)")

    score = clamp_score(score)

    hard_reject = float(cfg.get("hard_reject_score", 45))
    if "周线下降趋势(weekly_downtrend)" in flags and score < 55:
        state = "weekly_downtrend"
    elif "周线过热(weekly_too_hot)" in flags:
        state = "weekly_too_hot"
    elif score >= 80:
        state = "weekly_strong_uptrend"
    elif score >= 70:
        state = "weekly_uptrend"
    elif score >= 55:
        state = "weekly_repairing"
    elif score >= hard_reject:
        state = "weekly_sideways"
    else:
        state = "weekly_downtrend"

    return WeeklyScore(
        weekly_score=round(score, 2),
        weekly_state=state,
        weekly_flags=list(dict.fromkeys(flags)),
        weekly_reasons=list(dict.fromkeys(reasons)),
        close=close,
        ma5=ma5,
        ma10=ma10,
        ma20=ma20,
    ).to_dict()


def filter_by_weekly_trend(
    candidates: List[Dict[str, Any]],
    *,
    mode: str = "observe",
    strict: bool = False,
    config: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    兼容旧调用：filter_by_weekly_trend

    candidates 每项建议包含：
    - code/symbol
    - daily_bars

    返回：
    {
      "passed": [...],
      "rejected": [...],
      "scored": [...]
    }
    """
    cfg = dict(config) if isinstance(config, dict) else {}
    hard_reject_score = float(cfg.get("hard_reject_score", 45))
    passed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    scored: List[Dict[str, Any]] = []

    for c in _to_records(candidates):
        item = dict(c)
        bars = _pick_bars(item)

        try:
            ws = score_weekly_trend(bars, config=cfg)
        except Exception as exc:
            item["weekly_score"] = item.get("weekly_score", 50.0)
            item["weekly_state"] = item.get("weekly_state", "weekly_error")
            old_flags = item.get("weekly_flags")
            if not isinstance(old_flags, list):
                old_flags = []
            item["weekly_flags"] = list(dict.fromkeys(old_flags + [f"周线评分异常(weekly_score_error):{exc}"]))
            item["weekly_reasons"] = item.get("weekly_reasons") if isinstance(item.get("weekly_reasons"), list) else []
            rejected.append(item)
            scored.append(item)
            continue

        item.update(ws)
        scored.append(item)

        hard_reject = (
            ws["weekly_score"] < hard_reject_score
            or ws["weekly_state"] in {"weekly_downtrend", "weekly_too_hot"}
        )

        if mode == "observe" and not strict:
            hard_reject = ws["weekly_score"] < 35

        if hard_reject:
            rejected.append(item)
        else:
            passed.append(item)

    return {"passed": passed, "rejected": rejected, "scored": scored}
