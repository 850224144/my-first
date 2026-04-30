"""
援军评分模块。

正式定义：
主线题材连续上涨后，第一次大分歧，
龙头/中军/换手龙不破结构，
随后场外增量资金放量承接，形成援军阳线，
并与日线二买位置共振。

v2.4.0 只评分、只展示，不直接改 paper_trader 买入。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
import math

try:
    from .data_normalizer import clamp_score
except Exception:
    def clamp_score(value: float, low: float = 0, high: float = 100) -> float:
        return max(low, min(high, float(value)))


@dataclass
class YuanjunScore:
    yuanjun_score: float
    yuanjun_state: str
    yuanjun_flags: List[str]
    yuanjun_reasons: List[str]
    divergence_score: float
    divergence_count: int
    rescue_candle_score: float
    yj_candle_low: Optional[float] = None
    yj_candle_mid: Optional[float] = None
    yj_candle_high: Optional[float] = None
    yj_stop_loss: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        v = float(str(x).replace(",", ""))
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _records(bars: Any) -> List[Dict[str, Any]]:
    if bars is None:
        return []
    if hasattr(bars, "to_dict"):
        try:
            return bars.to_dict("records")
        except Exception:
            pass
    if isinstance(bars, list):
        return [dict(x) for x in bars if isinstance(x, dict)]
    return []


def _last_n(bars: Any, n: int) -> List[Dict[str, Any]]:
    rows = _records(bars)
    return rows[-n:] if len(rows) >= n else rows


def score_divergence(
    *,
    mainline_days: int = 0,
    theme_heat_score: float = 50,
    broken_count: int = 0,
    limit_down_count: int = 0,
    leader_resilient: bool = False,
    previous_divergence_count: int = 0,
) -> Dict[str, Any]:
    """
    第一次大分歧评分。
    """
    flags: List[str] = []
    reasons: List[str] = []
    score = 40.0

    if mainline_days >= 3:
        score += 18
        reasons.append(f"主线持续 {mainline_days} 天")
    else:
        flags.append("主线持续时间不足(mainline_days_insufficient)")

    if theme_heat_score >= 70:
        score += 14
        reasons.append("题材热度较强")
    elif theme_heat_score < 45:
        score -= 12
        flags.append("题材热度不足(theme_heat_weak)")

    if broken_count > 0:
        score += min(18, broken_count * 4)
        reasons.append(f"出现炸板/分歧 {broken_count} 只")
    else:
        flags.append("分歧不明显(divergence_not_obvious)")

    if limit_down_count > 0:
        score += min(10, limit_down_count * 2)
        reasons.append(f"出现跌停分化 {limit_down_count} 只")

    if leader_resilient:
        score += 18
        reasons.append("龙头/中军分歧日抗跌")
    else:
        flags.append("龙头抗跌未确认(leader_resilience_unconfirmed)")

    if previous_divergence_count >= 1:
        flags.append("非第一次分歧(divergence_count_too_many)")
        score -= 30

    return {
        "divergence_score": round(clamp_score(score), 2),
        "divergence_count": previous_divergence_count + (1 if score >= 60 else 0),
        "divergence_flags": list(dict.fromkeys(flags)),
        "divergence_reasons": list(dict.fromkeys(reasons)),
    }


def score_rescue_candle(
    daily_bars: Any,
    *,
    sector_follow_limit_up_count: int = 0,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    援军阳线评分。
    需要最近两根日 K。
    """
    cfg = config or {}
    rows = _last_n(daily_bars, 6)
    flags: List[str] = []
    reasons: List[str] = []

    if len(rows) < 2:
        return {
            "rescue_candle_score": 45.0,
            "rescue_flags": ["K线数据不足(rescue_kline_insufficient)"],
            "rescue_reasons": [],
            "yj_candle_low": None,
            "yj_candle_mid": None,
            "yj_candle_high": None,
            "yj_stop_loss": None,
        }

    last = rows[-1]
    prev = rows[-2]

    open_ = _float(last.get("open"))
    high = _float(last.get("high"))
    low = _float(last.get("low"))
    close = _float(last.get("close") or last.get("price"))
    volume = _float(last.get("volume"), 0.0)
    prev_close = _float(prev.get("close") or prev.get("price"))
    prev_volume = _float(prev.get("volume"), 0.0)

    if None in (open_, high, low, close, prev_close):
        return {
            "rescue_candle_score": 45.0,
            "rescue_flags": ["K线字段不完整(rescue_kline_fields_missing)"],
            "rescue_reasons": [],
            "yj_candle_low": low,
            "yj_candle_mid": None,
            "yj_candle_high": high,
            "yj_stop_loss": low,
        }

    gain_pct = (close / prev_close - 1.0) * 100.0 if prev_close else 0.0
    vol_ratio = volume / prev_volume if prev_volume else 0.0
    intraday_range = high - low if high and low else 0.0
    close_position = (close - low) / intraday_range if intraday_range > 0 else 0.5
    upper_shadow = (high - close) / intraday_range if intraday_range > 0 else 0.0

    score = 45.0

    if close > open_:
        score += 8
        reasons.append("当日收阳")
    else:
        score -= 8
        flags.append("未收阳(rescue_not_bullish)")

    min_gain = float(cfg.get("rescue_gain_pct_min", 3))
    strong_gain = float(cfg.get("strong_rescue_gain_pct_min", 5))
    if gain_pct >= strong_gain:
        score += 18
        reasons.append(f"涨幅较强 {gain_pct:.2f}%")
    elif gain_pct >= min_gain:
        score += 10
        reasons.append(f"涨幅达标 {gain_pct:.2f}%")
    else:
        flags.append("上涨幅度不足(rescue_gain_weak)")

    min_vol = float(cfg.get("rescue_volume_ratio_min", 1.5))
    strong_vol = float(cfg.get("strong_rescue_volume_ratio_min", 2.0))
    if vol_ratio >= strong_vol:
        score += 18
        reasons.append(f"成交量放大 {vol_ratio:.2f} 倍")
    elif vol_ratio >= min_vol:
        score += 10
        reasons.append(f"成交量温和放大 {vol_ratio:.2f} 倍")
    else:
        flags.append("量能未确认(volume_not_confirm)")

    if close_position >= 0.7:
        score += 12
        reasons.append("收盘位置靠近日内高位")
    elif close_position < 0.45:
        score -= 10
        flags.append("收盘位置偏弱(rescue_close_position_weak)")

    if upper_shadow > 0.45 and volume > prev_volume:
        score -= 15
        flags.append("放量长上影/滞涨(rescue_stalling)")

    strong_follow = int(cfg.get("strong_sector_follow_limit_up_count", 5))
    min_follow = int(cfg.get("min_sector_follow_limit_up_count", 3))
    if sector_follow_limit_up_count >= strong_follow:
        score += 14
        reasons.append(f"板块跟风涨停 {sector_follow_limit_up_count} 只")
    elif sector_follow_limit_up_count >= min_follow:
        score += 8
        reasons.append(f"板块存在跟风涨停 {sector_follow_limit_up_count} 只")
    elif sector_follow_limit_up_count <= 0:
        score -= 18
        flags.append("无板块效应(no_sector_follow)")
    else:
        flags.append("板块跟风不足(sector_follow_weak)")

    yj_mid = (high + low) / 2.0 if high is not None and low is not None else None

    return {
        "rescue_candle_score": round(clamp_score(score), 2),
        "rescue_flags": list(dict.fromkeys(flags)),
        "rescue_reasons": list(dict.fromkeys(reasons)),
        "yj_candle_low": low,
        "yj_candle_mid": yj_mid,
        "yj_candle_high": high,
        "yj_stop_loss": low,
    }


def score_yuanjun(
    *,
    daily_bars: Any,
    theme_heat_score: float = 50,
    leader_score: float = 50,
    leader_type: str = "normal_stock",
    mainline_days: int = 0,
    broken_count: int = 0,
    limit_down_count: int = 0,
    sector_follow_limit_up_count: int = 0,
    previous_divergence_count: int = 0,
    leader_resilient: bool = False,
    stage_gain_pct: Optional[float] = None,
    pullback_new_low: bool = False,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = config or {}
    flags: List[str] = []
    reasons: List[str] = []

    div = score_divergence(
        mainline_days=mainline_days,
        theme_heat_score=theme_heat_score,
        broken_count=broken_count,
        limit_down_count=limit_down_count,
        leader_resilient=leader_resilient,
        previous_divergence_count=previous_divergence_count,
    )
    rescue = score_rescue_candle(
        daily_bars,
        sector_follow_limit_up_count=sector_follow_limit_up_count,
        config=cfg,
    )

    divergence_score = float(div["divergence_score"])
    rescue_score = float(rescue["rescue_candle_score"])

    score = (
        float(theme_heat_score) * 0.20
        + float(divergence_score) * 0.20
        + float(leader_score) * 0.25
        + float(rescue_score) * 0.25
        + 50.0 * 0.10
    )

    flags.extend(div.get("divergence_flags", []))
    flags.extend(rescue.get("rescue_flags", []))
    reasons.extend(div.get("divergence_reasons", []))
    reasons.extend(rescue.get("rescue_reasons", []))

    if leader_type == "follower":
        flags.append("后排跟风股，援军不救杂毛(leader_follower)")
        score -= 25

    if previous_divergence_count >= int(cfg.get("max_divergence_count", 1)):
        flags.append("主线第二次及以上分歧，放弃援军(divergence_count_too_many)")
        score -= 25

    if pullback_new_low:
        flags.append("回调创新低(pullback_new_low)")
        score -= 25

    high_position_gain = float(cfg.get("high_position_gain_pct", 100))
    if stage_gain_pct is not None and float(stage_gain_pct) >= high_position_gain:
        flags.append("高位援军风险(high_position_yuanjun)")
        score -= 10
        reasons.append(f"阶段涨幅 {float(stage_gain_pct):.2f}% 偏高，禁止强买")

    score = clamp_score(score)

    if score >= 85:
        state = "YJ_STRONG_CONFIRMED"
    elif score >= 75:
        state = "YJ_CONFIRMED"
    elif score >= 65:
        state = "YJ_NEAR_TRIGGER"
    elif score >= 50:
        state = "YJ_WATCH_ONLY"
    else:
        state = "YJ_REJECTED"

    return YuanjunScore(
        yuanjun_score=round(score, 2),
        yuanjun_state=state,
        yuanjun_flags=list(dict.fromkeys(flags)),
        yuanjun_reasons=list(dict.fromkeys(reasons)),
        divergence_score=round(divergence_score, 2),
        divergence_count=int(div.get("divergence_count", 0)),
        rescue_candle_score=round(rescue_score, 2),
        yj_candle_low=rescue.get("yj_candle_low"),
        yj_candle_mid=rescue.get("yj_candle_mid"),
        yj_candle_high=rescue.get("yj_candle_high"),
        yj_stop_loss=rescue.get("yj_stop_loss"),
    ).to_dict()
