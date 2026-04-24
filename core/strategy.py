# -*- coding: utf-8 -*-
"""
core/strategy.py

类缠论二买 V1（工程落地版）
核心思想：
健康上涨 → 健康缩量回调 → 止跌缩量企稳 → 尾盘/收盘温和突破确认

注意：这不是完整笔、线段、中枢版本；它是先把“二买精神”量化落地。
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo

import polars as pl

CN_TZ = ZoneInfo("Asia/Shanghai")

# ========================= 参数区 =========================
TREND_LOOKBACK_MIN = 40
TREND_LOOKBACK_MAX = 90
TREND_GAIN_MIN = 0.25
TREND_GAIN_MAX = 0.80

PULLBACK_DAYS_MIN = 5
PULLBACK_DAYS_MAX = 25
PULLBACK_MIN = 0.08
PULLBACK_MAX = 0.25
PULLBACK_VOL_RATIO_MAX = 0.80

STABLE_DAYS = 5
STABLE_AMPLITUDE_MAX = 0.08

CONFIRM_PLATFORM_DAYS = 7
CONFIRM_PCT_MIN = 0.015
CONFIRM_PCT_MAX = 0.065
CONFIRM_VOL_RATIO_MIN = 1.10
CONFIRM_VOL_RATIO_MAX = 1.80
CONFIRM_VOL_RATIO_HARD_MAX = 2.50

MAX_STOP_DISTANCE = 0.08
DEFAULT_ACCOUNT_RISK = 0.01
MAX_POSITION_STRONG = 0.15
MAX_POSITION_NEUTRAL = 0.08

SIGNAL_NONE = "none"
SIGNAL_WEAK_OBSERVE = "weak_observe"
SIGNAL_OBSERVE = "observe"
SIGNAL_CONFIRM = "confirm"

SCAN_OBSERVE = "observe"       # 盘中观察，如 11:00
SCAN_TAIL = "tail_confirm"     # 14:45-14:55
SCAN_AFTER = "after_close"     # 15:10 后/盘后


# ========================= 工具函数 =========================

def _last(df: pl.DataFrame, col: str, default: float = 0.0) -> float:
    try:
        v = df.select(pl.col(col).last()).item()
        return float(v) if v is not None else default
    except Exception:
        return default


def _max_consecutive(values: List[int]) -> int:
    best = 0
    cur = 0
    for x in values:
        if int(x) == 1:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def get_scan_mode(now: Optional[datetime] = None) -> str:
    """根据当前中国市场时间判断扫描模式。"""
    n = now or datetime.now(CN_TZ)
    hm = n.hour * 100 + n.minute
    if 1445 <= hm <= 1455:
        return SCAN_TAIL
    if hm >= 1510 or hm < 930:
        return SCAN_AFTER
    return SCAN_OBSERVE


def _empty_result(reason: str = "数据不足") -> Dict[str, Any]:
    return {
        "signal": SIGNAL_NONE,
        "score": 0,
        "scores": {"trend": 0, "pullback": 0, "stable": 0, "confirm": 0},
        "reasons": [reason],
        "meta": {},
    }


# ========================= 四段评分 =========================

def score_trend(df: pl.DataFrame) -> Dict[str, Any]:
    """健康上涨段评分，满分25。"""
    score = 0
    reasons: List[str] = []
    meta: Dict[str, Any] = {}

    if df is None or len(df) < 130:
        return {"score": 0, "reasons": ["趋势段数据不足"], "meta": meta}

    work = df.tail(130).with_row_index("idx")
    n = len(work)

    # 阶段高点：优先找最近 5-30 日前的高点，避免把今天突破点当作原趋势高点
    high_area_start = max(0, n - 35)
    high_area_len = min(30, n - high_area_start)
    high_area = work.slice(high_area_start, high_area_len)
    if len(high_area) == 0:
        return {"score": 0, "reasons": ["无法定位阶段高点"], "meta": meta}

    stage_high_row = high_area.sort("high", descending=True).row(0, named=True)
    high_idx = int(stage_high_row["idx"])
    stage_high = float(stage_high_row["high"])

    left_start = max(0, high_idx - TREND_LOOKBACK_MAX)
    left = work.slice(left_start, high_idx - left_start + 1)
    if len(left) < TREND_LOOKBACK_MIN:
        return {"score": 0, "reasons": ["上涨段周期不足40日"], "meta": meta}

    low_row = left.sort("low").row(0, named=True)
    low_idx = int(low_row["idx"])
    stage_low = float(low_row["low"])
    up_days = high_idx - low_idx
    gain = stage_high / stage_low - 1 if stage_low > 0 else 0

    meta.update({"stage_low": stage_low, "stage_high": stage_high, "up_days": up_days, "trend_gain": gain, "stage_high_idx": high_idx})

    if TREND_LOOKBACK_MIN <= up_days <= TREND_LOOKBACK_MAX:
        score += 6
    else:
        reasons.append(f"上涨周期不理想：{up_days}日")

    if TREND_GAIN_MIN <= gain <= TREND_GAIN_MAX:
        score += 7
    else:
        reasons.append(f"上涨幅度不符合：{gain:.1%}")

    ma20 = _last(df, "ma20")
    ma60 = _last(df, "ma60")
    ma60_slope = _last(df, "ma60_slope20")
    close = _last(df, "close")

    if ma20 > ma60:
        score += 4
    else:
        reasons.append("ma20 未在 ma60 上方")

    if ma60_slope >= -0.01:
        score += 3
    else:
        reasons.append("ma60 明显向下")

    if close >= ma60 * 0.98:
        score += 2
    else:
        reasons.append("当前价格有效跌破 ma60")

    # 排除妖股/连续涨停/连续一字板
    last40 = df.tail(40)
    limit_flags = last40.select("limit_up_like").to_series().to_list() if "limit_up_like" in last40.columns else []
    one_flags = last40.select("one_price_limit_like").to_series().to_list() if "one_price_limit_like" in last40.columns else []
    max_limit = _max_consecutive(limit_flags)
    max_one = _max_consecutive(one_flags)
    meta.update({"max_consecutive_limit_up": max_limit, "max_consecutive_one_price": max_one})

    if max_limit < 3 and max_one < 2:
        score += 3
    else:
        reasons.append("疑似妖股/连续涨停/连续一字板，剔除")

    return {"score": min(score, 25), "reasons": reasons, "meta": meta}


def score_pullback(df: pl.DataFrame, trend_meta: Dict[str, Any]) -> Dict[str, Any]:
    """健康缩量回调评分，满分25。"""
    score = 0
    reasons: List[str] = []
    meta: Dict[str, Any] = {}

    if not trend_meta or "stage_high" not in trend_meta:
        return {"score": 0, "reasons": ["缺少趋势段信息"], "meta": meta}

    work = df.tail(130).with_row_index("idx")
    high_idx = int(trend_meta.get("stage_high_idx", 0))
    stage_high = float(trend_meta.get("stage_high", 0))
    stage_low = float(trend_meta.get("stage_low", 0))

    n = len(work)
    pullback_len = n - high_idx - 1
    if pullback_len <= 0:
        return {"score": 0, "reasons": ["尚未形成回调段"], "meta": meta}

    pullback = work.slice(high_idx + 1, pullback_len)
    if len(pullback) == 0:
        return {"score": 0, "reasons": ["回调段为空"], "meta": meta}

    pullback_low = float(pullback.select(pl.min("low")).item())
    drawdown = stage_high / pullback_low - 1 if pullback_low > 0 else 0
    meta.update({"pullback_days": pullback_len, "pullback_low": pullback_low, "pullback_drawdown": drawdown})

    if PULLBACK_DAYS_MIN <= pullback_len <= PULLBACK_DAYS_MAX:
        score += 5
    else:
        reasons.append(f"回调时间不理想：{pullback_len}日")

    if PULLBACK_MIN <= drawdown <= PULLBACK_MAX:
        score += 7
    else:
        reasons.append(f"回调幅度不符合：{drawdown:.1%}")

    # 回调缩量：回调均量 < 上涨段均量的80%
    up_start_idx = int(trend_meta.get("stage_high_idx", 0)) - int(trend_meta.get("up_days", 0))
    up_start_idx = max(0, up_start_idx)
    up_seg = work.slice(up_start_idx, high_idx - up_start_idx + 1)
    up_vol = float(up_seg.select(pl.mean("volume")).item()) if len(up_seg) > 0 else 0
    pull_vol = float(pullback.select(pl.mean("volume")).item()) if len(pullback) > 0 else 0
    vol_ratio = pull_vol / up_vol if up_vol > 0 else 999
    meta.update({"pullback_vol_ratio_vs_up": vol_ratio})

    if vol_ratio <= PULLBACK_VOL_RATIO_MAX:
        score += 5
    else:
        reasons.append(f"回调未明显缩量：{vol_ratio:.2f}")

    ma60 = _last(df, "ma60")
    close = _last(df, "close")
    if pullback_low >= ma60 * 0.97 and close >= ma60 * 0.98:
        score += 4
    else:
        reasons.append("回调有效跌破 ma60")

    if pullback_low > stage_low * 1.03:
        score += 2
    else:
        reasons.append("回调跌近/跌破上涨启动区域")

    big_down_count = int(pullback.select(pl.sum("big_volume_down")).item()) if "big_volume_down" in pullback.columns else 0
    meta.update({"big_volume_down_count": big_down_count})
    if big_down_count <= 1:
        score += 2
    else:
        reasons.append("回调中放量大阴线过多")

    return {"score": min(score, 25), "reasons": reasons, "meta": meta}


def score_stabilization(df: pl.DataFrame, pull_meta: Dict[str, Any]) -> Dict[str, Any]:
    """止跌缩量企稳评分，满分25。"""
    score = 0
    reasons: List[str] = []
    meta: Dict[str, Any] = {}

    if df is None or len(df) < 20:
        return {"score": 0, "reasons": ["企稳段数据不足"], "meta": meta}
    if not pull_meta or "pullback_low" not in pull_meta:
        return {"score": 0, "reasons": ["缺少回调段信息"], "meta": meta}

    last = df.tail(STABLE_DAYS)
    pullback_low = float(pull_meta.get("pullback_low", 0))
    low5 = float(last.select(pl.min("low")).item())
    high5 = float(last.select(pl.max("high")).item())
    amp5 = high5 / low5 - 1 if low5 > 0 else 999
    vol5 = float(last.select(pl.mean("volume")).item())
    vol_ma20 = _last(df, "vol_ma20")
    close = _last(df, "close")
    ma10 = _last(df, "ma10")
    big_down_count = int(last.select(pl.sum("big_volume_down")).item()) if "big_volume_down" in last.columns else 0

    lows = last.select("low").to_series().to_list()
    low_not_down = lows[-1] >= min(lows[:-1]) if len(lows) >= 2 else False

    meta.update({
        "stable_low5": low5,
        "stable_amp5": amp5,
        "stable_vol_ratio": vol5 / vol_ma20 if vol_ma20 > 0 else 999,
        "stable_big_down_count": big_down_count,
    })

    if low5 >= pullback_low * 0.995:
        score += 6
    else:
        reasons.append("最近5日再创新低")

    if low_not_down:
        score += 4
    else:
        reasons.append("低点仍在下移")

    if amp5 <= STABLE_AMPLITUDE_MAX:
        score += 5
    else:
        reasons.append(f"企稳段振幅过大：{amp5:.1%}")

    if vol_ma20 > 0 and vol5 < vol_ma20:
        score += 5
    else:
        reasons.append("企稳段没有缩量")

    if big_down_count == 0:
        score += 3
    else:
        reasons.append("企稳段存在放量大阴线")

    if ma10 > 0 and close >= ma10 * 0.98:
        score += 2
    else:
        reasons.append("收盘价离 ma10 偏弱")

    return {"score": min(score, 25), "reasons": reasons, "meta": meta}


def score_confirmation(df: pl.DataFrame) -> Dict[str, Any]:
    """尾盘/收盘温和突破确认评分，满分25。"""
    score = 0
    reasons: List[str] = []
    meta: Dict[str, Any] = {}

    if df is None or len(df) < CONFIRM_PLATFORM_DAYS + 2:
        return {"score": 0, "reasons": ["确认段数据不足"], "meta": meta}

    last = df.tail(1)
    prev = df.slice(max(0, len(df) - CONFIRM_PLATFORM_DAYS - 1), CONFIRM_PLATFORM_DAYS)
    platform_high = float(prev.select(pl.max("high")).item())
    close = _last(df, "close")
    high = _last(df, "high")
    low = _last(df, "low")
    pct = _last(df, "pct_chg")
    vol_ratio = _last(df, "vol_ratio")
    ma5 = _last(df, "ma5")
    ma10 = _last(df, "ma10")
    upper_shadow = _last(df, "upper_shadow_ratio")

    meta.update({
        "platform_high": platform_high,
        "confirm_close": close,
        "confirm_pct": pct,
        "confirm_vol_ratio": vol_ratio,
        "upper_shadow_ratio": upper_shadow,
    })

    if close > platform_high:
        score += 7
    else:
        reasons.append("尚未突破3-7日平台高点")

    if CONFIRM_PCT_MIN <= pct <= CONFIRM_PCT_MAX:
        score += 5
    else:
        reasons.append(f"确认涨幅不理想：{pct:.1%}")

    if CONFIRM_VOL_RATIO_MIN <= vol_ratio <= CONFIRM_VOL_RATIO_MAX:
        score += 5
    elif CONFIRM_VOL_RATIO_MAX < vol_ratio <= CONFIRM_VOL_RATIO_HARD_MAX:
        score += 2
        reasons.append(f"放量偏猛：{vol_ratio:.2f}")
    else:
        reasons.append(f"量能不在温和区间：{vol_ratio:.2f}")

    if close >= ma5 or close >= ma10:
        score += 3
    else:
        reasons.append("未站回 ma5/ma10")

    # 不追涨停、不买长上影
    if pct < 0.09:
        score += 3
    else:
        reasons.append("接近/达到涨停，不追")

    if high > low and upper_shadow <= 0.35:
        score += 2
    else:
        reasons.append("上影线偏长，疑似冲高回落")

    return {"score": min(score, 25), "reasons": reasons, "meta": meta}


# ========================= 总评估 =========================

def evaluate_second_buy(df: pl.DataFrame, scan_mode: Optional[str] = None) -> Dict[str, Any]:
    """
    返回二买评分和信号级别。
    df 必须已经 compute_features。
    """
    if df is None or len(df) < 130:
        return _empty_result("数据不足130根K线")

    mode = scan_mode or get_scan_mode()

    t = score_trend(df)
    p = score_pullback(df, t["meta"])
    s = score_stabilization(df, p["meta"])
    c = score_confirmation(df)

    scores = {
        "trend": int(t["score"]),
        "pullback": int(p["score"]),
        "stable": int(s["score"]),
        "confirm": int(c["score"]),
    }
    total = sum(scores.values())
    reasons = t["reasons"] + p["reasons"] + s["reasons"] + c["reasons"]
    meta = {**t["meta"], **p["meta"], **s["meta"], **c["meta"], "scan_mode": mode}

    # 分级：确认必须四段都不能太差，避免总分被某一段掩盖
    signal = SIGNAL_NONE
    if total >= 80 and scores["trend"] >= 18 and scores["pullback"] >= 18 and scores["stable"] >= 18 and scores["confirm"] >= 18:
        signal = SIGNAL_CONFIRM
    elif total >= 70 and scores["trend"] >= 18 and scores["pullback"] >= 16 and scores["stable"] >= 16:
        signal = SIGNAL_OBSERVE
    elif total >= 60:
        signal = SIGNAL_WEAK_OBSERVE

    # 11点等非尾盘/非盘后，只能观察，不能确认
    if mode == SCAN_OBSERVE and signal == SIGNAL_CONFIRM:
        signal = SIGNAL_OBSERVE
        reasons.append("当前不是尾盘/收盘，确认信号降级为观察信号")

    return {
        "signal": signal,
        "score": int(total),
        "scores": scores,
        "reasons": reasons[:8],
        "meta": meta,
    }


# 兼容旧接口：只返回是否确认二买
def is_second_buy(df: pl.DataFrame) -> bool:
    return evaluate_second_buy(df).get("signal") == SIGNAL_CONFIRM


# 兼容旧接口：是否温和放量确认
def volume_confirm(df: pl.DataFrame) -> bool:
    vr = _last(df, "vol_ratio")
    return CONFIRM_VOL_RATIO_MIN <= vr <= CONFIRM_VOL_RATIO_HARD_MAX


def trade_plan(
    df: pl.DataFrame,
    signal_result: Optional[Dict[str, Any]] = None,
    market_state: str = "neutral",
    account_risk: float = DEFAULT_ACCOUNT_RISK,
) -> Dict[str, Any]:
    """
    交易计划：
    - 买入价：当前/收盘确认价附近
    - 止损：企稳平台低点下方1%、ma60附近，取更靠近买入价且不超过8%的结构止损
    - 仓位：单笔账户风险控制 + 市场状态上限
    """
    if df is None or len(df) < 20:
        return {"valid": False, "reason": "数据不足"}

    close = _last(df, "close")
    ma60 = _last(df, "ma60")
    low5 = float(df.tail(STABLE_DAYS).select(pl.min("low")).item())
    low10 = float(df.tail(10).select(pl.min("low")).item())
    buy = close

    candidates = []
    if low5 > 0:
        candidates.append(low5 * 0.99)
    if low10 > 0:
        candidates.append(low10 * 0.99)
    if ma60 > 0:
        candidates.append(ma60 * 0.995)

    # 只保留低于买入价的止损位，取最靠近买入价的那个
    valid_stops = [x for x in candidates if 0 < x < buy]
    if not valid_stops:
        return {"valid": False, "reason": "没有有效止损位"}

    stop = max(valid_stops)
    risk_pct = (buy - stop) / buy
    if risk_pct <= 0:
        return {"valid": False, "reason": "止损风险异常"}
    if risk_pct > MAX_STOP_DISTANCE:
        return {"valid": False, "reason": f"止损距离过大：{risk_pct:.1%}"}

    max_position = MAX_POSITION_STRONG if market_state == "strong" else MAX_POSITION_NEUTRAL
    position = min(max_position, account_risk / risk_pct)
    target1 = buy * 1.10
    target2 = buy * 1.18

    return {
        "valid": True,
        "buy": round(buy, 2),
        "stop": round(stop, 2),
        "risk_pct": round(risk_pct, 4),
        "position": round(position, 3),
        "target1": round(target1, 2),
        "target2": round(target2, 2),
        "max_chase_pct": 0.03,
        "note": "尾盘确认价附近；若次日高开超过3%，放弃追入",
    }
