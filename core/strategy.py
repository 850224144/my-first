# core/strategy.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import math

import polars as pl


# ============================================================
# 二买评分模型 V1
# 逻辑：
# 健康上涨 → 缩量回调 → 止跌企稳 → 温和突破确认
#
# 总分：
# 趋势段 25 + 回调段 25 + 企稳段 25 + 确认段 25
#
# 兼容：
# - score_second_buy(df, mode="observe") 返回评分字典
# - is_second_buy(df) 返回 True/False
# - make_trade_plan(df, score_result) 返回交易计划
# ============================================================


def _as_polars(df: Any) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        return df.clone()

    try:
        return pl.from_pandas(df)
    except Exception:
        raise ValueError("strategy.py 只支持 Polars DataFrame 或可转为 Polars 的 DataFrame")


def _prepare_df(df: Any) -> pl.DataFrame:
    """
    标准化日线数据。
    必要字段：
    date/open/high/low/close/volume
    amount 可缺失，缺失时用 close * volume 估算。
    """
    df = _as_polars(df)

    # 统一小写字段名
    rename_map = {}
    for c in df.columns:
        lc = str(c).lower()
        if lc != c:
            rename_map[c] = lc
    if rename_map:
        df = df.rename(rename_map)

    required = ["date", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"缺少必要字段: {missing}")

    if "amount" not in df.columns:
        df = df.with_columns(
            (pl.col("close").cast(pl.Float64, strict=False) * pl.col("volume").cast(pl.Float64, strict=False)).alias("amount")
        )

    df = df.with_columns(
        [
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
        ]
    )

    df = df.drop_nulls(["date", "open", "high", "low", "close", "volume"])
    df = df.filter(
        (pl.col("open") > 0)
        & (pl.col("high") > 0)
        & (pl.col("low") > 0)
        & (pl.col("close") > 0)
        & (pl.col("volume") >= 0)
    )

    df = df.sort("date")

    if len(df) < 120:
        return df

    df = df.with_columns(
        [
            pl.col("close").shift(1).alias("prev_close"),
            pl.col("close").rolling_mean(window_size=5).alias("ma5"),
            pl.col("close").rolling_mean(window_size=10).alias("ma10"),
            pl.col("close").rolling_mean(window_size=20).alias("ma20"),
            pl.col("close").rolling_mean(window_size=60).alias("ma60"),
            pl.col("volume").rolling_mean(window_size=5).alias("vol5"),
            pl.col("volume").rolling_mean(window_size=10).alias("vol10"),
            pl.col("volume").rolling_mean(window_size=20).alias("vol20"),
            pl.col("amount").rolling_mean(window_size=20).alias("amount20"),
        ]
    )

    df = df.with_columns(
        [
            ((pl.col("close") / pl.col("prev_close") - 1) * 100).alias("pct_chg"),
            ((pl.col("high") - pl.col("low")) / pl.col("close") * 100).alias("amp_pct"),
            ((pl.col("close") - pl.col("open")).abs() / (pl.col("high") - pl.col("low") + 1e-9)).alias("body_ratio"),
            ((pl.col("high") - pl.max_horizontal(["open", "close"])) / (pl.col("high") - pl.col("low") + 1e-9)).alias("upper_shadow_ratio"),
        ]
    )

    return df


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, float) and math.isnan(x):
            return default
        return float(x)
    except Exception:
        return default


def _last_value(df: pl.DataFrame, col: str, default: float = 0.0) -> float:
    if col not in df.columns or df.is_empty():
        return default
    return _safe_float(df[col][-1], default)


def _max_consecutive_limit_up(df: pl.DataFrame, start: int, end: int) -> int:
    """
    粗略判断连续涨停。
    不区分 10% / 20% 涨跌幅制度，统一用 9.5% 作为风险识别。
    """
    pct = df["pct_chg"].to_list() if "pct_chg" in df.columns else []
    max_run = 0
    run = 0

    for i in range(max(1, start), min(end, len(df))):
        v = _safe_float(pct[i], 0)
        if v >= 9.5:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0

    return max_run


def _has_one_word_limit_boards(df: pl.DataFrame, start: int, end: int) -> bool:
    """
    粗略排除连续一字板。
    high≈low 且涨幅较大，视作一字板。
    """
    highs = df["high"].to_list()
    lows = df["low"].to_list()
    pct = df["pct_chg"].to_list() if "pct_chg" in df.columns else []

    one_word_count = 0

    for i in range(max(1, start), min(end, len(df))):
        h = _safe_float(highs[i])
        l = _safe_float(lows[i])
        p = _safe_float(pct[i], 0)

        if h > 0 and abs(h - l) / h < 0.002 and p >= 9.5:
            one_word_count += 1

    return one_word_count >= 2


def _find_structure(df: pl.DataFrame) -> Dict[str, Any]:
    """
    找最近 90 日内的一段上涨 + 回调结构。
    返回：
    - start_idx: 起涨低点
    - peak_idx: 阶段高点
    - trough_idx: 回调低点
    - rise_pct
    - pullback_pct
    - pullback_days
    """
    n = len(df)
    if n < 120:
        return {"ok": False, "reason": "data_not_enough"}

    lookback = min(90, n)
    base = n - lookback

    highs = df["high"].to_list()
    lows = df["low"].to_list()

    # 阶段高点：最近 90 日最高点
    peak_rel = max(range(lookback), key=lambda i: _safe_float(highs[base + i], 0))
    peak_idx = base + peak_rel
    peak_high = _safe_float(highs[peak_idx], 0)

    if peak_idx <= base + 10:
        return {"ok": False, "reason": "peak_too_early"}

    # 起涨低点：高点之前最低点
    start_idx = min(range(base, peak_idx + 1), key=lambda i: _safe_float(lows[i], 10**18))
    start_low = _safe_float(lows[start_idx], 0)

    if start_low <= 0 or peak_high <= 0:
        return {"ok": False, "reason": "invalid_price"}

    rise_pct = (peak_high / start_low - 1) * 100

    # 高点之后到当前的回调段
    if peak_idx >= n - 2:
        trough_idx = peak_idx
        trough_low = _safe_float(lows[peak_idx], 0)
        pullback_days = 0
    else:
        trough_idx = min(range(peak_idx + 1, n), key=lambda i: _safe_float(lows[i], 10**18))
        trough_low = _safe_float(lows[trough_idx], 0)
        pullback_days = n - peak_idx - 1

    pullback_pct = (peak_high / trough_low - 1) * 100 if trough_low > 0 else 0

    return {
        "ok": True,
        "start_idx": start_idx,
        "peak_idx": peak_idx,
        "trough_idx": trough_idx,
        "start_low": start_low,
        "peak_high": peak_high,
        "trough_low": trough_low,
        "rise_pct": rise_pct,
        "pullback_pct": pullback_pct,
        "pullback_days": pullback_days,
    }


def _score_trend(df: pl.DataFrame, st: Dict[str, Any]) -> Tuple[int, List[str], List[str]]:
    score = 0
    reasons: List[str] = []
    warnings: List[str] = []

    if not st.get("ok"):
        return 0, ["趋势结构识别失败"], ["trend_structure_failed"]

    rise_pct = st["rise_pct"]
    start_idx = st["start_idx"]
    peak_idx = st["peak_idx"]

    # 1. 涨幅 10 分
    if rise_pct < 20:
        s = 0
        warnings.append("trend_too_weak")
    elif 20 <= rise_pct < 30:
        s = 5
    elif 30 <= rise_pct <= 80:
        s = 10
    elif 80 < rise_pct <= 100:
        s = 8
    else:
        s = 4
        warnings.append("trend_too_hot")
    score += s
    reasons.append(f"趋势涨幅 {rise_pct:.1f}%：{s}/10")

    # 2. 均线结构 5 分
    ma20 = _last_value(df, "ma20")
    ma60 = _last_value(df, "ma60")
    ma60_prev = _safe_float(df["ma60"][-20], ma60) if "ma60" in df.columns and len(df) >= 80 else ma60

    if ma20 > ma60 and ma60 > ma60_prev:
        s = 5
    elif ma20 > ma60:
        s = 3
    elif abs(ma20 - ma60) / ma60 < 0.03 if ma60 else False:
        s = 1
    else:
        s = 0
        warnings.append("ma_structure_weak")
    score += s
    reasons.append(f"均线结构 ma20={ma20:.2f}, ma60={ma60:.2f}：{s}/5")

    # 3. 上涨质量 5 分
    max_lu = _max_consecutive_limit_up(df, start_idx, peak_idx + 1)
    one_word = _has_one_word_limit_boards(df, start_idx, peak_idx + 1)

    closes = df["close"].to_list()
    up_days = 0
    total_days = max(1, peak_idx - start_idx)
    for i in range(start_idx + 1, peak_idx + 1):
        if _safe_float(closes[i]) > _safe_float(closes[i - 1]):
            up_days += 1

    up_ratio = up_days / total_days

    if max_lu >= 3 or one_word:
        s = 0
        warnings.append("limit_up_speculation")
    elif up_ratio >= 0.55:
        s = 5
    elif up_ratio >= 0.45:
        s = 3
    else:
        s = 2
    score += s
    reasons.append(f"上涨质量 up_ratio={up_ratio:.2f}, 连续涨停={max_lu}：{s}/5")

    # 4. 上涨量能 5 分
    vols = df["volume"].to_list()
    mid = start_idx + max(1, (peak_idx - start_idx) // 2)

    vol_early = sum(_safe_float(v) for v in vols[start_idx:mid]) / max(1, mid - start_idx)
    vol_late = sum(_safe_float(v) for v in vols[mid:peak_idx + 1]) / max(1, peak_idx + 1 - mid)

    ratio = vol_late / vol_early if vol_early > 0 else 1

    if 1.05 <= ratio <= 2.2:
        s = 5
    elif 0.85 <= ratio < 1.05:
        s = 3
    elif ratio < 0.85:
        s = 1
        warnings.append("rise_without_volume")
    else:
        s = 0
        warnings.append("volume_blowoff")
    score += s
    reasons.append(f"上涨量能 ratio={ratio:.2f}：{s}/5")

    return score, reasons, warnings


def _score_pullback(df: pl.DataFrame, st: Dict[str, Any]) -> Tuple[int, List[str], List[str]]:
    score = 0
    reasons: List[str] = []
    warnings: List[str] = []

    if not st.get("ok"):
        return 0, ["回调结构识别失败"], ["pullback_structure_failed"]

    pullback_pct = st["pullback_pct"]
    pullback_days = st["pullback_days"]
    start_low = st["start_low"]
    trough_low = st["trough_low"]
    start_idx = st["start_idx"]
    peak_idx = st["peak_idx"]

    # 1. 回撤幅度 10 分
    if 8 <= pullback_pct <= 15:
        s = 10
    elif 15 < pullback_pct <= 25:
        s = 8
    elif 5 <= pullback_pct < 8:
        s = 5
    elif pullback_pct < 5:
        s = 2
        warnings.append("pullback_too_shallow")
    else:
        s = 0
        warnings.append("pullback_too_deep")
    score += s
    reasons.append(f"回撤幅度 {pullback_pct:.1f}%：{s}/10")

    # 2. 回调时间 5 分
    if 5 <= pullback_days <= 15:
        s = 5
    elif 15 < pullback_days <= 25:
        s = 4
    elif 3 <= pullback_days < 5:
        s = 2
        warnings.append("pullback_too_fast")
    else:
        s = 1 if pullback_days > 0 else 0
        warnings.append("pullback_days_not_ideal")
    score += s
    reasons.append(f"回调天数 {pullback_days}：{s}/5")

    # 3. 缩量回调 5 分
    vols = df["volume"].to_list()
    trend_vol = sum(_safe_float(v) for v in vols[start_idx:peak_idx + 1]) / max(1, peak_idx + 1 - start_idx)
    pb_vol = sum(_safe_float(v) for v in vols[peak_idx + 1:]) / max(1, len(vols) - peak_idx - 1)

    ratio = pb_vol / trend_vol if trend_vol > 0 and pb_vol > 0 else 1

    if ratio < 0.8:
        s = 5
    elif ratio < 1.05:
        s = 3
    else:
        s = 0
        warnings.append("pullback_volume_too_high")
    score += s
    reasons.append(f"回调量能 ratio={ratio:.2f}：{s}/5")

    # 4. 结构完整性 5 分
    ma60 = _last_value(df, "ma60")
    latest_close = _last_value(df, "close")

    if trough_low >= ma60 * 0.98 and trough_low > start_low:
        s = 5
    elif trough_low >= ma60 * 0.95 and latest_close >= ma60:
        s = 3
        warnings.append("ma60_slight_break")
    elif trough_low <= start_low:
        s = 0
        warnings.append("break_start_low")
    else:
        s = 2
    score += s
    reasons.append(f"结构完整 trough={trough_low:.2f}, ma60={ma60:.2f}, start={start_low:.2f}：{s}/5")

    return score, reasons, warnings


def _score_stabilize(df: pl.DataFrame, st: Dict[str, Any]) -> Tuple[int, List[str], List[str]]:
    score = 0
    reasons: List[str] = []
    warnings: List[str] = []

    if len(df) < 30:
        return 0, ["企稳数据不足"], ["stabilize_data_not_enough"]

    lows = df["low"].to_list()
    highs = df["high"].to_list()
    vols = df["volume"].to_list()

    recent_3_low = min(_safe_float(x, 10**18) for x in lows[-3:])
    recent_5_low = min(_safe_float(x, 10**18) for x in lows[-5:])
    recent_7_low = min(_safe_float(x, 10**18) for x in lows[-7:])
    prev_7_low = min(_safe_float(x, 10**18) for x in lows[-14:-7]) if len(lows) >= 14 else recent_7_low

    # 1. 是否止跌 10 分
    if recent_7_low >= prev_7_low:
        s = 10
    elif recent_3_low >= recent_7_low:
        s = 5
    else:
        s = 0
        warnings.append("still_making_new_low")
    score += s
    reasons.append(f"止跌 recent7_low={recent_7_low:.2f}, prev7_low={prev_7_low:.2f}：{s}/10")

    # 2. 低点抬高 5 分
    low_1 = min(_safe_float(x, 10**18) for x in lows[-3:])
    low_2 = min(_safe_float(x, 10**18) for x in lows[-6:-3])
    low_3 = min(_safe_float(x, 10**18) for x in lows[-9:-6])

    if low_1 > low_2 > low_3:
        s = 5
    elif low_1 >= low_2 * 0.995:
        s = 3
    else:
        s = 0
        warnings.append("lows_not_rising")
    score += s
    reasons.append(f"低点抬高 low1={low_1:.2f}, low2={low_2:.2f}, low3={low_3:.2f}：{s}/5")

    # 3. 波动收敛 5 分
    def avg_amp(start: int, end: int) -> float:
        vals = []
        for h, l in zip(highs[start:end], lows[start:end]):
            h = _safe_float(h)
            l = _safe_float(l)
            if h > 0:
                vals.append((h - l) / h)
        return sum(vals) / len(vals) if vals else 0

    amp_recent = avg_amp(-5, len(df))
    amp_prev = avg_amp(-15, -5)

    if amp_prev > 0 and amp_recent < amp_prev * 0.75:
        s = 5
    elif amp_prev > 0 and amp_recent < amp_prev * 1.05:
        s = 3
    else:
        s = 0
        warnings.append("volatility_not_contracting")
    score += s
    reasons.append(f"波动收敛 recent={amp_recent:.3f}, prev={amp_prev:.3f}：{s}/5")

    # 4. 量能 5 分
    vol_recent = sum(_safe_float(v) for v in vols[-5:]) / 5
    vol20 = _last_value(df, "vol20")

    ratio = vol_recent / vol20 if vol20 > 0 else 1

    if ratio < 0.85:
        s = 5
    elif ratio <= 1.05:
        s = 3
    else:
        s = 0
        warnings.append("stabilize_volume_high")
    score += s
    reasons.append(f"企稳量能 ratio={ratio:.2f}：{s}/5")

    return score, reasons, warnings


def _score_confirm(df: pl.DataFrame, mode: str = "observe") -> Tuple[int, List[str], List[str]]:
    score = 0
    reasons: List[str] = []
    warnings: List[str] = []

    if len(df) < 30:
        return 0, ["确认数据不足"], ["confirm_data_not_enough"]

    close = _last_value(df, "close")
    open_ = _last_value(df, "open")
    high = _last_value(df, "high")
    low = _last_value(df, "low")
    prev_close = _last_value(df, "prev_close")
    pct = _last_value(df, "pct_chg")
    vol = _last_value(df, "volume")
    vol20 = _last_value(df, "vol20")
    body_ratio = _last_value(df, "body_ratio")
    upper_shadow = _last_value(df, "upper_shadow_ratio")

    highs = df["high"].to_list()
    platform_high = max(_safe_float(x) for x in highs[-8:-1]) if len(highs) >= 8 else max(_safe_float(x) for x in highs[:-1])

    # 1. 突破 10 分
    if close > platform_high:
        s = 10
    elif mode == "observe" and close >= platform_high * 0.985:
        s = 5
        warnings.append("near_breakout")
    else:
        s = 0
        warnings.append("no_breakout")
    score += s
    reasons.append(f"突破平台 close={close:.2f}, platform={platform_high:.2f}：{s}/10")

    # 2. 涨幅 5 分
    if 1.5 <= pct <= 5:
        s = 5
    elif 5 < pct <= 7:
        s = 4
    elif pct > 7:
        s = 2
        warnings.append("too_hot_today")
    elif mode == "observe" and 0.5 <= pct < 1.5:
        s = 3
    else:
        s = 1 if pct > 0 else 0
        warnings.append("today_pct_weak")
    score += s
    reasons.append(f"当日涨幅 {pct:.2f}%：{s}/5")

    # 3. 成交量 5 分
    ratio = vol / vol20 if vol20 > 0 else 1

    if 1.1 <= ratio <= 1.8:
        s = 5
    elif 0.9 <= ratio < 1.1:
        s = 3
    elif 1.8 < ratio <= 2.5:
        s = 2
        warnings.append("volume_too_high")
    else:
        s = 0
        warnings.append("volume_not_confirm")
    score += s
    reasons.append(f"确认量能 ratio={ratio:.2f}：{s}/5")

    # 4. K线质量 5 分
    is_limit = pct >= 9.5

    if close > open_ and body_ratio >= 0.5 and upper_shadow <= 0.35 and not is_limit:
        s = 5
    elif close > open_ and upper_shadow <= 0.45:
        s = 3
    elif is_limit:
        s = 2
        warnings.append("limit_up_not_ideal")
    else:
        s = 0
        warnings.append("kline_quality_weak")
    score += s
    reasons.append(f"K线质量 body={body_ratio:.2f}, upper={upper_shadow:.2f}：{s}/5")

    return score, reasons, warnings


def _veto_checks(df: pl.DataFrame, st: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    强制否决条件。
    """
    veto: List[str] = []

    if len(df) < 250:
        veto.append("上市/数据不足250根")

    if not st.get("ok"):
        veto.append(f"结构识别失败: {st.get('reason')}")

    last_close = _last_value(df, "close")
    if last_close < 3:
        veto.append("价格低于3元")

    amount20 = _last_value(df, "amount20")
    if amount20 > 0 and amount20 < 80_000_000:
        veto.append("20日均成交额低于8000万")

    if st.get("ok"):
        max_lu = _max_consecutive_limit_up(df, st["start_idx"], st["peak_idx"] + 1)
        if max_lu >= 3:
            veto.append("连续涨停>=3，疑似妖股")

        if st.get("pullback_pct", 0) > 30:
            veto.append("回撤超过30%，结构破坏")

        if st.get("trough_low", 0) <= st.get("start_low", 0):
            veto.append("跌破起涨点")

    return len(veto) > 0, veto


def score_second_buy(df: Any, mode: str = "observe") -> Optional[Dict[str, Any]]:
    """
    二买评分入口。

    mode:
    - observe: 盘中观察，允许接近突破
    - tail_confirm: 尾盘确认，要求更强确认
    - after_close: 收盘复盘，要求更完整日线确认

    返回：
    {
        "total_score": int,
        "signal": "ignore/weak_observe/observe/strong_observe/confirm",
        "trend_score": int,
        "pullback_score": int,
        "stabilize_score": int,
        "confirm_score": int,
        "reasons": list[str],
        "warnings": list[str],
        "veto": bool,
        "veto_reasons": list[str],
    }
    """
    try:
        df = _prepare_df(df)
    except Exception as e:
        return {
            "total_score": 0,
            "signal": "ignore",
            "trend_score": 0,
            "pullback_score": 0,
            "stabilize_score": 0,
            "confirm_score": 0,
            "reasons": [f"数据预处理失败: {e}"],
            "warnings": ["data_prepare_failed"],
            "veto": True,
            "veto_reasons": [str(e)],
        }

    if df.is_empty() or len(df) < 120:
        return None

    st = _find_structure(df)

    veto, veto_reasons = _veto_checks(df, st)

    trend_score, trend_reasons, trend_warn = _score_trend(df, st)
    pullback_score, pullback_reasons, pullback_warn = _score_pullback(df, st)
    stabilize_score, stabilize_reasons, stabilize_warn = _score_stabilize(df, st)
    confirm_score, confirm_reasons, confirm_warn = _score_confirm(df, mode=mode)

    total = trend_score + pullback_score + stabilize_score + confirm_score

    warnings = trend_warn + pullback_warn + stabilize_warn + confirm_warn

    if veto:
        # 强制否决时保留原始评分，但实际 total_score 归零，方便 run_scan 过滤。
        raw_total = total
        total = 0
    else:
        raw_total = total

    if total >= 90:
        signal = "confirm"
    elif total >= 80:
        signal = "strong_observe" if mode == "observe" else "confirm"
    elif total >= 70:
        signal = "observe"
    elif total >= 60:
        signal = "weak_observe"
    else:
        signal = "ignore"

    return {
        "total_score": int(total),
        "raw_score": int(raw_total),
        "signal": signal,
        "trend_score": int(trend_score),
        "pullback_score": int(pullback_score),
        "stabilize_score": int(stabilize_score),
        "confirm_score": int(confirm_score),
        "reasons": trend_reasons + pullback_reasons + stabilize_reasons + confirm_reasons,
        "warnings": warnings,
        "veto": veto,
        "veto_reasons": veto_reasons,
        "structure": {
            k: v for k, v in st.items()
            if k in [
                "start_idx",
                "peak_idx",
                "trough_idx",
                "rise_pct",
                "pullback_pct",
                "pullback_days",
                "start_low",
                "peak_high",
                "trough_low",
            ]
        },
    }


def is_second_buy(df: Any, mode: str = "observe") -> bool:
    """
    兼容旧版本入口。
    True 表示达到确认级别。
    """
    res = score_second_buy(df, mode=mode)
    if not res:
        return False

    return (
        not res.get("veto", False)
        and res.get("total_score", 0) >= 80
    )


def make_trade_plan(df: Any, score_result: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    根据当前日线和评分结果生成交易计划 V2。

    核心原则：
    1. 止损用真实结构止损，不再统一压成 8%
    2. 风险过大则只观察，不给积极开仓建议
    3. 盘中观察信号不等于立即买入
    """
    try:
        df = _prepare_df(df)
    except Exception:
        return None

    if df.is_empty() or len(df) < 60:
        return None

    close = _last_value(df, "close")
    high = _last_value(df, "high")
    low = _last_value(df, "low")
    ma5 = _last_value(df, "ma5")
    ma10 = _last_value(df, "ma10")

    if close <= 0:
        return None

    lows = df["low"].to_list()
    highs = df["high"].to_list()

    recent_low_5 = min(_safe_float(x, close) for x in lows[-5:])
    recent_low_10 = min(_safe_float(x, close) for x in lows[-10:])
    recent_low_20 = min(_safe_float(x, close) for x in lows[-20:])

    platform_high = (
        max(_safe_float(x, close) for x in highs[-8:-1])
        if len(highs) >= 8
        else high
    )

    score = 0
    signal = "unknown"
    warnings: List[str] = []
    veto_reasons: List[str] = []

    if isinstance(score_result, dict):
        score = int(score_result.get("total_score", 0) or 0)
        signal = score_result.get("signal", "unknown")
        warnings = score_result.get("warnings", []) or []
        veto_reasons = score_result.get("veto_reasons", []) or []

    # ===== 1. 入场价与触发价 =====
    entry_price = close

    # 如果还没真正突破，则触发价用平台高点
    trigger_price = max(close, platform_high)

    # 如果当天过热，不建议追现价
    too_hot = "too_hot_today" in warnings
    volume_not_confirm = "volume_not_confirm" in warnings
    near_breakout = "near_breakout" in warnings

    # ===== 2. 真实结构止损 =====
    # 优先用最近10日低点下方2%作为结构止损
    structure_stop = recent_low_10 * 0.98

    # 如果最近10日低点太近，则参考20日低点
    # 防止止损太浅，正常波动就被洗掉
    shallow_stop = close * 0.975
    if structure_stop > shallow_stop:
        structure_stop = min(recent_low_20 * 0.98, structure_stop)

    stop_loss = structure_stop

    if stop_loss <= 0 or stop_loss >= close:
        return None

    risk_pct = (close - stop_loss) / close * 100

    # ===== 3. 风险分级 =====
    if risk_pct <= 3:
        risk_level = "低"
    elif risk_pct <= 5:
        risk_level = "正常"
    elif risk_pct <= 8:
        risk_level = "偏高"
    else:
        risk_level = "过高"

    # ===== 4. 仓位逻辑 =====
    action = "观察"
    position = "观察，不建议开仓"
    entry_type = "观察候选"

    if veto_reasons:
        action = "放弃"
        position = "不参与"
        entry_type = "强制否决"
    elif risk_pct > 8:
        action = "只观察"
        position = "不建议开仓"
        entry_type = "风险过大，等待回踩"
    elif too_hot:
        action = "只观察"
        position = "不追高"
        entry_type = "当日涨幅过大，等回踩确认"
    elif volume_not_confirm and score < 85:
        action = "等待确认"
        position = "观察，不急开仓"
        entry_type = "量能未确认"
    elif near_breakout:
        action = "等待突破"
        position = "10%-15%"
        entry_type = "突破触发价后再考虑"
    else:
        if score >= 90 and risk_pct <= 5:
            action = "可执行"
            position = "30%-40%"
            entry_type = "确认买点"
        elif score >= 80 and risk_pct <= 5:
            action = "可轻仓执行"
            position = "20%-30%"
            entry_type = "强观察/轻仓试错"
        elif score >= 70 and risk_pct <= 5:
            action = "观察为主"
            position = "10%-20%"
            entry_type = "观察候选"
        elif score >= 70 and risk_pct <= 8:
            action = "轻仓观察"
            position = "5%-10%"
            entry_type = "风险偏高，只能轻仓"
        else:
            action = "观察"
            position = "观察，不建议开仓"
            entry_type = "分数或风险不理想"

    # ===== 5. 目标位 =====
    risk_abs = close - stop_loss

    take_profit_1 = close + risk_abs * 1.5
    take_profit_2 = close + risk_abs * 2.5

    # 如果目标位过高，给出保守提示
    target1_pct = (take_profit_1 / close - 1) * 100
    target2_pct = (take_profit_2 / close - 1) * 100

    # ===== 6. 备注 =====
    notes = []

    notes.append(f"操作建议：{action}")
    notes.append(f"入场类型：{entry_type}")
    notes.append(f"风险等级：{risk_level}")

    if risk_pct > 8:
        notes.append("结构止损距离过远，当前不适合直接开仓。")
    elif risk_pct > 5:
        notes.append("风险偏高，只适合小仓位观察。")

    if too_hot:
        notes.append("当日涨幅偏大，避免追高，等待回踩或次日确认。")

    if volume_not_confirm:
        notes.append("量能未确认，突破有效性不足。")

    if near_breakout:
        notes.append("尚未完全突破，需等待触发价确认。")

    if "volatility_not_contracting" in warnings:
        notes.append("波动未明显收敛，可能仍在震荡。")

    if "lows_not_rising" in warnings:
        notes.append("低点抬高不充分，止跌结构一般。")

    if "stabilize_volume_high" in warnings:
        notes.append("企稳阶段量能偏高，需防止分歧。")

    return {
        "entry_price": round(entry_price, 2),
        "trigger_price": round(trigger_price, 2),
        "stop_loss": round(stop_loss, 2),
        "take_profit_1": round(take_profit_1, 2),
        "take_profit_2": round(take_profit_2, 2),
        "risk_pct": round(risk_pct, 2),
        "risk_level": risk_level,
        "target1_pct": round(target1_pct, 2),
        "target2_pct": round(target2_pct, 2),
        "position_suggestion": position,
        "action": action,
        "entry_type": entry_type,
        "signal": signal,
        "invalid_condition": f"跌破结构止损 {round(stop_loss, 2)}，或放量跌破最近10日低点",
        "note": "；".join(notes),
    }


# 兼容可能存在的旧名称
def trade_plan(df: Any, score_result: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    return make_trade_plan(df, score_result)


__all__ = [
    "score_second_buy",
    "is_second_buy",
    "make_trade_plan",
    "trade_plan",
]