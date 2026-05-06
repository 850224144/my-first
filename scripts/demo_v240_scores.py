#!/usr/bin/env python3
"""
v2.4.0 本地评分演示。
不请求网络，只验证评分模块能跑。
"""

from pathlib import Path
import sys
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.weekly import score_weekly_trend
from core.sector import score_sector_for_stock
from core.yuanjun import score_yuanjun
from core.signal_engine import build_observation_signal


def make_daily_bars():
    today = dt.date.today()
    bars = []
    price = 30.0
    for i in range(120):
        d = today - dt.timedelta(days=180-i)
        if d.weekday() >= 5:
            continue
        price *= 1.002
        open_ = price * 0.99
        close = price
        high = price * 1.02
        low = price * 0.98
        volume = 100000 + i * 800
        bars.append({
            "trade_date": d.isoformat(),
            "open": round(open_, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "volume": volume,
            "amount": volume * close * 100,
        })
    # 最后一根做援军阳线
    if len(bars) >= 2:
        prev = bars[-2]
        bars[-1]["open"] = round(prev["close"] * 1.01, 2)
        bars[-1]["close"] = round(prev["close"] * 1.055, 2)
        bars[-1]["high"] = round(prev["close"] * 1.06, 2)
        bars[-1]["low"] = round(prev["close"] * 1.005, 2)
        bars[-1]["volume"] = prev["volume"] * 2.2
    return bars


def main():
    bars = make_daily_bars()

    core_pools = {
        "limit_up": [
            {"symbol": "603019.SH", "stock_chi_name": "中科曙光", "limit_up_days": 1, "first_limit_up_time": 93000, "surge_reason": "AI算力"},
            {"symbol": "000001.SZ", "stock_chi_name": "示例A", "limit_up_days": 1, "surge_reason": "AI算力"},
            {"symbol": "000002.SZ", "stock_chi_name": "示例B", "limit_up_days": 1, "surge_reason": "AI算力"},
            {"symbol": "000003.SZ", "stock_chi_name": "示例C", "limit_up_days": 1, "surge_reason": "AI算力"},
            {"symbol": "000004.SZ", "stock_chi_name": "示例D", "limit_up_days": 1, "surge_reason": "AI算力"},
        ],
        "continuous_limit_up": [
            {"symbol": "603019.SH", "stock_chi_name": "中科曙光", "limit_up_days": 2, "surge_reason": "AI算力"},
        ],
        "strong_stock": [
            {"symbol": "603019.SH", "stock_chi_name": "中科曙光", "surge_reason": "AI算力"},
        ],
        "limit_up_broken": [],
        "limit_down": [],
    }

    weekly = score_weekly_trend(bars)
    sector = score_sector_for_stock("603019.SH", core_pools=core_pools)

    yuanjun = score_yuanjun(
        daily_bars=bars,
        theme_heat_score=sector["sector_score"],
        leader_score=sector["leader_score"],
        leader_type=sector["leader_type"],
        mainline_days=4,
        broken_count=2,
        limit_down_count=1,
        sector_follow_limit_up_count=5,
        previous_divergence_count=0,
        leader_resilient=True,
        stage_gain_pct=45,
    )

    candidate = {
        "symbol": "603019.SH",
        "daily_2buy_score": 83,
        "risk_pct": 5.8,
        "current_price": bars[-1]["close"],
        "trigger_price": bars[-1]["close"] * 0.995,
        "fresh_quote": True,
        **weekly,
        **sector,
        **yuanjun,
    }
    signal = build_observation_signal(candidate)

    print("weekly:", weekly)
    print("sector:", sector)
    print("yuanjun:", yuanjun)
    print("signal:", signal)


if __name__ == "__main__":
    main()
