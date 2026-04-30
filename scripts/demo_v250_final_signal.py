#!/usr/bin/env python3
"""
v2.5.0 最终信号演示。
不请求网络。
"""

from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.final_signal_engine import build_final_signal
from core.paper_trade_ext import build_paper_trade_record


def main():
    candidate = {
        "symbol": "603019.SH",
        "stock_name": "中科曙光",
        "daily_2buy_score": 86,
        "risk_pct": 5.5,
        "current_price": 36.8,
        "trigger_price": 36.5,
        "fresh_quote": True,
        "market_state": "normal",
        "weekly_score": 72,
        "weekly_state": "weekly_uptrend",
        "sector_score": 75,
        "sector_state": "active_mainline",
        "leader_score": 78,
        "leader_type": "turnover_leader",
        "yuanjun_score": 82,
        "yuanjun_state": "YJ_CONFIRMED",
        "rescue_candle_score": 90,
        "yj_stop_loss": 34.5,
        "target_1": 40.0,
        "target_2": 45.0,
        "theme_name": "AI算力",
    }

    signal = build_final_signal(candidate)
    print("final_signal:")
    print(json.dumps(signal, ensure_ascii=False, indent=2))

    if signal["should_write_paper_trade"]:
        record = build_paper_trade_record(
            candidate=candidate,
            signal=signal,
            trade_date="2026-04-30",
            entry_type=signal["entry_type"],
            trading_days=["2026-04-30", "2026-05-06"],
        )
        print("\npaper_trade_record:")
        print(json.dumps(record, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
