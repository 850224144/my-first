#!/usr/bin/env python3
"""
v2.5.0 开盘复核演示。
不请求网络。
"""

from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.open_recheck import open_recheck, open_confirm
from core.paper_trade_ext import build_paper_trade_record


def main():
    trade_plan = {
        "symbol": "603019.SH",
        "stock_name": "中科曙光",
        "plan_type": "OPEN_RECHECK",
        "planned_buy_price": 36.5,
        "yj_candle_mid": 35.5,
        "yj_stop_loss": 34.5,
        "target_1": 40.0,
        "target_2": 45.0,
        "risk_pct": 5.5,
        "daily_2buy_score": 85,
        "sector_score": 75,
        "leader_score": 78,
        "weekly_score": 72,
        "yuanjun_score": 82,
        "theme_name": "AI算力",
    }
    quote_0926 = {"open": 36.8, "price": 36.8, "fresh_quote": True}
    quote_0930 = {"open": 36.8, "price": 36.9, "fresh_quote": True}

    recheck = open_recheck(trade_plan, quote_0926, market_state="normal")
    confirm = open_confirm(trade_plan, quote_0930, market_state="normal")

    print("open_recheck:")
    print(json.dumps(recheck, ensure_ascii=False, indent=2))
    print("\nopen_confirm:")
    print(json.dumps(confirm, ensure_ascii=False, indent=2))

    if confirm["should_write_paper_trade"]:
        record = build_paper_trade_record(
            candidate=trade_plan,
            signal={**confirm, "signal_status": confirm["open_status"]},
            trade_date="2026-05-06",
            entry_type="next_open",
            trading_days=["2026-04-30", "2026-05-06", "2026-05-07"],
        )
        print("\nopen paper_trade_record:")
        print(json.dumps(record, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
