#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.pipeline_v251 import process_tail_candidate_v251, process_open_recheck_v251, process_open_confirm_v251


def main():
    strong_candidate = {
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
        "yj_candle_mid": 35.5,
        "target_1": 40.0,
        "target_2": 45.0,
        "theme_name": "AI算力",
    }

    near_candidate = dict(strong_candidate)
    near_candidate.update({
        "current_price": 36.1,
        "trigger_price": 36.5,
        "daily_2buy_score": 82,
    })

    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        strong = process_tail_candidate_v251(
            strong_candidate,
            trade_date="2026-04-30",
            trading_days=["2026-04-30", "2026-05-06"],
            db_path=f.name,
            persist=True,
        )
        near = process_tail_candidate_v251(
            near_candidate,
            trade_date="2026-04-30",
            trading_days=["2026-04-30", "2026-05-06"],
            db_path=f.name,
            persist=True,
        )

        print("strong signal:")
        print(json.dumps({k: strong.get(k) for k in ["signal_status", "signal_reasons", "paper_trade_record"]}, ensure_ascii=False, indent=2))

        print("\nnear signal:")
        print(json.dumps({k: near.get(k) for k in ["signal_status", "downgrade_flags", "open_recheck_plan"]}, ensure_ascii=False, indent=2))

        if near.get("open_recheck_plan"):
            plan = near["open_recheck_plan"]
            r1 = process_open_recheck_v251(
                plan,
                {"open": 36.6, "price": 36.6, "fresh_quote": True},
                trade_date="2026-05-06",
                db_path=f.name,
                persist=True,
            )
            r2 = process_open_confirm_v251(
                plan,
                {"open": 36.6, "price": 36.7, "fresh_quote": True},
                trade_date="2026-05-06",
                trading_days=["2026-04-30", "2026-05-06", "2026-05-07"],
                db_path=f.name,
                persist=True,
            )
            print("\nopen recheck:")
            print(json.dumps({k: r1.get(k) for k in ["open_status", "reasons", "risk_flags"]}, ensure_ascii=False, indent=2))
            print("\nopen confirm:")
            print(json.dumps({k: r2.get(k) for k in ["open_status", "reasons", "paper_trade_record"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
