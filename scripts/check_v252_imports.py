#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.production_guard_v252 import attach_demo_mode, attach_real_mode, ProductionGuardError, assert_not_demo_payload
    from core.project_data_adapter_v252 import inspect_project_real_data
    from core.pipeline_v252 import process_tail_candidate_v252

    demo = attach_demo_mode({"symbol": "603019.SH"})
    try:
        assert_not_demo_payload(demo)
        raise AssertionError("demo payload should be blocked")
    except ProductionGuardError:
        pass

    candidate = attach_real_mode({
        "symbol": "603019.SH",
        "stock_name": "中科曙光",
        "daily_2buy_score": 86,
        "risk_pct": 5.5,
        "current_price": 36.8,
        "trigger_price": 36.5,
        "fresh_quote": True,
        "weekly_score": 72,
        "sector_score": 75,
        "leader_score": 78,
        "yuanjun_score": 82,
        "rescue_candle_score": 90,
        "yj_stop_loss": 34.5,
        "yj_candle_mid": 35.5,
    })

    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        out = process_tail_candidate_v252(
            candidate,
            trade_date="2026-04-30",
            trading_days=["2026-04-30", "2026-05-06"],
            db_path=f.name,
            persist=True,
        )
        assert out["signal_status"] in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}
        assert out["production_warnings"] == []

    print("v2.5.2 imports OK")
    print("demo guard OK")
    print("real pipeline wrapper OK")

if __name__ == "__main__":
    main()
