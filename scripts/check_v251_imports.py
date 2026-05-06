#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.state_store_v251 import ensure_state_db, upsert_final_signal
    from core.trade_plan_v251 import build_open_recheck_plan, should_create_open_recheck_plan
    from core.message_formatter_v251 import normalize_signal_reasons, format_final_signal_message
    from core.pipeline_v251 import process_tail_candidate_v251, process_open_recheck_v251, process_open_confirm_v251

    candidate = {
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
    }

    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        out = process_tail_candidate_v251(
            candidate,
            trade_date="2026-04-30",
            trading_days=["2026-04-30", "2026-05-06"],
            db_path=f.name,
            persist=True,
        )
        assert out["signal_status"] in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}
        assert out["signal_reasons"], "signal_reasons should not be empty"
        assert "paper_trade_record" in out
        assert "wecom_message" in out

    print("v2.5.1 imports OK")
    print("tail candidate pipeline OK")
    print("state store OK")
    print("message formatter OK")

if __name__ == "__main__":
    main()
