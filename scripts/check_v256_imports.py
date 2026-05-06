#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.tail_candidate_diagnostics_v256 import diagnose_tail_candidates, format_tail_diagnosis_report
    from core.xgb_core_pools_v255 import load_xgb_core_pools_v255
    from core.watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255

    sample = [{
        "symbol": "600000.SH",
        "signal_status": "REJECTED",
        "daily_2buy_score": 82,
        "risk_pct": 10,
        "current_price": 10,
        "trigger_price": 10,
        "fresh_quote": True,
        "blocking_flags": ["风险比例过高(risk_pct_too_high)"],
    }]
    d = diagnose_tail_candidates(sample)
    assert d["total"] == 1
    assert d["category_counter"].get("risk_too_high") == 1

    print("v2.5.6 imports OK")
    print("tail diagnosis OK")
    print("xgb pools loader available OK")

if __name__ == "__main__":
    main()
