#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.risk_quality_v260 import classify_observe_quality, summarize_observe_quality
    from core.stop_loss_diagnostics_v260 import diagnose_stop_loss_for_candidate
    from core.observe_gate_v260 import apply_observe_gate_v260

    item = {
        "symbol": "600000.SH",
        "daily_2buy_score": 82,
        "risk_pct": 7.5,
        "current_price": 10.0,
        "trigger_price": 9.9,
        "fresh_quote": True,
        "risk_flags": [],
    }
    q = classify_observe_quality(item)
    assert q["observe_quality"] in {"tail_ready", "observe_keep"}
    d = diagnose_stop_loss_for_candidate({**item, "stop_loss": 9.2})
    assert d["current_risk_pct"] == 7.5
    g = apply_observe_gate_v260([item])
    assert len(g) == 1

    print("v2.6.0 imports OK")
    print("risk quality OK")
    print("stop loss diagnostics OK")
    print("observe gate OK")

if __name__ == "__main__":
    main()
