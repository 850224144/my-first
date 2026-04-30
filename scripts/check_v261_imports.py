#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.observe_gate_store_v261 import build_observe_gate_outputs_v261
    from core.tail_focus_loader_v261 import load_tail_focus_v261, tail_focus_symbols_v261
    from core.observe_gate_report_v261 import build_observe_gate_summary_v261, format_observe_gate_summary_md_v261

    summary = build_observe_gate_summary_v261(
        trade_date="2026-04-30",
        quality_rows=[{"symbol": "600000.SH", "observe_quality": "observe_keep", "risk_bucket": "5-8", "signal_status": "WATCH_ONLY"}],
        tail_focus_rows=[{"symbol": "600000.SH", "observe_quality": "observe_keep", "observe_priority": 80}],
        low_priority_rows=[],
    )
    assert summary["tail_focus_count"] == 1
    md = format_observe_gate_summary_md_v261(summary)
    assert "Observe Gate Summary" in md

    print("v2.6.1 imports OK")
    print("observe gate store import OK")
    print("tail focus loader import OK")
    print("summary report OK")

if __name__ == "__main__":
    main()
