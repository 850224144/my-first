#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.safe_fields_v262 import normalize_list_field, clean_record_fields, stringify_list
    from core.tail_confirm_report_v262 import build_tail_confirm_summary_v262
    from core.tail_confirm_runner_v262 import run_tail_confirm_from_tail_focus_v262

    raw = "['波动未收敛' '量能未确认']"
    arr = normalize_list_field(raw)
    assert arr == ["波动未收敛", "量能未确认"]
    assert stringify_list(raw) == "波动未收敛，量能未确认"

    rec = clean_record_fields({"risk_reasons": raw})
    assert rec["risk_reasons"] == ["波动未收敛", "量能未确认"]

    s = build_tail_confirm_summary_v262(
        trade_date="2026-04-30",
        results=[{"signal_status": "WATCH_ONLY", "observe_quality": "observe_keep", "should_write_paper_trade": False}],
    )
    assert s["watch_count"] == 1

    print("v2.6.2 imports OK")
    print("safe field normalize OK")
    print("tail confirm report OK")
    print("tail confirm runner import OK")

if __name__ == "__main__":
    main()
