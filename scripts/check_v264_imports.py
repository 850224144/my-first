#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.tail_reason_formatter_v264 import explain_tail_row_v264
    from core.tail_confirm_report_v264 import build_tail_confirm_summary_v264, build_tail_daily_section_v264
    from core.tail_confirm_runner_v264 import run_tail_confirm_from_tail_focus_v264
    from core.intraday_pipeline_v264 import run_intraday_tail_pipeline_v264

    row = {
        "symbol": "300750.SZ",
        "signal_status": "REJECTED",
        "current_price": 437.02,
        "trigger_price": 457.64,
        "daily_2buy_score": 72,
        "risk_pct": 5.4,
        "blocking_flags": [],
        "signal_reasons": ["周线较强"],
        "risk_flags": ["周线较强"],
    }
    exp = explain_tail_row_v264(row)
    assert any("价格未突破" in x for x in exp["explain_reasons"])
    assert any("二买总分不足" in x for x in exp["explain_reasons"])

    summary = build_tail_confirm_summary_v264(trade_date="2026-04-30", results=[row])
    section = build_tail_daily_section_v264(summary, [row])
    assert "尾盘确认" in section

    print("v2.6.4 imports OK")
    print("tail reason formatter OK")
    print("tail report v264 OK")
    print("intraday pipeline import OK")

if __name__ == "__main__":
    main()
