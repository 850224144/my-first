#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.tail_reason_compactor_v265 import compact_reasons, explain_tail_row_v265
    from core.tail_confirm_report_v265 import build_tail_confirm_summary_v265, build_tail_daily_section_v265
    from core.tail_confirm_runner_v265 import run_tail_confirm_from_tail_focus_v265
    from core.intraday_pipeline_v265 import run_intraday_tail_pipeline_v265

    raw = [
        "量能未确认(volume_not_confirm)",
        "volatility_not_contracting",
        "volume_not_confirm",
        "操作建议：等待确认;入场类型：量能未确认;风险等级：偏高",
        "量能未确认",
        "波动未收敛",
    ]
    compact = compact_reasons(raw, max_items=5)
    assert compact.count("量能未确认(volume_not_confirm)") == 1
    assert "波动未收敛(volatility_not_contracting)" in compact

    row = {
        "symbol": "300750.SZ",
        "signal_status": "REJECTED",
        "current_price": 437.02,
        "trigger_price": 457.64,
        "daily_2buy_score": 72,
        "risk_pct": 5.4,
        "signal_reasons": ["周线较强"],
    }
    exp = explain_tail_row_v265(row)
    assert any("价格未突破" in x for x in exp["explain_reasons"])
    assert any("二买总分不足" in x for x in exp["explain_reasons"])

    summary = build_tail_confirm_summary_v265(trade_date="2026-04-30", results=[row])
    section = build_tail_daily_section_v265(summary, [row])
    assert "尾盘确认" in section

    print("v2.6.5 imports OK")
    print("reason compactor OK")
    print("tail report v265 OK")
    print("intraday pipeline v265 import OK")

if __name__ == "__main__":
    main()
