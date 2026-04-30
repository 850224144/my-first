#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.buy_bridge_v280 import is_buy_row_v280, format_buy_bridge_summary_v280
    from core.open_recheck_plan_v280 import build_open_recheck_plan_v280, build_paper_trade_candidate_v280

    row = {
        "symbol": "600000.SH",
        "stock_name": "测试股",
        "signal_status": "BUY_TRIGGERED",
        "should_write_paper_trade": True,
        "current_price": 10.0,
        "trigger_price": 9.9,
        "risk_pct": 5.5,
    }
    assert is_buy_row_v280(row) is True
    plan = build_open_recheck_plan_v280(row, trade_date="2026-04-30")
    assert plan["plan_type"] == "OPEN_RECHECK"
    rec = build_paper_trade_candidate_v280(row, trade_date="2026-04-30")
    assert rec["paper_status"] == "PENDING_BUY_CONFIRM"

    print("v2.8.0 imports OK")
    print("buy row detection OK")
    print("open recheck plan builder OK")
    print("paper trade candidate builder OK")

if __name__ == "__main__":
    main()
