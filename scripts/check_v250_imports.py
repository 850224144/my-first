#!/usr/bin/env python3
"""
v2.5.0 导入检查。
运行：
python scripts/check_v250_imports.py
"""

from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from core.final_signal_engine import build_final_signal, is_buy_signal
    from core.open_recheck import open_recheck, open_confirm
    from core.paper_trade_ext import build_paper_trade_record, next_trading_date, is_sellable
    from core.notify_dedupe import should_notify, record_notification
    from core.daily_stats_v250 import build_v250_daily_section

    candidate = {
        "symbol": "603019.SH",
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
    }
    signal = build_final_signal(candidate)
    assert signal["signal_status"] in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}
    assert is_buy_signal(signal)

    plan = {
        "symbol": "603019.SH",
        "planned_buy_price": 36.5,
        "yj_candle_mid": 35.5,
        "yj_stop_loss": 34.5,
    }
    quote = {"open": 36.8, "price": 36.9, "fresh_quote": True}
    r1 = open_recheck(plan, quote)
    r2 = open_confirm(plan, quote)
    assert r1["open_status"] == "OPEN_RECHECK_PASSED"
    assert r2["open_status"] == "OPEN_BUY_TRIGGERED"

    rec = build_paper_trade_record(
        candidate=candidate,
        signal=signal,
        trade_date="2026-04-30",
        entry_type="close_tail",
        trading_days=["2026-04-30", "2026-05-06"],
    )
    assert rec["sellable_date"] == "2026-05-06"

    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        ok, reason = should_notify(f.name, trade_date="2026-04-30", symbol="603019.SH", status="BUY_TRIGGERED")
        assert ok
        record_notification(f.name, trade_date="2026-04-30", symbol="603019.SH", status="BUY_TRIGGERED")
        ok2, reason2 = should_notify(f.name, trade_date="2026-04-30", symbol="603019.SH", status="BUY_TRIGGERED")
        assert not ok2

    section = build_v250_daily_section([{**candidate, **signal}])
    assert "v2.5.0" in section

    print("v2.5.0 imports OK")
    print("final signal OK")
    print("open recheck/confirm OK")
    print("paper trade ext OK")
    print("notify dedupe OK")
    print("daily stats OK")


if __name__ == "__main__":
    main()
