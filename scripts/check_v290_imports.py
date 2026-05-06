#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.daily_report_aggregator_v290 import format_daily_report_md_v290
    from core.wecom_sender_v290 import split_text_by_bytes_v290
    from core.system_health_v290 import build_system_health_v290

    md = format_daily_report_md_v290(
        trade_date="2026-04-30",
        observe_json={"total": 1, "tail_focus_count": 1, "low_priority_count": 0},
        tail_json={"buy_count": 0, "watch_count": 1, "rejected_count": 0},
        buy_json={},
        observe_md="",
        tail_md="## 尾盘确认\n- 测试",
        buy_bridge_md="",
        positions_rows=0,
        paper_candidates_rows=0,
        open_recheck_rows=0,
        health={"status": "ok", "missing_required": []},
    )
    assert "A股二买交易助手日报" in md
    chunks = split_text_by_bytes_v290("a" * 8000, max_bytes=3500)
    assert len(chunks) >= 2
    health = build_system_health_v290(ROOT)
    assert "status" in health

    print("v2.9.0 imports OK")
    print("daily report aggregator OK")
    print("wecom splitter OK")
    print("system health OK")

if __name__ == "__main__":
    main()
