#!/usr/bin/env python3
"""
v2.6.5 一键运行：
1. observe gate
2. tail confirm
默认不写 trading_state.db。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.intraday_pipeline_v265 import run_intraday_tail_pipeline_v265

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--persist-tail", action="store_true")
    parser.add_argument("--no-fetch-xgb", action="store_true")
    args = parser.parse_args()

    result = run_intraday_tail_pipeline_v265(
        trade_date=args.date,
        root=ROOT,
        persist_tail=args.persist_tail,
        fetch_xgb_if_empty=not args.no_fetch_xgb,
    )

    print("【v2.6.5 Intraday Tail Pipeline 完成】")
    print(dumps({
        "trade_date": result["trade_date"],
        "observe_input_rows": result["observe"]["input_rows"],
        "tail_focus_rows": result["observe"]["tail_focus_rows"],
        "low_priority_rows": result["observe"]["low_priority_rows"],
        "tail_result_rows": result["tail_confirm"]["result_rows"],
        "buy_count": result["tail_confirm"]["buy_count"],
        "watch_count": result["tail_confirm"]["watch_count"],
        "rejected_count": result["tail_confirm"]["rejected_count"],
        "consistency": result["consistency"],
        "observe_summary": result["observe"]["summary_md_path"],
        "tail_summary": result["tail_confirm"]["summary_md_path"],
        "daily_section": result["tail_confirm"]["daily_section_path"],
    }))

if __name__ == "__main__":
    main()
