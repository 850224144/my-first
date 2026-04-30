#!/usr/bin/env python3
"""
v2.6.2 从 data/watchlist_tail_focus.parquet 执行尾盘确认。
默认 persist=False，只输出结果，不写 trading_state.db。
需要写库时加 --persist。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.tail_confirm_runner_v262 import run_tail_confirm_from_tail_focus_v262

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--persist", action="store_true", help="写入 data/trading_state.db")
    parser.add_argument("--tail-focus-path", default=str(ROOT / "data" / "watchlist_tail_focus.parquet"))
    args = parser.parse_args()

    result = run_tail_confirm_from_tail_focus_v262(
        trade_date=args.date,
        tail_focus_path=args.tail_focus_path,
        output_results_path=ROOT / "data" / "tail_confirm_results.parquet",
        report_dir=ROOT / "data" / "reports",
        db_path=str(ROOT / "data" / "trading_state.db"),
        persist=args.persist,
    )

    print("【v2.6.2 Tail Confirm 完成】")
    print(dumps({
        "trade_date": result["trade_date"],
        "input_rows": result["input_rows"],
        "result_rows": result["result_rows"],
        "buy_count": result["buy_count"],
        "near_count": result["near_count"],
        "watch_count": result["watch_count"],
        "output_results_path": result["output_results_path"],
        "summary_json_path": result["summary_json_path"],
        "summary_md_path": result["summary_md_path"],
        "status_counter": result["summary"]["status_counter"],
        "quality_counter": result["summary"]["quality_counter"],
    }))

    print("")
    print("明细：")
    for x in result["results"]:
        print(
            f"- {x.get('symbol') or x.get('code')} {x.get('stock_name') or x.get('name') or ''} | "
            f"{x.get('signal_status')} | quality={x.get('observe_quality')} "
            f"risk={x.get('risk_pct')} price={x.get('current_price')} trigger={x.get('trigger_price')} "
            f"write_paper={x.get('should_write_paper_trade')}"
        )

if __name__ == "__main__":
    main()
