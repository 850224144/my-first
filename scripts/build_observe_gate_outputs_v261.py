#!/usr/bin/env python3
"""
v2.6.1 生成 Observe Gate 输出文件。
会写入：
- data/watchlist_quality.parquet
- data/watchlist_tail_focus.parquet
- data/watchlist_low_priority.parquet
- data/reports/observe_gate_summary_<date>.json
- data/reports/observe_gate_summary_<date>.md

不会覆盖 data/watchlist.parquet。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.observe_gate_store_v261 import build_observe_gate_outputs_v261

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--no-fetch-xgb", action="store_true")
    args = parser.parse_args()

    result = build_observe_gate_outputs_v261(
        trade_date=args.date,
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        output_quality_path=ROOT / "data" / "watchlist_quality.parquet",
        output_tail_focus_path=ROOT / "data" / "watchlist_tail_focus.parquet",
        output_low_priority_path=ROOT / "data" / "watchlist_low_priority.parquet",
        report_dir=ROOT / "data" / "reports",
        limit=args.limit,
        fetch_xgb_if_empty=not args.no_fetch_xgb,
    )

    print("【v2.6.1 Observe Gate 输出完成】")
    print(dumps({
        "trade_date": result["trade_date"],
        "input_rows": result["input_rows"],
        "quality_rows": result["quality_rows"],
        "tail_focus_rows": result["tail_focus_rows"],
        "low_priority_rows": result["low_priority_rows"],
        "output_quality_path": result["output_quality_path"],
        "output_tail_focus_path": result["output_tail_focus_path"],
        "output_low_priority_path": result["output_low_priority_path"],
        "summary_json_path": result["summary_json_path"],
        "summary_md_path": result["summary_md_path"],
        "quality_counter": result["summary"]["quality_counter"],
        "risk_bucket_counter": result["summary"]["risk_bucket_counter"],
    }))

if __name__ == "__main__":
    main()
