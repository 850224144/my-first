#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.buy_bridge_v280 import build_buy_bridge_v280

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--tail-results", default=str(ROOT / "data" / "tail_confirm_results_v265.parquet"))
    args = parser.parse_args()

    result = build_buy_bridge_v280(
        trade_date=args.date,
        tail_results_path=args.tail_results,
        paper_candidates_path=ROOT / "data" / "paper_trade_candidates.parquet",
        open_recheck_path=ROOT / "data" / "trade_plan_open_recheck.parquet",
        report_dir=ROOT / "data" / "reports",
    )

    print("【v2.8.0 BUY Bridge 完成】")
    print(dumps(result["summary"]))
    print("")
    print("输出：")
    print(result["paper_candidates_path"])
    print(result["open_recheck_path"])
    print(result["summary_md_path"])

if __name__ == "__main__":
    main()
