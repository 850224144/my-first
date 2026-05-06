#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.daily_report_aggregator_v290 import build_daily_report_v290

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    args = parser.parse_args()

    result = build_daily_report_v290(trade_date=args.date, root=ROOT)

    print("【v2.9.0 日报生成完成】")
    print(json.dumps({
        "trade_date": result["trade_date"],
        "md_path": result["md_path"],
        "json_path": result["json_path"],
        "health_path": result["health_path"],
    }, ensure_ascii=False, indent=2))

    print("")
    print(Path(result["md_path"]).read_text(encoding="utf-8"))

if __name__ == "__main__":
    main()
