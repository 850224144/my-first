#!/usr/bin/env python3
"""
v2.6.0 observe 风险质量诊断。
只读，不写库，不建仓。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v259 import preview_watchlist_with_xgb_clean_v259
from core.risk_quality_v260 import format_observe_quality_report, summarize_observe_quality

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    report = preview_watchlist_with_xgb_clean_v259(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=args.date,
        limit=args.limit,
        fetch_xgb_if_empty=False,
    )

    print(format_observe_quality_report(report["results"]))

    s = summarize_observe_quality(report["results"])
    print("")
    print("机器可读摘要：")
    print(dumps({
        "quality_counter": s["quality_counter"],
        "risk_bucket_counter": s["risk_bucket_counter"],
        "fresh_count": s["fresh_count"],
        "price_triggered_count": s["price_triggered_count"],
    }))

    print("")
    print("判断：")
    print("tail_ready/observe_keep 才适合进入尾盘重点确认。")
    print("noise_high_risk 不是删除数据，而是降低刷新和提醒优先级。")

if __name__ == "__main__":
    main()
