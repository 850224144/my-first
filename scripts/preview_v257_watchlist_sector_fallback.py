#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v257 import preview_watchlist_with_sector_fallback_v257
from core.tail_candidate_diagnostics_v256 import format_tail_diagnosis_report

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--fetch-xgb", action="store_true")
    args = parser.parse_args()

    report = preview_watchlist_with_sector_fallback_v257(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        sector_hot_path=ROOT / "data" / "sector_hot.parquet",
        trade_date=args.date,
        limit=args.limit,
        allow_xgb_fetch=args.fetch_xgb,
    )

    print("【v2.5.7 watchlist + sector_hot fallback 预览】")
    print(f"trade_date: {report.get('trade_date')}")
    print(f"rows: {report.get('rows')}")
    print(f"sector_hot_fallback_matched: {report.get('sector_hot_fallback_matched')}")
    print("")
    print("quote_report:")
    print(dumps(report.get("quote_report")))
    print("")
    print("core_pools_report:")
    print(dumps(report.get("core_pools_report")))
    print("")
    print("final status:")
    c = Counter(x.get("signal_status") for x in report["results"])
    for k, v in c.items():
        print(f"- {k}: {v}")

    print("")
    print(format_tail_diagnosis_report(report["results"]))

    print("")
    print("前 20 个候选：")
    for x in report["results"][:20]:
        print("----")
        print(f"{x.get('symbol')} {x.get('stock_name') or ''}")
        print(f"status={x.get('signal_status')} daily={x.get('daily_2buy_score')} risk={x.get('risk_pct')}")
        print(f"price={x.get('current_price')} trigger={x.get('trigger_price')} fresh={x.get('fresh_quote')}")
        print(f"theme={x.get('theme_name')} weekly={x.get('weekly_score')} sector={x.get('sector_score')} leader={x.get('leader_score')} yuanjun={x.get('yuanjun_score')}")
        if x.get("blocking_flags"):
            print("blocking:", x.get("blocking_flags")[:6])
        if x.get("downgrade_flags"):
            print("downgrade:", x.get("downgrade_flags")[:6])
        if x.get("risk_flags"):
            print("risk_flags:", x.get("risk_flags")[:6])

if __name__ == "__main__":
    main()
