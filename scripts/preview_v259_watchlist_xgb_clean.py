#!/usr/bin/env python3
"""
v2.5.9 watchlist + XGB live pools 预览。
只读，不写库，不建仓。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v259 import preview_watchlist_with_xgb_clean_v259
from core.tail_candidate_diagnostics_v259 import format_tail_diagnosis_report_v259

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--no-fetch-xgb", action="store_true")
    args = parser.parse_args()

    report = preview_watchlist_with_xgb_clean_v259(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=args.date,
        limit=args.limit,
        fetch_xgb_if_empty=not args.no_fetch_xgb,
    )

    print("【v2.5.9 watchlist + XGB clean 预览】")
    print(f"trade_date: {report.get('trade_date')}")
    print(f"rows: {report.get('rows')}")
    print("")
    print("quote_report:")
    print(dumps(report.get("quote_report")))
    print("")
    print("xgb_live_pools_report:")
    print(dumps(report.get("xgb_live_pools_report")))
    print("")
    print("xgb_enrich_report:")
    print(dumps(report.get("xgb_enrich_report")))
    print("")

    c = Counter(x.get("signal_status") for x in report["results"])
    print("final status:")
    for k, v in c.items():
        print(f"- {k}: {v}")

    print("")
    print(format_tail_diagnosis_report_v259(report["results"]))

    print("")
    print("前 25 个候选：")
    for x in report["results"][:25]:
        print("----")
        print(f"{x.get('symbol')} {x.get('stock_name') or ''}")
        print(f"status={x.get('signal_status')} daily={x.get('daily_2buy_score')} risk={x.get('risk_pct')}")
        print(f"price={x.get('current_price')} trigger={x.get('trigger_price')} fresh={x.get('fresh_quote')}")
        print(f"theme={x.get('theme_name')} pools={x.get('xgb_pools')}")
        print(f"weekly={x.get('weekly_score')} sector={x.get('sector_score')} leader={x.get('leader_score')} yuanjun={x.get('yuanjun_score')}")
        if x.get("blocking_flags"):
            print("blocking:", x.get("blocking_flags")[:6])
        if x.get("downgrade_flags"):
            print("downgrade:", x.get("downgrade_flags")[:6])
        if x.get("risk_flags"):
            print("risk_flags:", x.get("risk_flags")[:8])

if __name__ == "__main__":
    main()
