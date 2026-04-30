#!/usr/bin/env python3
"""
v2.5.6 watchlist + realtime_quote + XGB core_pools 预览。
只读，不写库，不建仓。

用法：
python scripts/preview_v256_watchlist_with_xgb.py
python scripts/preview_v256_watchlist_with_xgb.py --date 2026-04-30
python scripts/preview_v256_watchlist_with_xgb.py --date 2026-04-30 --fetch-xgb
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255
from core.tail_candidate_diagnostics_v256 import format_tail_diagnosis_report
from core.realtime_quote_loader_v255 import inspect_realtime_quote_table
from core.xgb_core_pools_v255 import load_xgb_core_pools_v255, core_pools_report


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--fetch-xgb", action="store_true", help="允许联网获取 XGB core_pools")
    args = parser.parse_args()

    print("【realtime_quote 检查】")
    print(dumps(inspect_realtime_quote_table(ROOT / "data" / "stock_data.duckdb")))
    print("")

    core_pools = load_xgb_core_pools_v255(
        trade_date=args.date,
        cache_root=ROOT / "data" / "xgb",
        allow_fetch=args.fetch_xgb,
    )
    print("【XGB core_pools】")
    print(dumps(core_pools_report(core_pools)))
    print("")

    report = preview_final_signals_from_watchlist_v255(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=args.date,
        limit=args.limit,
        core_pools=core_pools,
        allow_xgb_fetch=False,
    )

    print("【实时行情覆盖】")
    print(dumps(report["quote_report"]))
    print("")

    print("【最终信号分布】")
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
        print(f"weekly={x.get('weekly_score')} sector={x.get('sector_score')} leader={x.get('leader_score')} yuanjun={x.get('yuanjun_score')}")
        if x.get("blocking_flags"):
            print("blocking:", x.get("blocking_flags")[:6])
        if x.get("downgrade_flags"):
            print("downgrade:", x.get("downgrade_flags")[:6])
        if x.get("risk_flags"):
            print("risk_flags:", x.get("risk_flags")[:6])


if __name__ == "__main__":
    main()
