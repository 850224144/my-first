#!/usr/bin/env python3
"""
v2.5.9 风险分布诊断。
目的：判断 observe 阶段是不是放进了太多止损距离过远的票。
只读，不写库。
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

def bucket_risk(r):
    try:
        v = float(r)
    except Exception:
        return "unknown"
    if v <= 5:
        return "<=5"
    if v <= 8:
        return "5-8"
    if v <= 12:
        return "8-12"
    if v <= 20:
        return "12-20"
    if v <= 30:
        return "20-30"
    return ">30"

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

    rows = report["results"]
    buckets = Counter(bucket_risk(x.get("risk_pct")) for x in rows)
    by_action = Counter(str(x.get("action")) for x in rows)
    by_risk_level = Counter(str(x.get("risk_level")) for x in rows)

    print("【v2.5.9 风险分布诊断】")
    print(f"候选数: {len(rows)}")
    print("")
    print("risk_pct 分布:")
    for k in ["<=5", "5-8", "8-12", "12-20", "20-30", ">30", "unknown"]:
        if buckets.get(k):
            print(f"- {k}: {buckets[k]}")

    print("")
    print("action 分布:")
    for k, v in by_action.most_common(20):
        print(f"- {k}: {v}")

    print("")
    print("risk_level 分布:")
    for k, v in by_risk_level.most_common(20):
        print(f"- {k}: {v}")

    print("")
    print("risk_pct<=8 且价格触发的候选:")
    hit = []
    for x in rows:
        try:
            risk = float(x.get("risk_pct"))
            price = float(x.get("current_price") or 0)
            trigger = float(x.get("trigger_price") or 0)
        except Exception:
            continue
        if risk <= 8 and trigger > 0 and price >= trigger:
            hit.append(x)
    if not hit:
        print("- 无")
    else:
        for x in hit:
            print(f"- {x.get('symbol')} {x.get('stock_name') or ''} risk={x.get('risk_pct')} price={x.get('current_price')} trigger={x.get('trigger_price')} status={x.get('signal_status')}")

    print("")
    print("判断：")
    print("如果 8% 以上占绝大多数，说明 observe 阶段太宽或结构止损取太远。")
    print("不要放松 tail_confirm 的 risk_pct<=8，应该优化 observe 入池质量和止损算法。")

if __name__ == "__main__":
    main()
