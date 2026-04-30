#!/usr/bin/env python3
"""
v2.5.3 真实 watchlist 候选预览。
只读，不写数据库，不写 paper_trader。
"""

from pathlib import Path
import sys
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v253 import preview_final_signals_from_watchlist_v253
from core.duckdb_daily_loader_v253 import inspect_duckdb


def main():
    trade_date = dt.date.today().isoformat()

    duck = inspect_duckdb(ROOT / "data" / "stock_data.duckdb")
    print("【DuckDB 检查】")
    print(json.dumps(duck, ensure_ascii=False, indent=2)[:4000])
    print("")

    report = preview_final_signals_from_watchlist_v253(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        trade_date=trade_date,
        limit=20,
        quote_map=None,
        core_pools=None,
    )

    print("【v2.5.3 Watchlist 候选预览】")
    print(f"读取行数: {report['rows']}")
    print("")
    print("映射后字段覆盖：")
    print(json.dumps(report["candidate_ready_report_before_score"], ensure_ascii=False, indent=2))
    print("")
    print("评分补齐后字段覆盖：")
    print(json.dumps(report["candidate_ready_report_after_score"], ensure_ascii=False, indent=2))
    print("")

    status_counter = Counter(x.get("signal_status") for x in report["results"])
    print("最终信号预览分布：")
    for k, v in status_counter.items():
        print(f"- {k}: {v}")

    print("")
    print("前 10 个候选预览：")
    for x in report["results"][:10]:
        print("----")
        print(f"{x.get('symbol')} {x.get('stock_name')}")
        print(f"status={x.get('signal_status')}, daily={x.get('daily_2buy_score')}, risk={x.get('risk_pct')}")
        print(f"price={x.get('current_price')}, trigger={x.get('trigger_price')}, fresh={x.get('fresh_quote')}")
        print(f"weekly={x.get('weekly_score')} sector={x.get('sector_score')} leader={x.get('leader_score')} yuanjun={x.get('yuanjun_score')}")
        if x.get("blocking_flags"):
            print("blocking:", x.get("blocking_flags")[:5])
        if x.get("downgrade_flags"):
            print("downgrade:", x.get("downgrade_flags")[:5])
        if x.get("risk_flags"):
            print("risk_flags:", x.get("risk_flags")[:5])

    print("")
    print("说明：如果大部分是 REJECTED 且原因包含 quote_not_fresh，说明还需要把实时行情 quote_map 接进来。")


if __name__ == "__main__":
    main()
