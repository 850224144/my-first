#!/usr/bin/env python3
"""
v2.5.4 真实 watchlist + DuckDB realtime_quote 预览。
只读，不写库，不建仓。
"""

from pathlib import Path
import sys
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.realtime_quote_loader_v254 import inspect_realtime_quote_table
from core.watchlist_pipeline_v254 import preview_final_signals_from_watchlist_v254


def main():
    trade_date = dt.date.today().isoformat()

    print("【realtime_quote 检查】")
    qinfo = inspect_realtime_quote_table(ROOT / "data" / "stock_data.duckdb")
    print(json.dumps(qinfo, ensure_ascii=False, indent=2)[:5000])
    print("")

    report = preview_final_signals_from_watchlist_v254(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        trade_date=trade_date,
        limit=50,
        quote_map=None,
        core_pools=None,
    )

    print("【v2.5.4 Watchlist + 实时行情预览】")
    print(f"trade_date: {report['trade_date']}")
    print(f"读取行数: {report['rows']}")
    print("")
    print("实时行情覆盖：")
    print(json.dumps(report["quote_report"], ensure_ascii=False, indent=2))
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

    flag_counter = Counter()
    for x in report["results"]:
        for k in ["blocking_flags", "downgrade_flags", "risk_flags"]:
            v = x.get(k)
            if isinstance(v, list):
                for item in v:
                    flag_counter[str(item)] += 1

    print("")
    print("Top 拒绝/降级原因：")
    for k, v in flag_counter.most_common(15):
        print(f"- {k}: {v}")

    print("")
    print("前 15 个候选预览：")
    for x in report["results"][:15]:
        print("----")
        print(f"{x.get('symbol')} {x.get('stock_name') or ''}")
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
    print("说明：")
    print("1. 如果 quote_report fresh_ratio=0，说明 realtime_quote 不是今天的数据。")
    print("2. 如果 quote_not_fresh 消失，实时行情接入成功。")
    print("3. 如果仍然 REJECTED，多数是 risk_pct_too_high / too_hot_today / sector_data_missing，这属于策略判断。")


if __name__ == "__main__":
    main()
