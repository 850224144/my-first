#!/usr/bin/env python3
"""
v2.5.5 watchlist 全链路预览。
只读，不写库，不建仓。
"""

from pathlib import Path
import sys
import datetime as dt
import json
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.realtime_quote_loader_v255 import inspect_realtime_quote_table
from core.watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255


def json_safe_default(obj):
    if isinstance(obj, (dt.date, dt.datetime, dt.time)):
        return obj.isoformat()
    return str(obj)


def dumps(obj, limit=None):
    text = json.dumps(obj, ensure_ascii=False, indent=2, default=json_safe_default)
    return text[:limit] if limit else text


def main():
    trade_date = dt.date.today().isoformat()

    print("【realtime_quote 检查】")
    qinfo = inspect_realtime_quote_table(ROOT / "data" / "stock_data.duckdb")
    print(dumps(qinfo, limit=5000))
    print("")

    report = preview_final_signals_from_watchlist_v255(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=trade_date,
        limit=50,
        allow_xgb_fetch=False,
    )

    print("【v2.5.5 Watchlist 全链路预览】")
    print(f"trade_date: {report['trade_date']}")
    print(f"读取行数: {report['rows']}")
    print("")
    print("实时行情覆盖：")
    print(dumps(report["quote_report"]))
    print("")
    print("选股宝 core_pools：")
    print(dumps(report["core_pools_report"]))
    print("")
    print("映射后字段覆盖：")
    print(dumps(report["candidate_ready_report_before_score"]))
    print("")
    print("评分补齐后字段覆盖：")
    print(dumps(report["candidate_ready_report_after_score"]))
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
    for k, v in flag_counter.most_common(20):
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
            print("blocking:", x.get("blocking_flags")[:6])
        if x.get("downgrade_flags"):
            print("downgrade:", x.get("downgrade_flags")[:6])
        if x.get("risk_flags"):
            print("risk_flags:", x.get("risk_flags")[:6])

    print("")
    print("判断建议：")
    print("1. fresh_ratio=0：先处理 realtime_quote 是否为今天数据。")
    print("2. fresh_ratio>0 但仍 REJECTED：看 risk_pct_too_high / too_hot_today / price_not_triggered。")
    print("3. sector/yuanjun 长期 50：说明选股宝 core_pools 缓存还没接入。")
    print("4. 本脚本只读，不写库，不建仓。")


if __name__ == "__main__":
    main()
