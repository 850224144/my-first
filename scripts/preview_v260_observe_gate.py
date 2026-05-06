#!/usr/bin/env python3
"""
v2.6.0 observe gate 预览。
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
from core.observe_gate_v260 import apply_observe_gate_v260, summarize_observe_gate_v260
from core.duckdb_daily_loader_v253 import load_daily_bars_from_duckdb

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--load-bars", action="store_true", help="加载日K做撤军线诊断，稍慢")
    args = parser.parse_args()

    report = preview_watchlist_with_xgb_clean_v259(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=args.date,
        limit=args.limit,
        fetch_xgb_if_empty=False,
    )

    bars_map = {}
    if args.load_bars:
        for item in report["results"]:
            sym = item.get("symbol") or item.get("code")
            if not sym:
                continue
            bars_map[sym] = load_daily_bars_from_duckdb(sym, db_path=ROOT / "data" / "stock_data.duckdb", limit=80)

    gated = apply_observe_gate_v260(report["results"], daily_bars_map=bars_map)
    summary = summarize_observe_gate_v260(gated)

    print("【v2.6.0 Observe Gate 预览】")
    print(f"候选数: {len(gated)}")
    print(f"tail_focus_count: {summary['tail_focus_count']}")
    print(f"deprioritize_count: {summary['deprioritize_count']}")
    print(f"possible_stop_compression_count: {summary['possible_stop_compression_count']}")
    print("")
    print("observe_quality:")
    print(dumps(summary["observe_quality"]["quality_counter"]))
    print("")
    print("risk_bucket:")
    print(dumps(summary["observe_quality"]["risk_bucket_counter"]))

    print("")
    print("进入尾盘重点确认 Top30:")
    if not summary["tail_focus"]:
        print("- 无")
    for x in summary["tail_focus"]:
        print(
            f"- {x.get('symbol')} {x.get('stock_name') or ''} "
            f"quality={x.get('observe_quality')} p={x.get('observe_priority')} "
            f"risk={x.get('risk_pct')} daily={x.get('daily_2buy_score')} "
            f"price={x.get('current_price')} trigger={x.get('trigger_price')} "
            f"status={x.get('signal_status')}"
        )

    print("")
    print("可能是撤军线过远导致的误杀 Top30:")
    if not summary["possible_stop_compression"]:
        print("- 无")
    for x in summary["possible_stop_compression"]:
        d = x.get("stop_loss_diagnosis") or {}
        props = d.get("compressible_proposals") or []
        best = props[0] if props else {}
        print(
            f"- {x.get('symbol')} {x.get('stock_name') or ''} "
            f"risk={x.get('risk_pct')} current_stop={d.get('current_stop_loss')} "
            f"proposal={best.get('source')} stop={best.get('stop_loss')} risk={best.get('risk_pct')}"
        )

    print("")
    print("说明：")
    print("1. 这个脚本只做观察池分层，不写库、不建仓。")
    print("2. 如果 tail_focus 很少，说明当前确实没有多少可交易候选。")
    print("3. 如果 possible_stop_compression 很多，下一版再优化结构止损算法。")

if __name__ == "__main__":
    main()
