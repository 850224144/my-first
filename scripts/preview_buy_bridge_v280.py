#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.parquet_safe_writer_v263 import read_parquet_safe_v263

def show(path):
    rows = read_parquet_safe_v263(path)
    print(f"path: {path}")
    print(f"rows: {len(rows)}")
    for r in rows[-20:]:
        print(f"- {r.get('symbol')} {r.get('stock_name') or ''} status={r.get('signal_status') or r.get('status')} price={r.get('buy_price') or r.get('planned_buy_price')} risk={r.get('risk_pct')}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    args = parser.parse_args()

    print("【纸面交易候选】")
    show(ROOT / "data" / "paper_trade_candidates.parquet")
    print("")
    print("【开盘复核计划】")
    show(ROOT / "data" / "trade_plan_open_recheck.parquet")

if __name__ == "__main__":
    main()
