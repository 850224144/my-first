#!/usr/bin/env python3
"""
预览 data/watchlist_tail_focus.parquet。
"""

from pathlib import Path
import sys
import argparse
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.tail_focus_loader_v261 import load_tail_focus_v261

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="仅展示用，占位参数；文件为当前 tail_focus 文件")
    parser.add_argument("--path", default=str(ROOT / "data" / "watchlist_tail_focus.parquet"))
    args = parser.parse_args()

    rows = load_tail_focus_v261(args.path)

    print("【v2.6.1 Tail Focus 预览】")
    print(f"path: {args.path}")
    print(f"rows: {len(rows)}")
    print("")

    if not rows:
        print("无 tail focus 候选。")
        return

    for r in rows[:50]:
        print(
            f"- {r.get('symbol') or r.get('code')} {r.get('stock_name') or r.get('name') or ''} | "
            f"quality={r.get('observe_quality')} p={r.get('observe_priority')} "
            f"risk={r.get('risk_pct')} daily={r.get('daily_2buy_score')} "
            f"price={r.get('current_price')} trigger={r.get('trigger_price')} "
            f"status={r.get('signal_status')} "
            f"reasons={r.get('risk_reasons')}"
        )

if __name__ == "__main__":
    main()
