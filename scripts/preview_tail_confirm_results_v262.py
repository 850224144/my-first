#!/usr/bin/env python3
"""
预览 data/tail_confirm_results.parquet。
"""

from pathlib import Path
import sys
import argparse
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.safe_fields_v262 import clean_record_fields, stringify_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--path", default=str(ROOT / "data" / "tail_confirm_results.parquet"))
    args = parser.parse_args()

    import pandas as pd

    p = Path(args.path)
    print("【v2.6.2 Tail Confirm Results 预览】")
    print(f"path: {p}")

    if not p.exists():
        print("结果文件不存在，请先运行 scripts/run_tail_confirm_v262.py")
        return

    df = pd.read_parquet(p)
    if df.empty:
        print("rows: 0")
        return

    rows = [clean_record_fields(x) for x in df.where(df.notna(), None).to_dict("records")]
    print(f"rows: {len(rows)}")

    c = Counter(str(x.get("signal_status")) for x in rows)
    print("status:")
    for k, v in c.items():
        print(f"- {k}: {v}")

    print("")
    for x in rows[:50]:
        reasons = x.get("signal_reasons") or x.get("blocking_flags") or x.get("downgrade_flags") or x.get("risk_reasons")
        print(
            f"- {x.get('symbol') or x.get('code')} {x.get('stock_name') or x.get('name') or ''} | "
            f"{x.get('signal_status')} | quality={x.get('observe_quality')} "
            f"risk={x.get('risk_pct')} price={x.get('current_price')} trigger={x.get('trigger_price')} | "
            f"{stringify_list(reasons)}"
        )

if __name__ == "__main__":
    main()
