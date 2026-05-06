#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.xgb_probe_v257 import probe_xgb_pool_variants, summarize_probe

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--pool", action="append", help="只探测指定 pool，可多次传")
    args = parser.parse_args()

    results = probe_xgb_pool_variants(trade_date=args.date, pools=args.pool)
    summary = summarize_probe(results)

    print("【XGB pool 接口探测汇总】")
    print(dumps(summary))
    print("")
    print("【全部探测结果摘要】")
    for r in results:
        print(
            f"pool={r.get('pool_name')} path={r.get('path')} param={r.get('param_key')} "
            f"date={r.get('date_value')} status={r.get('status_code')} code={r.get('code')} "
            f"count={r.get('count')} msg={r.get('msg')} err={r.get('error')}"
        )
        if int(r.get("count") or 0) > 0:
            print("  url:", r.get("url"))
            print("  shape:", dumps(r.get("shape")))

if __name__ == "__main__":
    main()
