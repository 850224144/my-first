#!/usr/bin/env python3
"""
v2.5.8 获取选股宝 live pools。
关键：不传 date。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.xgb_live_pools_v258 import fetch_and_cache_xgb_live_pools_v258, live_pools_report

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--cache-root", default=str(ROOT / "data" / "xgb"))
    parser.add_argument("--param-key", default="pool_name", choices=["pool_name", "pool", "type"])
    args = parser.parse_args()

    pools = fetch_and_cache_xgb_live_pools_v258(
        trade_date=args.date,
        cache_root=args.cache_root,
        param_key=args.param_key,
    )
    print("【XGB live pools 获取结果】")
    print(dumps(live_pools_report(pools)))
    print("")
    print("缓存目录：")
    print(Path(args.cache_root) / "live_pools" / args.date)

if __name__ == "__main__":
    main()
