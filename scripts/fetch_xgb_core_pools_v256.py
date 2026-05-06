#!/usr/bin/env python3
"""
手动获取选股宝 core_pools 并缓存到 data/xgb/pools/<date>/。

用法：
python scripts/fetch_xgb_core_pools_v256.py
python scripts/fetch_xgb_core_pools_v256.py --date 2026-04-30
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.xgb_client import XGBClient
from core.xgb_cache import XGBCache
from core.xgb_core_pools_v255 import CORE_POOLS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat(), help="交易日 YYYY-MM-DD")
    parser.add_argument("--cache-root", default=str(ROOT / "data" / "xgb"))
    args = parser.parse_args()

    client = XGBClient()
    cache = XGBCache(root=args.cache_root)

    print(f"开始获取选股宝 core_pools: date={args.date}")
    print(f"cache_root={args.cache_root}")

    summary = {}
    for pool in CORE_POOLS:
        data, stale, err = cache.get_pool(client, pool, trade_date=args.date, allow_stale=True)
        summary[pool] = {
            "count": len(data or []),
            "stale": stale,
            "error": err,
        }
        print(f"- {pool}: count={summary[pool]['count']} stale={stale} error={err}")

    print("")
    print("完成。缓存目录：")
    print(Path(args.cache_root) / "pools" / args.date)
    print("")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
