#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.scheduler_tail_jobs_v270 import (
    build_observe_gate_job_v270,
    tail_confirm_job_v270,
    intraday_tail_pipeline_job_v270,
)

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--job", choices=["observe", "tail", "pipeline"], required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--persist-tail", action="store_true")
    parser.add_argument("--no-fetch-xgb", action="store_true")
    args = parser.parse_args()

    if args.job == "observe":
        result = build_observe_gate_job_v270(
            root=ROOT,
            trade_date=args.date,
            force=args.force,
            fetch_xgb_if_empty=not args.no_fetch_xgb,
        )
    elif args.job == "tail":
        result = tail_confirm_job_v270(
            root=ROOT,
            trade_date=args.date,
            force=args.force,
            persist_tail=args.persist_tail,
        )
    else:
        result = intraday_tail_pipeline_job_v270(
            root=ROOT,
            trade_date=args.date,
            force=args.force,
            persist_tail=args.persist_tail,
            fetch_xgb_if_empty=not args.no_fetch_xgb,
        )

    print("【v2.7.0 job 执行完成】")
    print(dumps(result))

if __name__ == "__main__":
    main()
