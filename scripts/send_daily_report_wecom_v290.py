#!/usr/bin/env python3
from pathlib import Path
import sys
import argparse
import datetime as dt
import json
import os

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.daily_report_aggregator_v290 import build_daily_report_v290
from core.wecom_sender_v290 import send_wecom_markdown_v290

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--webhook", default=None)
    args = parser.parse_args()

    result = build_daily_report_v290(trade_date=args.date, root=ROOT)
    content = Path(result["md_path"]).read_text(encoding="utf-8")

    send_result = send_wecom_markdown_v290(
        webhook_url=args.webhook,
        content=content,
        dry_run=args.dry_run,
    )

    print("【v2.9.0 企业微信发送结果】")
    print(json.dumps(send_result, ensure_ascii=False, indent=2, default=str))

if __name__ == "__main__":
    main()
