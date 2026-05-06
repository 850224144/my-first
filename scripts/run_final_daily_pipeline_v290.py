#!/usr/bin/env python3
"""
v2.9.0 最终日内/日报一键流程：
1. v2.7/v2.6.5 intraday tail pipeline
2. v2.8 BUY bridge
3. v2.9 daily report
默认不发送企业微信；加 --send-wecom 才发送。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.intraday_pipeline_v265 import run_intraday_tail_pipeline_v265
from core.buy_bridge_v280 import build_buy_bridge_v280
from core.daily_report_aggregator_v290 import build_daily_report_v290
from core.wecom_sender_v290 import send_wecom_markdown_v290

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    parser.add_argument("--persist-tail", action="store_true")
    parser.add_argument("--send-wecom", action="store_true")
    parser.add_argument("--dry-run-wecom", action="store_true")
    parser.add_argument("--webhook", default=None)
    args = parser.parse_args()

    tail = run_intraday_tail_pipeline_v265(
        trade_date=args.date,
        root=ROOT,
        persist_tail=args.persist_tail,
        fetch_xgb_if_empty=True,
    )

    buy = build_buy_bridge_v280(
        trade_date=args.date,
        tail_results_path=ROOT / "data" / "tail_confirm_results_v265.parquet",
        paper_candidates_path=ROOT / "data" / "paper_trade_candidates.parquet",
        open_recheck_path=ROOT / "data" / "trade_plan_open_recheck.parquet",
        report_dir=ROOT / "data" / "reports",
    )

    report = build_daily_report_v290(trade_date=args.date, root=ROOT)

    wecom = None
    if args.send_wecom or args.dry_run_wecom:
        content = Path(report["md_path"]).read_text(encoding="utf-8")
        wecom = send_wecom_markdown_v290(
            webhook_url=args.webhook,
            content=content,
            dry_run=args.dry_run_wecom,
        )

    print("【v2.9.0 最终流程完成】")
    print(json.dumps({
        "trade_date": args.date,
        "tail_buy_count": tail["tail_confirm"]["buy_count"],
        "buy_bridge": buy["summary"],
        "daily_report": report["md_path"],
        "wecom": wecom,
    }, ensure_ascii=False, indent=2, default=str))

if __name__ == "__main__":
    main()
