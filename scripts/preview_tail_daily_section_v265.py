#!/usr/bin/env python3
"""
预览 v2.6.5 尾盘日报片段。
"""

from pathlib import Path
import sys
import argparse
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=dt.date.today().isoformat())
    args = parser.parse_args()

    p = ROOT / "data" / "reports" / f"tail_daily_section_v265_{args.date}.md"
    print("【v2.6.5 尾盘日报片段】")
    print(f"path: {p}")
    print("")
    if not p.exists():
        print("文件不存在，请先运行：")
        print(f"python scripts/run_tail_confirm_v265.py --date {args.date}")
        return
    print(p.read_text(encoding="utf-8"))

if __name__ == "__main__":
    main()
