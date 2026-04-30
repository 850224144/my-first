# run_paper.py
from __future__ import annotations

import argparse
from core.paper_trader import (
    load_paper_positions,
    load_paper_journal,
    track_paper_positions,
    generate_paper_report,
    paper_summary_stats,
    delete_paper_position,
)


def parse_args():
    p = argparse.ArgumentParser(description="纸面交易账本/T+1模拟交易")
    p.add_argument("--list", action="store_true", help="查看纸面持仓")
    p.add_argument("--journal", action="store_true", help="查看纸面交易日志")
    p.add_argument("--track", action="store_true", help="跟踪纸面持仓并生成报告")
    p.add_argument("--stats", action="store_true", help="查看纸面交易统计")
    p.add_argument("--delete-position", type=str, help="删除 open 纸面持仓，不写 journal")
    return p.parse_args()


def main():
    args = parse_args()
    if args.list:
        df = load_paper_positions(open_only=False)
        print("当前没有纸面持仓。" if df.is_empty() else df)
        return
    if args.journal:
        df = load_paper_journal()
        print("当前没有纸面交易日志。" if df.is_empty() else df)
        return
    if args.track:
        print(generate_paper_report())
        return
    if args.stats:
        print(paper_summary_stats())
        return
    if args.delete_position:
        n = delete_paper_position(args.delete_position)
        print(f"已删除 open 纸面持仓：{n} 条")
        return
    print("请指定操作：--list / --track / --journal / --stats / --delete-position 代码")


if __name__ == "__main__":
    main()
