# run_positions.py
from __future__ import annotations

import argparse
import os

from core.position_tracker import (
    add_position,
    load_positions,
    track_positions,
    close_position,
    generate_position_report,
)


def parse_args():
    parser = argparse.ArgumentParser(description="A股持仓跟踪模块")

    parser.add_argument("--list", action="store_true", help="查看当前持仓")
    parser.add_argument("--track", action="store_true", help="跟踪当前持仓并生成/推送报告")
    parser.add_argument("--no-push", action="store_true", help="只生成报告，不推送企业微信")

    parser.add_argument("--add-position", type=str, help="新增持仓股票代码")
    parser.add_argument("--buy-price", type=float, help="买入价")
    parser.add_argument("--shares", type=int, help="持仓股数")
    parser.add_argument("--buy-date", type=str, default="", help="买入日期 YYYY-MM-DD")
    parser.add_argument("--stop-loss", type=float, default=None, help="止损价")
    parser.add_argument("--target1", type=float, default=None, help="目标1")
    parser.add_argument("--target2", type=float, default=None, help="目标2")
    parser.add_argument("--name", type=str, default="", help="股票名称")
    parser.add_argument("--note", type=str, default="", help="备注")
    parser.add_argument("--force", action="store_true", help="如果已有 open 持仓则覆盖")

    parser.add_argument("--close-position", type=str, help="关闭持仓股票代码")
    parser.add_argument("--exit-price", type=float, help="卖出价")
    parser.add_argument("--exit-date", type=str, default="", help="卖出日期 YYYY-MM-DD")
    parser.add_argument("--exit-reason", type=str, default="manual_close", help="退出原因")

    return parser.parse_args()


def _push_position_report(content: str, no_push: bool = False):
    if no_push:
        return
    try:
        from core.notify import notify_position_report
        notify_position_report(content, webhook=os.getenv("WECHAT_WEBHOOK", ""))
    except Exception as e:
        print(f"⚠️ 持仓报告企业微信推送失败：{e}")


def _push_position_event(title: str, message: str):
    try:
        from core.notify import notify_system_event
        notify_system_event(title=title, message=message, level="INFO", job_name="run_positions")
    except Exception as e:
        print(f"⚠️ 持仓事件企业微信推送失败：{e}")


def main():
    args = parse_args()

    if args.list:
        df = load_positions(open_only=False)
        if df.is_empty():
            print("当前没有持仓记录。")
        else:
            print(df)
        return

    if args.track:
        content = generate_position_report()
        print(content)
        _push_position_report(content, no_push=args.no_push)
        return

    if args.add_position:
        if args.buy_price is None or args.shares is None:
            raise ValueError("--add-position 需要同时传 --buy-price 和 --shares")
        row = add_position(
            code=args.add_position,
            buy_price=args.buy_price,
            shares=args.shares,
            buy_date=args.buy_date or None,
            stop_loss=args.stop_loss,
            take_profit_1=args.target1,
            take_profit_2=args.target2,
            name=args.name,
            note=args.note,
            force=args.force,
        )
        print("✅ 已新增持仓：")
        print(row)
        _push_position_event(
            "新增持仓记录",
            f"{row.get('code')} {row.get('name','')} 已加入持仓跟踪。\n\n买入价：{row.get('buy_price')}\n股数：{row.get('shares')}\n止损：{row.get('stop_loss')}\n目标1：{row.get('take_profit_1')}\n目标2：{row.get('take_profit_2')}",
        )
        return

    if args.close_position:
        if args.exit_price is None:
            raise ValueError("--close-position 需要传 --exit-price")
        row = close_position(
            code=args.close_position,
            exit_price=args.exit_price,
            exit_reason=args.exit_reason,
            exit_date=args.exit_date or None,
        )
        print("✅ 已关闭持仓并写入交易日志：")
        print(row)
        _push_position_event(
            "持仓已关闭",
            f"{row.get('code')} {row.get('name','')} 已关闭持仓。\n\n卖出价：{row.get('exit_price')}\n退出原因：{row.get('exit_reason')}\n收益率：{row.get('pnl_pct')}%\n盈亏金额：{row.get('pnl_amount')}",
        )
        return

    print("请指定操作，例如：")
    print("python run_positions.py --list")
    print("python run_positions.py --track")
    print("python run_positions.py --add-position 002594 --buy-price 103.5 --shares 100")
    print("python run_positions.py --close-position 002594 --exit-price 110 --exit-reason take_profit")


if __name__ == "__main__":
    main()
