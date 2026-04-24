# -*- coding: utf-8 -*-
"""
core/backtest.py

Polars 回测 V1：
1. 信号质量回测：看二买信号有没有边际优势
2. 简化组合回测：次日开盘买，最大持仓、仓位、滑点、手续费、止损/止盈/时间退出

注意：尾盘14:50确认需要分钟数据才能严谨回测。这里先用“收盘确认 + 次日开盘”作为保守回测标准。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import polars as pl

from core.data import get_data
from core.feature import compute_features
from core.strategy import evaluate_second_buy, trade_plan, SIGNAL_CONFIRM, SCAN_AFTER

FEE_RATE = 0.0003
SLIPPAGE = 0.002
MAX_HOLD_DAYS = 20
TAKE_PROFIT_1 = 0.10
MAX_GAP_UP_BUY = 0.03


def _simulate_trade(df_feat: pl.DataFrame, signal_idx: int, code: str, name: str = "") -> Optional[Dict[str, Any]]:
    """信号日收盘确认，次日开盘买入。"""
    if signal_idx + 1 >= len(df_feat):
        return None

    signal_day = df_feat.row(signal_idx, named=True)
    next_day = df_feat.row(signal_idx + 1, named=True)

    signal_close = float(signal_day["close"])
    open_next = float(next_day["open"])
    if open_next > signal_close * (1 + MAX_GAP_UP_BUY):
        return None

    plan = trade_plan(df_feat.slice(0, signal_idx + 1), market_state="strong")
    if not plan.get("valid"):
        return None

    buy_price = open_next * (1 + SLIPPAGE)
    stop = float(plan["stop"])
    target = buy_price * (1 + TAKE_PROFIT_1)

    exit_price = None
    exit_date = None
    exit_reason = None
    hold_days = 0

    end_idx = min(len(df_feat), signal_idx + 1 + MAX_HOLD_DAYS)
    future = df_feat.slice(signal_idx + 1, end_idx - signal_idx - 1)
    if len(future) == 0:
        return None

    mfe = 0.0
    mae = 0.0

    for j, row in enumerate(future.iter_rows(named=True), 1):
        hold_days = j
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        date = row["date"]

        mfe = max(mfe, high / buy_price - 1)
        mae = min(mae, low / buy_price - 1)

        # 先看止损，再看止盈，保守处理
        if low <= stop:
            exit_price = stop * (1 - SLIPPAGE)
            exit_date = date
            exit_reason = "止损"
            break
        if high >= target:
            exit_price = target * (1 - SLIPPAGE)
            exit_date = date
            exit_reason = "止盈10%"
            break

    if exit_price is None:
        last = future.tail(1).row(0, named=True)
        exit_price = float(last["close"]) * (1 - SLIPPAGE)
        exit_date = last["date"]
        exit_reason = "时间退出"

    gross = exit_price / buy_price - 1
    net = gross - FEE_RATE * 2

    return {
        "code": code,
        "name": name,
        "signal_date": signal_day["date"],
        "entry_date": next_day["date"],
        "exit_date": exit_date,
        "entry": round(buy_price, 3),
        "exit": round(exit_price, 3),
        "stop": round(stop, 3),
        "return": net,
        "mfe": mfe,
        "mae": mae,
        "hold_days": hold_days,
        "exit_reason": exit_reason,
    }


def backtest_signal_quality_single(code: str, name: str = "", start_date: str = "2020-01-01") -> Optional[pl.DataFrame]:
    df = get_data(code, bars=1500)
    if df is None or len(df) < 300:
        return None
    df = df.filter(pl.col("date") >= pl.lit(start_date).str.strptime(pl.Date, "%Y-%m-%d"))
    feat = compute_features(df)
    if feat.is_empty() or len(feat) < 220:
        return None

    trades = []
    last_exit_idx = -1
    for i in range(150, len(feat) - MAX_HOLD_DAYS - 1):
        # 同一股票持仓期间不重复开仓
        if i <= last_exit_idx:
            continue
        window = feat.slice(0, i + 1)
        res = evaluate_second_buy(window, scan_mode=SCAN_AFTER)
        if res.get("signal") != SIGNAL_CONFIRM:
            continue
        trade = _simulate_trade(feat, i, code, name)
        if trade is None:
            continue
        trade["score"] = res.get("score")
        trade["scores"] = str(res.get("scores"))
        trades.append(trade)
        last_exit_idx = i + MAX_HOLD_DAYS

    return pl.DataFrame(trades) if trades else None


def summarize_trades(trades: pl.DataFrame) -> Dict[str, Any]:
    if trades is None or trades.is_empty():
        return {"总交易数": 0}

    returns = trades.select("return").to_series().to_numpy()
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    equity = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(equity)
    drawdown = equity / peak - 1

    return {
        "总交易数": int(len(returns)),
        "胜率": f"{(returns > 0).mean():.2%}",
        "平均收益": f"{returns.mean():.2%}",
        "收益中位数": f"{np.median(returns):.2%}",
        "平均盈利": f"{wins.mean():.2%}" if len(wins) else "0.00%",
        "平均亏损": f"{losses.mean():.2%}" if len(losses) else "0.00%",
        "盈亏比": f"{abs(wins.mean() / losses.mean()):.2f}" if len(wins) and len(losses) and losses.mean() != 0 else "-",
        "最大单笔亏损": f"{returns.min():.2%}",
        "最大回撤_按交易序列": f"{drawdown.min():.2%}",
        "累计收益_按交易复利": f"{equity[-1] - 1:.2%}",
        "平均持仓天数": f"{trades.select('hold_days').to_series().mean():.1f}",
    }


def batch_signal_quality_backtest(codes: List[str], names: Optional[Dict[str, str]] = None, max_workers: int = 8, top_n: Optional[int] = None) -> Tuple[Dict[str, Any], Optional[pl.DataFrame]]:
    codes_run = codes[:top_n] if top_n else codes
    all_trades = []
    names = names or {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(backtest_signal_quality_single, code, names.get(code, "")): code for code in codes_run}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                res = fut.result()
                if res is not None and not res.is_empty():
                    all_trades.append(res)
            except Exception:
                pass
            if done % 100 == 0:
                print(f"回测进度：{done}/{len(codes_run)}")

    if not all_trades:
        return {"总交易数": 0}, None

    trades = pl.concat(all_trades, how="diagonal_relaxed").sort("entry_date")
    return summarize_trades(trades), trades


# 兼容旧名称
def backtest_single(code: str, start_date: str = "2020-01-01", end_date: Optional[str] = None):
    res = backtest_signal_quality_single(code, start_date=start_date)
    if res is None:
        return None
    if end_date:
        res = res.filter(pl.col("signal_date") <= pl.lit(end_date).str.strptime(pl.Date, "%Y-%m-%d"))
    return res


def batch_backtest(codes: List[str], top_n: int = 100):
    stats, trades = batch_signal_quality_backtest(codes, top_n=top_n)
    return stats, trades
