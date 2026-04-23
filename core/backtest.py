import pandas as pd
import numpy as np
from core.feature import compute_features
from core.model import add_label, load_model, predict
from core.strategy import is_second_buy, volume_confirm, trade_plan


def backtest_single(code, start_date="2020-01-01", end_date=None):
    """单票回测"""
    from core.data import get_data
    df = get_data(code)
    if df is None or len(df) < 300:
        return None

    if end_date is None:
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
    if len(df) < 200:
        return None

    df_feat = compute_features(df)
    if df_feat.empty:
        return None

    model = load_model()
    trades = []

    # 滚动回测
    for i in range(200, len(df_feat)):
        df_slice = df_feat.iloc[:i + 1]

        # 预测
        prob = predict(model, df_slice) if model else 0.5
        if prob < 0.7:
            continue

        # 策略信号
        if not (is_second_buy(df_slice) and volume_confirm(df_slice)):
            continue

        # 生成交易计划
        plan = trade_plan(df.iloc[:i + 1], prob)
        buy_price = plan["buy"]
        stop_price = plan["stop"]

        # 后续20天表现
        future_window = df.iloc[i:i + 20]
        if len(future_window) < 20:
            continue

        # 模拟平仓
        entry_date = df.iloc[i]["date"]
        max_high = future_window["high"].max()
        min_low = future_window["low"].min()
        final_close = future_window.iloc[-1]["close"]

        if min_low <= stop_price:
            # 止损
            exit_price = stop_price
            profit = (exit_price - buy_price) / buy_price
            exit_reason = "止损"
        elif max_high >= buy_price * 1.15:
            # 止盈
            exit_price = buy_price * 1.15
            profit = 0.15
            exit_reason = "止盈"
        else:
            # 到期
            exit_price = final_close
            profit = (exit_price - buy_price) / buy_price
            exit_reason = "到期"

        trades.append({
            "date": entry_date,
            "code": code,
            "buy": buy_price,
            "stop": stop_price,
            "exit": exit_price,
            "profit": profit,
            "reason": exit_reason,
            "prob": prob
        })

    return pd.DataFrame(trades) if trades else None


def batch_backtest(codes, top_n=100):
    """批量回测"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tqdm import tqdm

    all_trades = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(backtest_single, code): code for code in codes[:top_n]}
        for future in tqdm(as_completed(futures), total=min(top_n, len(codes)), desc="回测进度"):
            res = future.result()
            if res is not None and not res.empty:
                all_trades.append(res)

    if not all_trades:
        return None, None

    all_trades = pd.concat(all_trades, ignore_index=True)

    # 回测统计
    total_trades = len(all_trades)
    win_rate = (all_trades["profit"] > 0).mean()
    avg_profit = all_trades["profit"].mean()
    max_drawdown = all_trades["profit"].min()
    sharpe = avg_profit / all_trades["profit"].std() * np.sqrt(250) if all_trades["profit"].std() > 0 else 0
    cumulative = (1 + all_trades["profit"]).prod() - 1

    stats = {
        "总交易数": total_trades,
        "胜率": f"{win_rate:.2%}",
        "平均收益": f"{avg_profit:.2%}",
        "最大亏损": f"{max_drawdown:.2%}",
        "夏普比率": f"{sharpe:.2f}",
        "累计收益": f"{cumulative:.2%}"
    }

    return stats, all_trades