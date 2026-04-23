# -*- coding: utf-8 -*-

from core.universe import get_all_stocks
from core.data import get_data

print("加载全市场...")

stocks = get_all_stocks()

results = []

print("开始扫描...")

for code in stocks:

    df = get_data(code)

    if df is None or len(df) < 100:
        continue

    last = df.iloc[-1]

    # 这里只做最基础测试（你后面再接策略）
    if float(last["close"]) > 0:

        results.append(code)

print("\n扫描完成：")
print("股票数量：", len(results))
print("样例：", results[:20])