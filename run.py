# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from core.universe import get_all_stocks
from core.data import get_data

print("加载全市场...")
stocks = get_all_stocks()
total = len(stocks)
print(f"市场股票总数: {total}")

results = []
errors = []

# 调试用统计
skipped_nodata = []      # get_data 返回 None
skipped_short = []       # 行数 < 100
skipped_close_zero = []  # 收盘价 <= 0

MAX_WORKERS = 20   # 可根据网络情况调整

def process_one(code):
    try:
        df = get_data(code)
        if df is None:
            skipped_nodata.append(code)
            return None
        if len(df) < 100:
            skipped_short.append(code)
            return None
        last = df.iloc[-1]
        if float(last["close"]) <= 0:
            skipped_close_zero.append(code)
            return None
        return code
    except Exception as e:
        errors.append((code, str(e)))
        return None

print("开始扫描...")
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_one, code): code for code in stocks}
    for future in tqdm(as_completed(futures), total=total, desc="扫描进度"):
        res = future.result()
        if res:
            results.append(res)

print("\n扫描完成：")
print("有效股票数量：", len(results))
print("过滤统计：")
print(f"  - 无数据(返回 None): {len(skipped_nodata)}")
print(f"  - 数据不足100行: {len(skipped_short)}")
print(f"  - 收盘价<=0: {len(skipped_close_zero)}")
if errors:
    print(f"异常出错股票：{len(errors)}")
print("样例：", results[:20])