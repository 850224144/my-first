from core.data import update_data
from core.feature import compute_features
from core.strategy import is_second_buy, volume_confirm, trade_plan
from core.model import train, predict
import pandas as pd

STOCK_POOL = ["300750","600519","000858","002594","600036","000333"]

all_data = []

print("加载数据...")

for code in STOCK_POOL:
    df = update_data(code)
    if df is None or len(df) < 100:
        continue

    df = compute_features(df)
    df['code'] = code
    all_data.append(df)

# full = all_data[0]
# for d in all_data[1:]:
#     full = full.append(d)
full = pd.concat(all_data, ignore_index=True)
full = full.sort_values(["code", "date"])
print("训练模型...")
model = train(full)

print("\n=== 二买机会 ===")

for code in STOCK_POOL:
    df = update_data(code)
    if df is None or len(df) < 100:
        continue

    df = compute_features(df)

    if not is_second_buy(df):
        continue

    if not volume_confirm(df):
        continue

    prob = predict(model, df)
    plan = trade_plan(df, prob)

    print(f"\n{code}")
    print(f"概率: {plan['prob']}")
    print(f"买入价: {plan['buy']}")
    print(f"止损: {plan['stop']}")
    print(f"仓位: {plan['position']}")