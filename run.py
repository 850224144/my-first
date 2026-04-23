# -*- coding: utf-8 -*-
"""
最终完整整合版
所有优化全包含：
1. 修复DuckDB致命错误
2. 所有过滤条件可配置（带开关+阈值）
3. 超详细逐条件调试打印
4. 自动生成sh000001.parquet
5. 数据双向同步（数据库+parquet）
"""

import os
import polars as pl
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime

from core.universe import get_all_stocks
from core.data import get_data, clean_old_cache, init_db
from core.feature import compute_features, FEATURES
from core.model import load_model, predict, train_general_model
from core.strategy import is_second_buy, volume_confirm, trade_plan
from core.market import market_filter
from core.alert import push_results

# ==============================================================================
# 📝 【核心】可配置过滤条件区，True是该条件不参与过滤False
# ==============================================================================
FILTER_CONFIG = {
    "基础数据_数据长度": {"ENABLED": True, "THRESHOLD": 50, "DESC": "历史K线数量不足"},
    "基础数据_收盘价大于0": {"ENABLED": True, "THRESHOLD": 0, "DESC": "最新收盘价<=0"},
    "基础数据_不过滤停牌": {"ENABLED": True, "THRESHOLD": 0, "DESC": "近3天成交量=0（停牌）"},
    "基础数据_不过滤一字涨跌停": {"ENABLED": True, "THRESHOLD": 0, "DESC": "一字涨跌停"},
    "大盘环境_不过滤": {"ENABLED": True, "THRESHOLD": 0, "DESC": "大盘未站上20日线"},
    "特征计算_不过滤": {"ENABLED": True, "THRESHOLD": 0, "DESC": "特征计算失败"},
    "模型概率_不过滤": {"ENABLED": True, "THRESHOLD": 0.5, "DESC": "模型概率不足"},
    "策略买点_不过滤": {"ENABLED": True, "THRESHOLD": 0, "DESC": "未满足策略买点条件"},
    "量能确认_不过滤": {"ENABLED": True, "THRESHOLD": 0, "DESC": "量能不足"}
}

# ==============================================================================
# 📝 其他配置
# ==============================================================================
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b"
PUSH_PLATFORM = "wechat"
#把 MODE 改成 "train是训练模式，scan是选股
MODE = "scan"
DEBUG_PRINT_LIMIT = 100

import os

MAX_WORKERS = min(os.cpu_count() * 3, 32)

# ==============================================================================
# 🚀 程序主逻辑
# ==============================================================================

print("=" * 80)
print("📊 A股量化选股系统（最终完整整合版）")
print("=" * 80)

print("\n📋 当前过滤条件配置：")
print("-" * 80)
for name, config in FILTER_CONFIG.items():
    status = "✅ 启用" if config["ENABLED"] else "❌ 关闭"
    print(f"  {name:30s} | {status} | {config['DESC']}")
print("-" * 80)
print("💡 选不出股票时，把对应条件的 ENABLED 改成 False 即可")
print("=" * 80)

detailed_filter_stats = {name: 0 for name in FILTER_CONFIG.keys()}
detailed_filter_stats["最终入选"] = 0

if MODE == "train":
    print("\n【模式】训练通用模型")
    print("-" * 80)

    init_db()
    clean_old_cache()

    stocks = get_all_stocks()
    print(f"✅ 加载股票池：{len(stocks)} 只")

    all_data = []
    print("\n📥 开始加载数据（前500只用于训练）...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_data, code): code for code in stocks[:500]}
        for future in tqdm(as_completed(futures), total=500, desc="加载进度"):
            code = futures[future]
            try:
                df = future.result()
                if df is not None and len(df) >= 200:
                    df_feat = compute_features(df)
                    if not df_feat.is_empty():
                        all_data.append((code, df_feat))
            except:
                pass

    print(f"\n✅ 有效数据样本：{len(all_data)} 只")

    model = train_general_model(all_data)

    if model:
        print("\n🎉 模型训练完成！")
        print("💡 现在可以修改 MODE = 'scan' 进行选股了")
    else:
        print("\n❌ 模型训练失败，请检查数据")

else:
    print("\n【模式】每日选股（最终完整整合版）")
    print("-" * 80)

    init_db()
    clean_old_cache()

    print("📈 校验大盘环境...")
    market_pass = market_filter()

    if FILTER_CONFIG["大盘环境_不过滤"]["ENABLED"]:
        market_pass = True
        print("⚠️  大盘环境过滤已关闭，直接放行")

    if not market_pass:
        print("\n❌ 大盘未站上20日线，关闭选股")
        print("=" * 80)
        print("✅ 程序执行完成")
        print("=" * 80)
        exit()

    stocks = get_all_stocks()
    print(f"✅ 市场股票总数: {len(stocks)}")

    model = load_model()
    if model is None:
        print("⚠️  未找到通用模型，将使用默认概率 0.6")
    else:
        print("✅ 通用模型加载成功")

    print("\n📥 第一阶段：预加载全市场数据并逐条件过滤...")
    stock_info_list = []
    debug_count = 0


    def load_and_filter(code):
        global debug_count
        debug_print = (debug_count < DEBUG_PRINT_LIMIT)
        filter_reason = ""
        passed = True

        try:
            df = get_data(code)

            if FILTER_CONFIG["基础数据_数据长度"]["ENABLED"]:
                if df is None or len(df) < FILTER_CONFIG["基础数据_数据长度"]["THRESHOLD"]:
                    filter_reason = FILTER_CONFIG["基础数据_数据长度"]["DESC"]
                    detailed_filter_stats["基础数据_数据长度"] += 1
                    passed = False

            if passed:
                last = df.slice(-1)
                close_last = last.select(pl.col("close")).item()

                if FILTER_CONFIG["基础数据_收盘价大于0"]["ENABLED"]:
                    if close_last <= FILTER_CONFIG["基础数据_收盘价大于0"]["THRESHOLD"]:
                        filter_reason = FILTER_CONFIG["基础数据_收盘价大于0"]["DESC"]
                        detailed_filter_stats["基础数据_收盘价大于0"] += 1
                        passed = False

            if passed:
                vol_last_3 = df.slice(-3).select(pl.sum("volume")).item()

                if not FILTER_CONFIG["基础数据_不过滤停牌"]["ENABLED"]:
                    if vol_last_3 == 0:
                        filter_reason = FILTER_CONFIG["基础数据_不过滤停牌"]["DESC"]
                        detailed_filter_stats["基础数据_不过滤停牌"] += 1
                        passed = False

            if passed:
                high_last = last.select(pl.col("high")).item()
                low_last = last.select(pl.col("low")).item()

                if not FILTER_CONFIG["基础数据_不过滤一字涨跌停"]["ENABLED"]:
                    if close_last == high_last == low_last:
                        filter_reason = FILTER_CONFIG["基础数据_不过滤一字涨跌停"]["DESC"]
                        detailed_filter_stats["基础数据_不过滤一字涨跌停"] += 1
                        passed = False

            if passed:
                df_feat = compute_features(df)

                if not FILTER_CONFIG["特征计算_不过滤"]["ENABLED"]:
                    if df_feat.is_empty():
                        filter_reason = FILTER_CONFIG["特征计算_不过滤"]["DESC"]
                        detailed_filter_stats["特征计算_不过滤"] += 1
                        passed = False

            if passed:
                if model:
                    last_feat = df_feat.select(FEATURES).slice(-1).to_numpy().ravel()
                    prob = predict(model, last_feat.reshape(1, -1))[0]
                else:
                    prob = 0.6

                if not FILTER_CONFIG["模型概率_不过滤"]["ENABLED"]:
                    if prob < FILTER_CONFIG["模型概率_不过滤"]["THRESHOLD"]:
                        filter_reason = f"{FILTER_CONFIG['模型概率_不过滤']['DESC']}(prob={prob:.2f})"
                        detailed_filter_stats["模型概率_不过滤"] += 1
                        passed = False

            if passed:
                if not FILTER_CONFIG["策略买点_不过滤"]["ENABLED"]:
                    if not is_second_buy(df):
                        filter_reason = FILTER_CONFIG["策略买点_不过滤"]["DESC"]
                        detailed_filter_stats["策略买点_不过滤"] += 1
                        passed = False

            if passed:
                if not FILTER_CONFIG["量能确认_不过滤"]["ENABLED"]:
                    if not volume_confirm(df):
                        filter_reason = FILTER_CONFIG["量能确认_不过滤"]["DESC"]
                        detailed_filter_stats["量能确认_不过滤"] += 1
                        passed = False

            if passed:
                detailed_filter_stats["最终入选"] += 1
                return {
                    "code": code,
                    "raw_df": df,
                    "features": df_feat.select(FEATURES).slice(-1).to_numpy().ravel(),
                    "prob": prob if 'prob' in locals() else 0.6
                }
            else:
                return None

        except Exception as e:
            filter_reason = f"程序异常：{str(e)}"
            return None

        finally:
            if debug_print:
                debug_count += 1
                status = "✅ 通过" if passed else f"❌ 淘汰（{filter_reason}）"
                print(f"[{debug_count:03d}] {code:6s} | {status}")


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(load_and_filter, code) for code in stocks]
        for future in tqdm(as_completed(futures), total=len(stocks), desc="预加载进度"):
            res = future.result()
            if res:
                stock_info_list.append(res)

    print("\n" + "=" * 80)
    print("📊 【超详细】全市场逐条件过滤统计")
    print("=" * 80)
    for name, count in detailed_filter_stats.items():
        config = FILTER_CONFIG.get(name, {})
        status = "✅ 启用" if config.get("ENABLED", True) else "❌ 关闭"
        print(f"  {name:30s} | {status:10s} | 淘汰/入选: {count:5d}")
    print("=" * 80)

    results = []
    for stock_info in stock_info_list:
        code = stock_info["code"]
        df = stock_info["raw_df"]
        prob = stock_info["prob"]

        plan = trade_plan(df, prob)
        results.append({"code": code, **plan})

    results = sorted(results, key=lambda x: x["prob"], reverse=True)
    if results:
        output_path = f"选股结果_{datetime.now().strftime('%Y%m%d')}.xlsx"
        pd.DataFrame(results).to_excel(output_path, index=False)
        print(f"\n✅ 有效股票数量：{len(results)}")
        print(f"✅ 结果已保存至：{output_path}")
        print("\n📋 前5只样例：")
        for r in results[:5]:
            print(f"  {r['code']:6s} | 买价:{r['buy']:6.2f} | 止损:{r['stop']:6.2f} | 概率:{r['prob']:.2f}")
    else:
        print("\n❌ 有效股票数量：0")
        print("\n💡 【解决建议】")
        print("  1. 看上面的「超详细过滤统计」")
        print("  2. 找到淘汰最多的条件，把ENABLED改成False")
        print("  3. 重新运行")

    push_results(results, webhook_url=WEBHOOK_URL, platform=PUSH_PLATFORM)

print("\n" + "=" * 80)
print("✅ 程序执行完成")
print("=" * 80)