# -*- coding: utf-8 -*-
"""
A股量化选股系统 - 性能优化版
优化内容：
1. 零成本优化：修复重复执行BUG、前置过滤、Parquet列裁剪
2. 中等成本优化：批量模型预测（提速10倍+）
"""

import os
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime

from core.universe import get_all_stocks
from core.data import get_data, clean_old_cache
from core.feature import compute_features, FEATURES
from core.model import load_model, predict, train_general_model
from core.strategy import is_second_buy, volume_confirm, trade_plan
from core.market import market_filter
from core.alert import push_results

# ==============================================================================
# 📝 配置区
# ==============================================================================
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b"
PUSH_PLATFORM = "wechat"
MODE = "scan"  # train / backtest / scan
DEBUG_PRINT_LIMIT = 20
PROB_THRESHOLD = 0.5

# 【优化】按CPU核心数设置合理线程数
import os

MAX_WORKERS = min(os.cpu_count() * 3, 32)

# ==============================================================================
# 🚀 程序主逻辑
# ==============================================================================

print("=" * 80)
print("📊 A股量化选股系统（性能优化版）")
print("=" * 80)

# 全局过滤统计
filter_stats = {
    "1_基础数据过滤": 0,
    "2_大盘环境过滤": 0,
    "3_特征计算": 0,
    "4_模型概率打分": 0,
    "5_核心策略买点": 0,
    "6_量能确认": 0,
    "最终入选": 0
}

if MODE == "train":
    # --------------------------------------------------------------------------
    # 模式 1：训练通用模型
    # --------------------------------------------------------------------------
    print("\n【模式】训练通用模型")
    print("-" * 80)

    # 【优化】启动时只执行1次缓存清理
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
                    if not df_feat.empty:
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
    # --------------------------------------------------------------------------
    # 模式 2：每日选股（默认，性能优化版）
    # --------------------------------------------------------------------------
    print("\n【模式】每日选股（性能优化版）")
    print("-" * 80)

    # 【优化1】启动时只执行1次缓存清理
    clean_old_cache()

    # 【优化2】启动时只执行1次大盘过滤
    print("📈 校验大盘环境...")
    market_pass = market_filter()

    if not market_pass:
        print("\n❌ 大盘未站上20日线，关闭选股")
        print("=" * 80)
        print("✅ 程序执行完成")
        print("=" * 80)
        exit()

    stocks = get_all_stocks()
    print(f"✅ 市场股票总数: {len(stocks)}")

    # 加载模型
    model = load_model()
    if model is None:
        print("⚠️  未找到通用模型，将使用默认概率 0.6")
    else:
        print("✅ 通用模型加载成功")

    # ==========================================================================
    # 【核心优化3】第一阶段：预加载+前置过滤+批量特征计算
    # 只有通过前置过滤的股票，才进入下一阶段
    # ==========================================================================
    print("\n📥 第一阶段：预加载全市场数据并计算特征...")
    stock_info_list = []


    def load_and_filter(code):
        try:
            df = get_data(code)

            # 【优化4】前置基础数据过滤，不合格直接return
            if df is None or len(df) < 100:
                return None

            last = df.iloc[-1]
            if (float(last["close"]) <= 0
                    or df["volume"].iloc[-3:].sum() == 0
                    or last["close"] == last["high"] == last["low"]):
                return None

            # 计算特征
            df_feat = compute_features(df)
            if df_feat.empty:
                return None

            # 只保留最后一行特征用于预测
            last_feat = df_feat[FEATURES].iloc[-1]

            return {
                "code": code,
                "raw_df": df,
                "features": last_feat
            }

        except:
            return None


    # 多线程预加载
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(load_and_filter, code) for code in stocks]
        for future in tqdm(as_completed(futures), total=len(stocks), desc="预加载进度"):
            res = future.result()
            if res:
                stock_info_list.append(res)

    # 更新过滤统计
    filter_stats["1_基础数据过滤"] = len(stocks) - len(stock_info_list)
    print(f"\n✅ 通过第一阶段筛选：{len(stock_info_list)} 只")

    if not stock_info_list:
        print("\n❌ 没有通过第一阶段筛选的股票")
    else:
        # ======================================================================
        # 【核心优化5】第二阶段：批量模型预测（一次性完成，提速10倍+）
        # ======================================================================
        print("\n🤖 第二阶段：批量模型预测...")

        # 构建特征矩阵
        feature_matrix = np.array([s["features"] for s in stock_info_list])

        # 批量预测
        if model:
            probs = predict(model, feature_matrix)
        else:
            probs = [0.6] * len(stock_info_list)

        # ======================================================================
        # 【核心优化6】第三阶段：策略筛选+生成交易计划
        # ======================================================================
        print("\n📋 第三阶段：策略筛选...")
        results = []
        debug_count = 0

        for idx, stock_info in enumerate(stock_info_list):
            code = stock_info["code"]
            df = stock_info["raw_df"]
            prob = probs[idx]

            # 调试打印（只打印前N只）
            debug_print = (debug_count < DEBUG_PRINT_LIMIT)
            step = 0
            reason = ""
            passed = False

            try:
                # 关卡4：模型概率打分
                step = 4
                if prob < PROB_THRESHOLD:
                    reason = f"概率不足（prob={prob:.2f}）"
                    filter_stats["4_模型概率打分"] += 1
                    continue

                # 关卡5：核心策略买点
                step = 5
                if not is_second_buy(df):
                    reason = "未满足策略买点条件"
                    filter_stats["5_核心策略买点"] += 1
                    continue

                # 关卡6：量能确认
                step = 6
                if not volume_confirm(df):
                    reason = "量能不足"
                    filter_stats["6_量能确认"] += 1
                    continue

                # 最终入选
                passed = True
                filter_stats["最终入选"] += 1
                plan = trade_plan(df, prob)
                results.append({"code": code, **plan})

            except Exception as e:
                reason = f"异常：{str(e)}"
                continue

            finally:
                if debug_print:
                    debug_count += 1
                    status = "✅ 通过" if passed else f"❌ 淘汰（{reason}）"
                    print(f"[{debug_count:02d}] {code:6s} | 关卡{step} | {status}")

        # ======================================================================
        # 输出结果
        # ======================================================================
        print("\n" + "=" * 80)
        print("📊 全市场过滤统计")
        print("=" * 80)
        for k, v in filter_stats.items():
            print(f"  {k:20s}: {v:5d}")
        print("=" * 80)

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
            print("💡 建议：降低 PROB_THRESHOLD 或放宽策略条件")

        # 推送结果
        push_results(results, webhook_url=WEBHOOK_URL, platform=PUSH_PLATFORM)

print("\n" + "=" * 80)
print("✅ 程序执行完成")
print("=" * 80)