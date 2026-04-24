# -*- coding: utf-8 -*-
"""
run_scan.py

A股类缠论二买 V1 主入口。

方案 D 数据层新流程：
1. build-basic：AkShare 获取真实 A 股基础表，并缓存。
2. validate-basic：新浪实时批量校验，400只/批，单线程，10秒间隔。
3. build-daily-cache：历史K分阶段缓存，腾讯主源，备用源兜底。
4. prepare-data：依次执行 1-3。
5. scan：只基于已经准备好的数据进行策略扫描。
"""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional

from core.data import (
    init_db,
    get_data_with_status,
    STATUS_NO_DATA,
    build_stock_basic,
    validate_stock_basic_with_sina,
    build_daily_cache,
    patch_daily_from_sina_realtime,
    preflight_data_sources,
)
from core.feature import compute_features
from core.strategy import evaluate_second_buy, trade_plan, get_scan_mode, SIGNAL_NONE
from core.market import get_market_state, MARKET_RISK_OFF
from core.universe import build_stock_universe, prepare_data
from core.sector import get_top_sectors, filter_universe_by_sectors
from core.model import load_model, rank_candidates_by_model
from core.alert import push_results
from core.logger import setup_logger, get_logger, log_reject, log_exception, summarize_rejects


def scan_one(row: Dict[str, Any], scan_mode: str, market_state: str) -> Optional[Dict[str, Any]]:
    code = row["code"]
    name = row.get("name", "")
    df, data_status = get_data_with_status(code, bars=360)
    if df is None:
        log_reject(code, "scan", "no_kline_data", f"data_status={data_status}", name=name)
        return None
    if len(df) < 260 or data_status == STATUS_NO_DATA:
        log_reject(code, "scan", "data_not_enough", f"bars={len(df)}, data_status={data_status}", name=name)
        return None

    feat = compute_features(df)
    if feat.is_empty() or len(feat) < 130:
        log_reject(code, "scan", "feature_not_enough", f"feature_rows={len(feat) if feat is not None else 0}", name=name)
        return None

    res = evaluate_second_buy(feat, scan_mode=scan_mode)
    if res.get("signal") == SIGNAL_NONE:
        log_reject(code, "strategy", "second_buy_not_match", " | ".join(res.get("reasons", [])), name=name)
        return None

    plan = trade_plan(feat, signal_result=res, market_state=market_state)
    if not plan.get("valid"):
        log_reject(code, "trade_plan", "invalid_plan", plan.get("reason", ""), name=name)
        return None

    return {
        "code": code,
        "name": name,
        "industry": row.get("industry", "未知"),
        "market": row.get("market", ""),
        "data_status": data_status,
        "signal": res.get("signal"),
        "score": res.get("score"),
        "scores": res.get("scores"),
        "reasons": res.get("reasons"),
        "meta": res.get("meta"),
        "plan": plan,
        "df_feat": feat,
    }


def scan_all(
    mode: Optional[str] = None,
    use_cache_universe: bool = True,
    max_workers: int = 2,
    limit: Optional[int] = None,
    webhook_url: Optional[str] = None,
    platform: str = "wechat",
    log_level: str = "INFO",
    log_dir: str = "logs",
    log_file: Optional[str] = None,
) -> List[Dict[str, Any]]:
    setup_logger(log_dir=log_dir, level=log_level, log_file=log_file)
    init_db()
    scan_mode = mode or get_scan_mode()

    market = get_market_state(debug=True)
    if market["state"] == MARKET_RISK_OFF:
        push_results([], webhook_url=webhook_url, platform=platform, market=market, scan_mode=scan_mode)
        summarize_rejects()
        return []

    universe = build_stock_universe(max_workers=max_workers, limit=limit, use_cache=use_cache_universe)
    if universe.is_empty():
        print("⚠️ 股票池为空，停止扫描")
        summarize_rejects()
        return []

    top_pct = float(market.get("sector_top_pct", 0.20))
    sector_table, top_sectors = get_top_sectors(universe, top_pct=top_pct, max_workers=max_workers)
    if not top_sectors:
        print("⚠️ 板块强度计算失败，保守处理：不扫描")
        summarize_rejects()
        return []

    print("\n强势行业：")
    print(sector_table.head(20))

    scan_universe = filter_universe_by_sectors(universe, top_sectors)
    if scan_universe.is_empty():
        print("⚠️ 强势行业内无股票，停止扫描")
        summarize_rejects()
        return []

    rows = scan_universe.to_dicts()
    if limit:
        rows = rows[:limit]

    print(f"开始扫描：{len(rows)} 只，模式={scan_mode} ...")
    candidates: List[Dict[str, Any]] = []
    workers = max(1, int(max_workers or 1))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scan_one, row, scan_mode, market["state"]): row["code"] for row in rows}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                res = fut.result()
                if res:
                    candidates.append(res)
            except Exception as e:
                code = futures.get(fut, "")
                log_reject(code, "scan", "future_exception", e)
                log_exception(f"扫描线程异常 code={code}", e)
            if done % 200 == 0:
                print(f"扫描进度：{done}/{len(rows)}，候选 {len(candidates)}")

    model = load_model()
    ranked = rank_candidates_by_model(candidates, model=model)
    for item in ranked:
        item.pop("df_feat", None)

    max_push = int(market.get("max_push", 10)) or 10
    push_results(ranked, webhook_url=webhook_url, platform=platform, market=market, scan_mode=scan_mode, top_n=max_push)
    summarize_rejects()
    return ranked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-db", action="store_true", help="初始化数据库")
    parser.add_argument("--preflight", action="store_true", help="数据源预检")
    parser.add_argument("--build-basic", action="store_true", help="构建/刷新 stock_basic 股票基础表")
    parser.add_argument("--validate-basic", action="store_true", help="用新浪实时批量接口校验 stock_basic")
    parser.add_argument("--build-daily-cache", action="store_true", help="构建/更新历史K缓存")
    parser.add_argument("--patch-daily-from-sina", action="store_true", help="15:05后用新浪实时行情补当天日线")
    parser.add_argument("--prepare-data", action="store_true", help="依次执行 build-basic、validate-basic、build-daily-cache")
    parser.add_argument("--build-universe", action="store_true", help="根据 stock_basic + 历史K过滤构建股票池")
    parser.add_argument("--refresh-basic", action="store_true", help="强制刷新 stock_basic，忽略7天缓存")
    parser.add_argument("--basic-cache-days", type=int, default=7, help="stock_basic 缓存有效天数，默认7天")
    parser.add_argument("--sina-batch-size", type=int, default=400, help="新浪实时批量大小，默认400只/批")
    parser.add_argument("--sina-batch-interval", type=float, default=10.0, help="新浪实时批次间隔，默认10秒")
    parser.add_argument("--daily-limit", type=int, default=None, help="历史K缓存处理数量限制，调试用")
    parser.add_argument("--daily-workers", type=int, default=1, help="历史K缓存线程数，默认1")
    parser.add_argument("--mode", choices=["observe", "tail_confirm", "after_close"], default=None, help="扫描模式")
    parser.add_argument("--no-cache-universe", action="store_true", help="不使用股票池缓存，重新构建")
    parser.add_argument("--workers", type=int, default=2, help="扫描/股票池过滤线程数，默认2")
    parser.add_argument("--limit", type=int, default=None, help="限制股票池/扫描数量，调试用")
    parser.add_argument("--webhook", type=str, default=os.getenv("WECHAT_WEBHOOK", ""))
    parser.add_argument("--platform", choices=["wechat", "dingtalk"], default="wechat")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=os.getenv("LOG_LEVEL", "INFO"))
    parser.add_argument("--log-dir", type=str, default=os.getenv("LOG_DIR", "logs"))
    parser.add_argument("--log-file", type=str, default=None)
    args = parser.parse_args()

    setup_logger(log_dir=args.log_dir, level=args.log_level, log_file=args.log_file)

    if args.init_db:
        init_db()
        print("✅ 数据库初始化完成")
        get_logger().info("数据库初始化完成")
        return

    if args.preflight:
        init_db()
        print(preflight_data_sources())
        return

    if args.build_basic:
        init_db()
        df = build_stock_basic(force_refresh=args.refresh_basic, cache_days=args.basic_cache_days)
        print(df.head(20))
        print(f"✅ stock_basic 数量：{len(df)}")
        return

    if args.validate_basic:
        init_db()
        quote = validate_stock_basic_with_sina(batch_size=args.sina_batch_size, batch_interval=args.sina_batch_interval)
        print(quote.head(20))
        print(f"✅ 新浪实时有效记录：{len(quote)}")
        return

    if args.build_daily_cache:
        init_db()
        stats = build_daily_cache(limit=args.daily_limit, workers=args.daily_workers)
        print(stats)
        summarize_rejects()
        return

    if args.patch_daily_from_sina:
        init_db()
        count = patch_daily_from_sina_realtime(batch_size=args.sina_batch_size, batch_interval=args.sina_batch_interval)
        print(f"✅ 新浪实时补日线：{count} 条")
        return

    if args.prepare_data:
        init_db()
        res = prepare_data(
            refresh_basic=args.refresh_basic,
            basic_cache_days=args.basic_cache_days,
            sina_batch_size=args.sina_batch_size,
            sina_batch_interval=args.sina_batch_interval,
            daily_limit=args.daily_limit,
            daily_workers=args.daily_workers,
        )
        print(res)
        summarize_rejects()
        return

    if args.build_universe:
        init_db()
        u = build_stock_universe(
            max_workers=args.workers,
            limit=args.limit,
            use_cache=False,
            force_refresh_basic=args.refresh_basic,
            basic_cache_days=args.basic_cache_days,
        )
        print(u.head(20))
        get_logger().info("构建股票池完成，数量=%s", len(u) if u is not None else 0)
        return

    scan_all(
        mode=args.mode,
        use_cache_universe=not args.no_cache_universe,
        max_workers=args.workers,
        limit=args.limit,
        webhook_url=args.webhook or None,
        platform=args.platform,
        log_level=args.log_level,
        log_dir=args.log_dir,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    main()
