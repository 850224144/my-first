# run_scan.py
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import polars as pl

try:
    from core.logger import get_logger, print_reject_summary, record_reject
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO)
    _fallback_logger = logging.getLogger("a_stock")

    def get_logger():
        return _fallback_logger

    def print_reject_summary():
        pass

    def record_reject(code: str, reason: str, detail: str = ""):
        _fallback_logger.info(f"reject {code} {reason} {detail}")

logger = get_logger()

from core.data import init_db, get_data_with_status, get_db_connection
from core.daily_cache_builder import build_daily_cache_optimized
from core.universe import build_stock_universe, print_coverage_report
from core.market import get_market_state
from core.alert import push_results
from core.intraday import append_realtime_bar
from core.realtime_guard import refresh_and_validate_realtime, should_require_realtime
from core.daily_refresher import refresh_daily_existing
from core.forward_stats import record_signal_results, update_forward_stats
from core.lifecycle import (
    save_watchlist,
    load_watchlist,
    load_watchlist_codes,
    save_trade_plan,
    generate_daily_report,
)


def parse_args():
    parser = argparse.ArgumentParser(description="A股趋势回踩二买扫描系统")
    parser.add_argument("--init-db", action="store_true", help="初始化数据库")
    parser.add_argument("--preflight", action="store_true", help="数据源预检")
    parser.add_argument("--coverage", action="store_true", help="打印数据覆盖率")
    parser.add_argument("--build-basic", action="store_true", help="构建股票基础表")
    parser.add_argument("--refresh-basic", action="store_true", help="强制刷新股票基础表")
    parser.add_argument("--validate-basic", action="store_true", help="新浪实时校验股票基础表")
    parser.add_argument("--build-daily-cache", action="store_true", help="构建/扩容历史K缓存")
    parser.add_argument("--refresh-daily-existing", action="store_true", help="正式收盘刷新已有有效日线缓存")
    parser.add_argument("--build-universe", action="store_true", help="构建股票池")
    parser.add_argument("--mode", choices=["observe", "tail_confirm", "after_close"], default=None)
    parser.add_argument("--watchlist-refresh", action="store_true", help="只刷新今日 watchlist；今日 watchlist 为空则回退全市场 observe")
    parser.add_argument("--daily-report", action="store_true", help="生成交易日报")
    parser.add_argument("--limit", type=int, default=None, help="扫描/股票池限制数量")
    parser.add_argument("--workers", type=int, default=1, help="扫描线程数")
    parser.add_argument("--daily-limit", type=int, default=None, help="历史K/已有缓存刷新数量")
    parser.add_argument("--daily-workers", type=int, default=1, help="历史K缓存线程数")
    parser.add_argument("--allow-remote-in-scan", action="store_true", help="扫描时允许远程补拉历史K")
    parser.add_argument("--no-realtime-refresh", action="store_true", help="盘中扫描不刷新实时行情，仅用于调试")
    parser.add_argument("--realtime-min-success", type=float, default=0.7, help="实时行情最低成功率，低于则停止盘中扫描")
    parser.add_argument("--realtime-max-age-minutes", type=int, default=20, help="实时行情最大允许陈旧分钟数")
    parser.add_argument("--strict-sector", action="store_true", help="严格板块过滤")
    parser.add_argument("--strict-weekly", action="store_true", help="严格周线过滤")
    parser.add_argument("--webhook", type=str, default="", help="企业微信/钉钉 webhook")
    parser.add_argument("--platform", choices=["wechat", "dingtalk"], default="wechat")
    return parser.parse_args()


def auto_mode() -> str:
    return "observe"


def run_preflight():
    try:
        from core.data import preflight_check
        result = preflight_check()
        print("数据源预检：")
        print(result)
        logger.info(f"数据源预检：{result}")
        return result
    except Exception as e:
        print(f"⚠️ 数据源预检失败：{e}")
        logger.error(f"数据源预检失败：{e}", exc_info=True)
        return None


def run_build_basic(args):
    try:
        from core.data import build_stock_basic
        try:
            return build_stock_basic(refresh=args.refresh_basic)
        except TypeError:
            return build_stock_basic()
    except Exception as e:
        print(f"⚠️ 构建股票基础表失败：{e}")
        logger.error(f"构建股票基础表失败：{e}", exc_info=True)
        return None


def run_validate_basic():
    try:
        from core.data import validate_stock_basic_by_sina
        return validate_stock_basic_by_sina()
    except Exception as e:
        print(f"⚠️ 新浪实时校验失败：{e}")
        logger.error(f"新浪实时校验失败：{e}", exc_info=True)
        return None


def run_build_universe(args):
    try:
        df = build_stock_universe(limit=args.limit, workers=args.workers)
    except TypeError:
        try:
            df = build_stock_universe(limit=args.limit)
        except TypeError:
            df = build_stock_universe()
    if df is None:
        df = pl.DataFrame()
    try:
        print(df.head(20))
    except Exception:
        print(df)
    logger.info(f"构建股票池完成，数量={len(df)}")
    print_reject_summary()
    return df


def load_universe(limit: Optional[int] = None) -> pl.DataFrame:
    path = "data/universe.parquet"
    if not os.path.exists(path):
        print("⚠️ 股票池缓存不存在，请先执行：python run_scan.py --build-universe --workers 1")
        logger.warning("股票池缓存不存在")
        return pl.DataFrame()
    df = pl.read_parquet(path)
    if limit:
        df = df.head(limit)
    logger.info(f"使用股票池缓存：{len(df)} 只")
    return df


def load_daily_from_cache(code: str, bars: int = 520) -> pl.DataFrame:
    con = get_db_connection()
    try:
        rows = con.execute(
            """
            SELECT date, open, high, low, close, volume, amount, adj_type, source
            FROM stock_daily
            WHERE code = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            [str(code).zfill(6), bars],
        ).fetchall()
        cols = [x[0] for x in con.description]
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows, schema=cols, orient="row")
        df = df.with_columns([
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
        ])
        return df.sort("date")
    finally:
        con.close()


def apply_sector_filter(universe_df: pl.DataFrame, market_state: Dict[str, Any], strict_sector: bool = False) -> pl.DataFrame:
    if universe_df.is_empty():
        return universe_df
    try:
        from core.sector import filter_universe_by_strong_sector
        filtered = filter_universe_by_strong_sector(universe_df, market_state=market_state, strict=strict_sector)
        if filtered is None or filtered.is_empty():
            if strict_sector:
                print("⚠️ 严格板块模式：板块过滤后为空，停止扫描")
                return pl.DataFrame()
            print("⚠️ 强势行业内无股票或板块成分匹配失败，调试模式跳过板块过滤继续扫描")
            return universe_df
        logger.info(f"板块过滤完成：input={len(universe_df)} output={len(filtered)}")
        return filtered
    except Exception as e:
        logger.warning(f"板块过滤失败：{e}")
        if strict_sector:
            print("⚠️ 严格板块模式：板块过滤失败，停止扫描")
            return pl.DataFrame()
        print("⚠️ 板块过滤失败，调试模式跳过板块过滤继续扫描")
        return universe_df


def apply_weekly_filter(universe_df: pl.DataFrame, strict_weekly: bool = False) -> pl.DataFrame:
    if universe_df.is_empty():
        return universe_df
    try:
        from core.weekly import filter_by_weekly_trend
        filtered = filter_by_weekly_trend(universe_df, strict=strict_weekly)
        if filtered is None or filtered.is_empty():
            if strict_weekly:
                print("⚠️ 严格周线模式：周线过滤后为空，停止扫描")
                return pl.DataFrame()
            print("⚠️ 周线过滤后为空，调试模式跳过周线过滤继续扫描")
            return universe_df
        logger.info(f"周线过滤完成：input={len(universe_df)} output={len(filtered)}")
        return filtered
    except Exception as e:
        logger.warning(f"周线过滤失败：{e}")
        if strict_weekly:
            print("⚠️ 严格周线模式：周线过滤失败，停止扫描")
            return pl.DataFrame()
        print("⚠️ 周线过滤失败，调试模式跳过周线过滤继续扫描")
        return universe_df


def _get_strategy_functions():
    import core.strategy as strategy
    score_func = getattr(strategy, "score_second_buy", None) or getattr(strategy, "is_second_buy", None)
    plan_func = getattr(strategy, "make_trade_plan", None) or getattr(strategy, "trade_plan", None)
    return score_func, plan_func


def scan_one(code: str, mode: str, allow_remote: bool = False) -> Optional[Dict[str, Any]]:
    try:
        if allow_remote:
            df, status = get_data_with_status(code, bars=520, force_refresh=True)
        else:
            df = load_daily_from_cache(code, bars=520)
            status = "cache"

        intraday_mode = "watchlist_refresh" if mode == "watchlist_refresh" else mode
        if intraday_mode in {"observe", "tail_confirm", "watchlist_refresh"}:
            df = append_realtime_bar(df, code, mode=intraday_mode)
            status = f"{status}+intraday_bar"

        if df is None or df.is_empty() or len(df) < 250:
            record_reject(code, "data_not_enough", "日线不足250根")
            return None

        score_func, plan_func = _get_strategy_functions()
        if score_func is None:
            record_reject(code, "strategy_missing", "strategy.py 中没有 score_second_buy/is_second_buy")
            return None

        try:
            score_result = score_func(df, mode="observe" if mode == "watchlist_refresh" else mode)
        except TypeError:
            score_result = score_func(df)

        if not score_result:
            record_reject(code, "second_buy_not_match", "二买结构不满足")
            return None

        if isinstance(score_result, dict):
            total_score = score_result.get("total_score") or score_result.get("score") or score_result.get("total") or 0
        else:
            total_score = 70 if score_result is True else 0

        min_score = 70 if mode in {"observe", "watchlist_refresh"} else 80
        if total_score < min_score:
            record_reject(code, "score_too_low", f"score={total_score}")
            return None

        plan = None
        if plan_func:
            try:
                plan = plan_func(df, score_result)
            except TypeError:
                try:
                    plan = plan_func(df)
                except TypeError:
                    plan = None
        plan = plan or {}

        return {
            "code": str(code).zfill(6),
            "name": "",
            "signal": score_result.get("signal") if isinstance(score_result, dict) else None,
            "total_score": total_score,
            "score": total_score,
            "trend_score": score_result.get("trend_score", 0) if isinstance(score_result, dict) else 0,
            "pullback_score": score_result.get("pullback_score", 0) if isinstance(score_result, dict) else 0,
            "stabilize_score": score_result.get("stabilize_score", 0) if isinstance(score_result, dict) else 0,
            "confirm_score": score_result.get("confirm_score", 0) if isinstance(score_result, dict) else 0,
            "raw_score": score_result.get("raw_score", total_score) if isinstance(score_result, dict) else total_score,
            "warnings": score_result.get("warnings", []) if isinstance(score_result, dict) else [],
            "veto": score_result.get("veto", False) if isinstance(score_result, dict) else False,
            "veto_reasons": score_result.get("veto_reasons", []) if isinstance(score_result, dict) else [],
            "entry_price": plan.get("entry_price"),
            "trigger_price": plan.get("trigger_price"),
            "stop_loss": plan.get("stop_loss"),
            "take_profit_1": plan.get("take_profit_1"),
            "take_profit_2": plan.get("take_profit_2"),
            "position_suggestion": plan.get("position_suggestion"),
            "risk_pct": plan.get("risk_pct"),
            "risk_level": plan.get("risk_level"),
            "action": plan.get("action"),
            "entry_type": plan.get("entry_type"),
            "invalid_condition": plan.get("invalid_condition"),
            "note": plan.get("note"),
            "mode": mode,
            "score_detail": score_result,
            "plan": plan,
            "data_status": status,
        }
    except Exception as e:
        record_reject(str(code), "exception", str(e))
        logger.debug(f"扫描异常 code={code} | {e}", exc_info=True)
        return None


def _filter_universe_by_codes(universe_df: pl.DataFrame, codes: List[str]) -> pl.DataFrame:
    if universe_df.is_empty() or not codes:
        return pl.DataFrame()
    return universe_df.filter(pl.col("code").cast(pl.Utf8).is_in([str(c).zfill(6) for c in codes]))


def load_today_watchlist_codes() -> List[str]:
    try:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        df = load_watchlist(active_only=True)
        if df.is_empty() or "code" not in df.columns:
            return []
        if "date" in df.columns:
            today_df = df.filter(pl.col("date").cast(pl.Utf8) == today)
            if not today_df.is_empty():
                return today_df["code"].cast(pl.Utf8).unique().to_list()
        if "last_seen_at" in df.columns:
            today_df = df.filter(pl.col("last_seen_at").cast(pl.Utf8).str.starts_with(today))
            if not today_df.is_empty():
                return today_df["code"].cast(pl.Utf8).unique().to_list()
        return []
    except Exception:
        return []


def _load_scan_universe(args, mode: str) -> pl.DataFrame:
    universe_df = load_universe(limit=args.limit)
    if universe_df.is_empty():
        return universe_df
    if getattr(args, "watchlist_refresh", False):
        today_codes = load_today_watchlist_codes()
        if today_codes:
            filtered = _filter_universe_by_codes(universe_df, today_codes)
            if not filtered.is_empty():
                print(f"📌 watchlist_refresh 扫描今日 watchlist：{len(filtered)} 只")
                return filtered
        print("⚠️ 今日 watchlist 为空或过期，watchlist_refresh 自动回退为全市场 observe")
        return universe_df
    if mode == "tail_confirm":
        today_codes = load_today_watchlist_codes()
        if today_codes:
            filtered = _filter_universe_by_codes(universe_df, today_codes)
            if not filtered.is_empty():
                print(f"📌 tail_confirm 优先扫描今日 watchlist：{len(filtered)} 只")
                return filtered
            print("⚠️ 今日 watchlist 与 universe 无交集，回退扫描全部 universe")
        else:
            print("⚠️ 今日 watchlist 为空，tail_confirm 回退扫描全部 universe")
    return universe_df


def _refresh_realtime_for_scan(args, mode: str, codes: List[str], market_state: Dict[str, Any]) -> bool:
    if args.no_realtime_refresh or not should_require_realtime(mode):
        return True
    if not codes:
        return True
    # 全市场 900 只按 400/批、10秒间隔；watchlist几十只会很快。
    batch_interval = 10 if len(codes) > 400 else 0
    summary = refresh_and_validate_realtime(
        codes=codes,
        batch_size=400,
        batch_interval=batch_interval,
        min_success_rate=args.realtime_min_success,
        max_age_minutes=args.realtime_max_age_minutes,
    )
    market_state["quote_status"] = summary
    print(summary.get("message"))
    logger.info(summary.get("message"))
    if not summary.get("ok"):
        print("⚠️ 实时行情刷新失败或成功率过低，本轮盘中扫描停止，避免使用旧数据出信号")
        logger.error(f"实时行情刷新失败，本轮停止：{summary}")
        return False
    return True


def run_scan(args, market_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    mode = "watchlist_refresh" if getattr(args, "watchlist_refresh", False) else (args.mode or auto_mode())
    universe_df = _load_scan_universe(args, mode)
    if universe_df.is_empty():
        print("⚠️ 股票池为空，停止扫描")
        return []

    universe_df = apply_sector_filter(universe_df, market_state=market_state, strict_sector=args.strict_sector)
    if universe_df.is_empty():
        print("⚠️ 板块过滤后无股票，停止扫描")
        return []

    universe_df = apply_weekly_filter(universe_df, strict_weekly=args.strict_weekly)
    if universe_df.is_empty():
        print("⚠️ 周线过滤后无股票，停止扫描")
        return []

    codes = universe_df["code"].cast(pl.Utf8).to_list()
    if not _refresh_realtime_for_scan(args, mode, codes, market_state):
        return []

    print_coverage_report()
    print(f"开始扫描：{len(codes)} 只，模式={mode} ...")
    results: List[Dict[str, Any]] = []
    processed = 0
    skipped = 0
    workers = max(1, int(args.workers or 1))
    score_mode = "observe" if mode == "watchlist_refresh" else mode

    if workers == 1:
        for code in codes:
            item = scan_one(code, mode=score_mode, allow_remote=args.allow_remote_in_scan)
            processed += 1
            if item:
                results.append(item)
            else:
                skipped += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(scan_one, code, score_mode, args.allow_remote_in_scan): code for code in codes}
            for fut in as_completed(futures):
                processed += 1
                try:
                    item = fut.result()
                except Exception:
                    item = None
                if item:
                    results.append(item)
                else:
                    skipped += 1

    results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)
    print("\n扫描报告：")
    print(f"  股票池：{len(codes)}")
    print(f"  成功处理：{processed}")
    print(f"  异常/无信号跳过：{skipped}")
    print(f"  信号候选：{len(results)}")
    logger.info(f"扫描报告：universe={len(codes)} processed={processed} skipped={skipped} candidates={len(results)}")

    if mode in {"observe", "watchlist_refresh"}:
        watch = save_watchlist(results, mode="observe", market_state=market_state)
        record_signal_results(results, mode=mode)
        print(f"📌 watchlist 已更新：{len(watch)} 条")
    elif mode == "tail_confirm":
        watch = save_watchlist(results, mode=mode, market_state=market_state)
        record_signal_results(results, mode=mode)
        print(f"📌 watchlist 尾盘状态已更新：{len(watch)} 条")
    elif mode == "after_close":
        watch = save_watchlist(results, mode=mode, market_state=market_state)
        plan = save_trade_plan(results, mode=mode, market_state=market_state)
        record_signal_results(results, mode=mode)
        update_forward_stats()
        print(f"📌 watchlist 已更新：{len(watch)} 条")
        print(f"📝 trade_plan 已生成：{len(plan)} 条")
    return results


def print_market_state(market_state: Dict[str, Any]):
    print("=" * 70)
    print(f"📈 大盘状态：{market_state.get('state')} | {market_state.get('message')}")
    for key, name in [("sh", "上证"), ("cyb", "创业板")]:
        idx = market_state.get(key)
        if isinstance(idx, dict) and idx:
            try:
                print(f"{name}：{idx.get('date')} close={idx.get('close'):.2f} ma20={idx.get('ma20'):.2f} ma60={idx.get('ma60'):.2f} data={idx.get('data_status')}")
            except Exception:
                print(f"{name}：{idx}")
    print("=" * 70)


def main():
    args = parse_args()
    logger.info("run_scan 启动")

    if args.init_db:
        init_db()
        print("✅ 数据库初始化完成")
        return
    if args.preflight:
        run_preflight()
        return
    if args.coverage:
        print_coverage_report()
        return
    if args.build_basic:
        run_build_basic(args)
        return
    if args.validate_basic:
        run_validate_basic()
        return
    if args.build_daily_cache:
        stats = build_daily_cache_optimized(daily_limit=args.daily_limit, daily_workers=args.daily_workers)
        print(stats)
        return
    if args.refresh_daily_existing:
        stats = refresh_daily_existing(limit=args.daily_limit, workers=args.daily_workers)
        print(stats)
        return
    if args.build_universe:
        run_build_universe(args)
        return
    if args.daily_report:
        update_forward_stats()
        content = generate_daily_report()
        print(content)
        try:
            from core.notify import notify_daily_report
            notify_daily_report(content, webhook=os.getenv("WECHAT_WEBHOOK", ""))
        except Exception as e:
            print(f"⚠️ 日报企业微信推送失败：{e}")
        return

    mode = "watchlist_refresh" if args.watchlist_refresh else (args.mode or auto_mode())
    market_state = get_market_state()
    print_market_state(market_state)

    if market_state.get("state") in {"弱势", "risk_off"}:
        print("⚠️ 弱势或风险关闭市场，不产生新开仓信号")
        logger.warning("弱势或 risk_off 市场，不产生新开仓信号")
        results = []
    else:
        results = run_scan(args, market_state=market_state)

    webhook = args.webhook or os.getenv("WECHAT_WEBHOOK", "")
    try:
        push_results(results, market_state=market_state, mode=mode, webhook=webhook, platform=args.platform)
    except TypeError:
        push_results(results)
    print_reject_summary()


if __name__ == "__main__":
    main()
