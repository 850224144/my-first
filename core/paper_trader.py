# core/paper_trader.py
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
import os
import uuid

import polars as pl

try:
    from core.notify import send_markdown
except Exception:
    send_markdown = None

PAPER_POSITIONS_PATH = "data/paper_positions.parquet"
PAPER_JOURNAL_PATH = "data/paper_trade_journal.parquet"
PAPER_STATS_PATH = "data/paper_daily_stats.parquet"
PAPER_REPORT_DIR = "data/reports"
PROJECT_NAME = os.getenv("PROJECT_NOTIFY_NAME", "A股二买交易助手")
STRATEGY_NAME = os.getenv("PAPER_STRATEGY_NAME", "second_buy_v1")

# 纸面账户参数：只用于统计，不真实下单
PAPER_ACCOUNT_SIZE = float(os.getenv("PAPER_ACCOUNT_SIZE", "100000"))
MAX_OPEN_POSITIONS = int(os.getenv("PAPER_MAX_OPEN_POSITIONS", "5"))
DEFAULT_SHARES = int(os.getenv("PAPER_DEFAULT_SHARES", "100"))
BUY_SLIPPAGE_PCT = float(os.getenv("PAPER_BUY_SLIPPAGE_PCT", "0.001"))
SELL_SLIPPAGE_PCT = float(os.getenv("PAPER_SELL_SLIPPAGE_PCT", "0.001"))
COMMISSION_PCT = float(os.getenv("PAPER_COMMISSION_PCT", "0.0003"))
STAMP_TAX_PCT = float(os.getenv("PAPER_STAMP_TAX_PCT", "0.0005"))

HARD_BLOCK_WARNINGS = {
    "no_breakout",
    "volume_not_confirm",
    "too_hot_today",
    "risk_too_high",
    "market_weak",
    "data_stale",
    "realtime_failed",
}

SOFT_WARNINGS = {
    "pullback_days_not_ideal",
    "volatility_not_contracting",
    "lows_not_rising",
    "ma60_slight_break",
}


def _ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(PAPER_REPORT_DIR, exist_ok=True)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _parse_date(v: Any) -> Optional[date]:
    try:
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        s = str(v)[:10]
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _next_trading_day(d: Optional[date] = None) -> str:
    """
    简化版交易日：跳过周六周日。
    后续可接真实交易日历。
    """
    d = d or datetime.now().date()
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:
        nd += timedelta(days=1)
    return nd.strftime("%Y-%m-%d")


def _read_parquet(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame()


def _write_parquet(df: pl.DataFrame, path: str):
    _ensure_dirs()
    df.write_parquet(path)


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return ",".join([str(x) for x in v])
    if isinstance(v, dict):
        return str(v)
    return str(v)


def _nested_get(item: Dict[str, Any], key: str, default=None):
    if not isinstance(item, dict):
        return default
    v = item.get(key)
    if v not in [None, ""]:
        return v
    sd = item.get("score_detail")
    if isinstance(sd, dict):
        v = sd.get(key)
        if v not in [None, ""]:
            return v
    plan = item.get("plan")
    if isinstance(plan, dict):
        v = plan.get(key)
        if v not in [None, ""]:
            return v
    return default


def _warnings_list(item: Dict[str, Any]) -> List[str]:
    w = _nested_get(item, "warnings", [])
    if w is None:
        return []
    if isinstance(w, list):
        return [str(x) for x in w]
    if isinstance(w, str):
        return [x.strip() for x in w.split(",") if x.strip()]
    return [str(w)]


def _get_db_connection():
    from core.data import get_db_connection
    return get_db_connection()


def _lookup_name(code: str) -> str:
    try:
        con = _get_db_connection()
        row = con.execute("SELECT name FROM stock_basic WHERE code=? LIMIT 1", [str(code).zfill(6)]).fetchone()
        con.close()
        return str(row[0]) if row and row[0] else ""
    except Exception:
        return ""


def _load_latest_quote(code: str) -> Dict[str, Any]:
    code = str(code).zfill(6)
    try:
        con = _get_db_connection()
        row = con.execute("""
            SELECT * FROM realtime_quote WHERE code=? LIMIT 1
        """, [code]).fetchone()
        cols = [x[0] for x in con.description]
        con.close()
        if row:
            d = dict(zip(cols, row))
            price = _to_float(d.get("price") or d.get("close") or d.get("current"), None)
            if price and price > 0:
                return {
                    "price": price,
                    "date": str(d.get("date") or _today_str()),
                    "time": str(d.get("time") or d.get("trade_time") or ""),
                    "source": "realtime_quote",
                }
    except Exception:
        pass

    try:
        con = _get_db_connection()
        row = con.execute("""
            SELECT close, date FROM stock_daily WHERE code=? ORDER BY date DESC LIMIT 1
        """, [code]).fetchone()
        con.close()
        if row:
            return {"price": _to_float(row[0]), "date": str(row[1]), "time": "", "source": "stock_daily"}
    except Exception:
        pass
    return {"price": None, "date": "", "time": "", "source": "none"}


def _load_daily_history(code: str, bars: int = 260) -> pl.DataFrame:
    try:
        con = _get_db_connection()
        rows = con.execute("""
            SELECT date, open, high, low, close, volume, amount
            FROM stock_daily
            WHERE code=?
            ORDER BY date DESC
            LIMIT ?
        """, [str(code).zfill(6), bars]).fetchall()
        cols = [x[0] for x in con.description]
        con.close()
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows, schema=cols, orient="row")
        return df.with_columns([
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.col("amount").cast(pl.Float64, strict=False),
        ]).sort("date")
    except Exception:
        return pl.DataFrame()


def load_paper_positions(open_only: bool = True) -> pl.DataFrame:
    df = _read_parquet(PAPER_POSITIONS_PATH)
    if df.is_empty():
        return df
    if open_only and "status" in df.columns:
        return df.filter(pl.col("status") == "open")
    return df


def load_paper_journal() -> pl.DataFrame:
    return _read_parquet(PAPER_JOURNAL_PATH)


def _already_open(code: str) -> bool:
    code = str(code).zfill(6)
    df = load_paper_positions(open_only=True)
    if not df.is_empty() and "code" in df.columns:
        if not df.filter(pl.col("code").cast(pl.Utf8).str.zfill(6) == code).is_empty():
            return True
    # 真实持仓表也检查，避免同一票重复提醒
    try:
        real_path = "data/positions.parquet"
        real = _read_parquet(real_path)
        if not real.is_empty() and "code" in real.columns and "status" in real.columns:
            hit = real.filter((pl.col("code").cast(pl.Utf8).str.zfill(6) == code) & (pl.col("status") == "open"))
            if not hit.is_empty():
                return True
    except Exception:
        pass
    return False


def _in_cooldown(code: str, today: Optional[date] = None) -> Tuple[bool, str]:
    today = today or datetime.now().date()
    j = load_paper_journal()
    if j.is_empty() or "code" not in j.columns or "sell_date" not in j.columns:
        return False, ""
    code = str(code).zfill(6)
    hit = j.filter(pl.col("code").cast(pl.Utf8).str.zfill(6) == code)
    if hit.is_empty():
        return False, ""
    # 最近一次退出
    rows = hit.sort("sell_date", descending=True).head(1).to_dicts()
    if not rows:
        return False, ""
    r = rows[0]
    sell_date = _parse_date(r.get("sell_date"))
    if not sell_date:
        return False, ""
    reason = str(r.get("exit_reason") or "")
    days = 5 if "stop" in reason else 3
    until = sell_date + timedelta(days=days)
    if today <= until:
        return True, until.strftime("%Y-%m-%d")
    return False, ""


def classify_buy_trigger(item: Dict[str, Any], market_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    market_state = market_state or {}
    state = str(market_state.get("state") or "")
    code = str(item.get("code", "")).zfill(6)
    name = str(item.get("name") or _lookup_name(code) or "")
    warnings = _warnings_list(item)

    total = _to_float(_nested_get(item, "total_score", _nested_get(item, "score", 0)), 0) or 0
    trend = _to_float(_nested_get(item, "trend_score", 0), 0) or 0
    pullback = _to_float(_nested_get(item, "pullback_score", 0), 0) or 0
    stabilize = _to_float(_nested_get(item, "stabilize_score", 0), 0) or 0
    confirm = _to_float(_nested_get(item, "confirm_score", 0), 0) or 0
    risk = _to_float(_nested_get(item, "risk_pct", None), None)
    trigger = _to_float(_nested_get(item, "trigger_price", None), None)
    entry = _to_float(_nested_get(item, "entry_price", None), None)
    stop_loss = _to_float(_nested_get(item, "stop_loss", None), None)
    tp1 = _to_float(_nested_get(item, "take_profit_1", None), None)
    tp2 = _to_float(_nested_get(item, "take_profit_2", None), None)

    quote = _load_latest_quote(code)
    current = _to_float(quote.get("price"), None) or entry
    if current and (entry is None or entry <= 0):
        entry = current

    reasons = []
    blocked = False

    if state in {"弱势", "risk_off"}:
        blocked = True; reasons.append(f"market_block:{state}")
    if not code:
        blocked = True; reasons.append("no_code")
    if risk is None or risk > 8:
        blocked = True; reasons.append(f"risk>{risk}")
    if trigger is None or current is None or current < trigger:
        blocked = True; reasons.append("not_breakout")
    if trigger and current and current > trigger * 1.03:
        blocked = True; reasons.append("too_far_from_trigger")
    hard = sorted(set(warnings) & HARD_BLOCK_WARNINGS)
    if hard:
        blocked = True; reasons.append("hard_warning:" + ",".join(hard))
    if total < 80:
        blocked = True; reasons.append("score_lt_80")
    if trend < 20 or pullback < 18 or stabilize < 18 or confirm < 18:
        blocked = True; reasons.append("score_component_not_balanced")
    if _already_open(code):
        blocked = True; reasons.append("already_open")
    cd, until = _in_cooldown(code)
    if cd:
        blocked = True; reasons.append(f"cooldown_until:{until}")

    near = False
    if (not blocked) is False:
        near = (
            state not in {"弱势", "risk_off"}
            and total >= 75
            and risk is not None and risk <= 8
            and trigger is not None and current is not None
            and current >= trigger * 0.985
            and current < trigger
            and "volume_not_confirm" not in warnings
            and "too_hot_today" not in warnings
            and not _already_open(code)
        )

    level = "NONE"
    if not blocked:
        level = "BUY_TRIGGERED"
        if total >= 90 and risk is not None and risk <= 5 and trigger and current <= trigger * 1.02:
            level = "STRONG_BUY_TRIGGERED"
    elif near:
        level = "NEAR_TRIGGER"

    return {
        "level": level,
        "blocked": blocked,
        "reasons": reasons,
        "code": code,
        "name": name,
        "current_price": current,
        "entry_price": entry,
        "trigger_price": trigger,
        "stop_loss": stop_loss,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "risk_pct": risk,
        "total_score": total,
        "trend_score": trend,
        "pullback_score": pullback,
        "stabilize_score": stabilize,
        "confirm_score": confirm,
        "warnings": warnings,
        "quote_date": quote.get("date"),
        "quote_time": quote.get("time"),
        "quote_source": quote.get("source"),
        "market_state": state,
        "raw_item": item,
    }


def _calc_position_pct(level: str, risk: Optional[float], market_state: str) -> float:
    if level == "STRONG_BUY_TRIGGERED":
        pct = 0.25
    elif level == "BUY_TRIGGERED":
        pct = 0.15
    else:
        pct = 0.0
    if market_state == "震荡":
        pct *= 0.5
    if risk is not None and risk > 5:
        pct *= 0.7
    return round(max(0.0, min(pct, 0.30)), 4)


def _send_paper_notify(title: str, content: str):
    if send_markdown is None:
        print(content)
        return
    try:
        send_markdown(title=title, content=content, category="PAPER_TRADE")
    except TypeError:
        try:
            send_markdown(content)
        except Exception as e:
            print(f"纸面交易通知失败：{e}")
    except Exception as e:
        print(f"纸面交易通知失败：{e}")


def _paper_position_row(sig: Dict[str, Any]) -> Dict[str, Any]:
    code = sig["code"]
    current = _to_float(sig.get("current_price"), None)
    trigger = _to_float(sig.get("trigger_price"), current)
    buy_price = current or trigger
    if buy_price is None:
        buy_price = 0.0
    buy_price = round(buy_price * (1 + BUY_SLIPPAGE_PCT), 3)

    stop_loss = _to_float(sig.get("stop_loss"), None)
    if stop_loss is None:
        stop_loss = round(buy_price * 0.92, 3)
    risk_abs = max(buy_price - stop_loss, 0.01)
    tp1 = _to_float(sig.get("take_profit_1"), None) or round(buy_price + risk_abs * 1.5, 3)
    tp2 = _to_float(sig.get("take_profit_2"), None) or round(buy_price + risk_abs * 2.5, 3)

    position_pct = _calc_position_pct(sig.get("level", ""), sig.get("risk_pct"), sig.get("market_state", ""))
    amount = PAPER_ACCOUNT_SIZE * position_pct
    shares = max(DEFAULT_SHARES, int(amount // buy_price // 100) * 100) if buy_price > 0 and position_pct > 0 else DEFAULT_SHARES

    today = datetime.now().date()
    cost_amount = round(buy_price * shares * (1 + COMMISSION_PCT), 2)

    return {
        "paper_id": str(uuid.uuid4())[:10],
        "strategy_name": STRATEGY_NAME,
        "code": code,
        "name": sig.get("name", ""),
        "status": "open",
        "source": "buy_trigger",
        "source_signal_date": _today_str(),
        "buy_signal_time": _now_str(),
        "buy_date": _today_str(),
        "buy_time": datetime.now().strftime("%H:%M:%S"),
        "buy_price": buy_price,
        "shares": shares,
        "cost_amount": cost_amount,
        "trigger_price": sig.get("trigger_price"),
        "stop_loss": float(stop_loss),
        "take_profit_1": float(tp1),
        "take_profit_2": float(tp2),
        "risk_pct": _to_float(sig.get("risk_pct"), None),
        "position_pct": position_pct,
        "can_sell_date": _next_trading_day(today),
        "t_plus_1_locked": True,
        "target1_hit": False,
        "target2_hit": False,
        "highest_price_after_buy": buy_price,
        "lowest_price_after_buy": buy_price,
        "max_profit_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "hold_days": 0,
        "entry_reason": sig.get("level"),
        "warnings": _to_str(sig.get("warnings", [])),
        "market_state_at_buy": sig.get("market_state", ""),
        "signal_score": sig.get("total_score"),
        "trend_score": sig.get("trend_score"),
        "pullback_score": sig.get("pullback_score"),
        "stabilize_score": sig.get("stabilize_score"),
        "confirm_score": sig.get("confirm_score"),
        "quote_date": sig.get("quote_date"),
        "quote_time": sig.get("quote_time"),
        "created_at": _now_str(),
        "updated_at": _now_str(),
    }


def create_paper_position(sig: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    _ensure_dirs()
    if sig.get("level") not in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}:
        return None
    if _already_open(sig["code"]):
        return None

    open_df = load_paper_positions(open_only=True)
    if not open_df.is_empty() and len(open_df) >= MAX_OPEN_POSITIONS:
        return None

    row = _paper_position_row(sig)
    old = load_paper_positions(open_only=False)
    new = pl.DataFrame([row])
    out = new if old.is_empty() else pl.concat([old, new], how="diagonal_relaxed")
    _write_parquet(out, PAPER_POSITIONS_PATH)

    title = f"【{PROJECT_NAME}｜纸面买入触发】"
    content = (
        f"{title}\n"
        f"> {row['code']} {row['name']}\n"
        f"> 级别：{sig.get('level')}\n"
        f"> 纸面买入价：{row['buy_price']}\n"
        f"> 触发价：{row.get('trigger_price')}\n"
        f"> 止损价：{row.get('stop_loss')}\n"
        f"> 目标1：{row.get('take_profit_1')}\n"
        f"> 目标2：{row.get('take_profit_2')}\n"
        f"> 风险：{row.get('risk_pct')}%\n"
        f"> 纸面仓位：{row.get('position_pct') * 100:.1f}%\n"
        f"> T+1可卖日：{row.get('can_sell_date')}\n"
        f"> 数据：{sig.get('quote_source')} {sig.get('quote_date')} {sig.get('quote_time')}\n"
        f"> 说明：仅纸面交易记录，不代表自动下单。"
    )
    _send_paper_notify(title, content)
    return row


def process_scan_results_for_paper(results: List[Dict[str, Any]], market_state: Optional[Dict[str, Any]] = None, mode: str = "observe") -> List[Dict[str, Any]]:
    """
    扫描结果进入纸面交易触发器。
    observe/watchlist_refresh/tail_confirm 都可触发，after_close 不触发建仓，只做计划。
    """
    if mode not in {"observe", "watchlist_refresh", "tail_confirm"}:
        return []
    triggered = []
    near = []
    for item in results or []:
        sig = classify_buy_trigger(item, market_state=market_state)
        if sig["level"] in {"BUY_TRIGGERED", "STRONG_BUY_TRIGGERED"}:
            row = create_paper_position(sig)
            if row:
                triggered.append(row)
        elif sig["level"] == "NEAR_TRIGGER":
            near.append(sig)

    # 近触发只做轻提醒，避免刷屏：每轮最多5只
    if near:
        lines = [f"【{PROJECT_NAME}｜接近买入触发】", f"本轮接近触发：{len(near)} 只，展示前5只"]
        for s in near[:5]:
            lines.append(f"- {s['code']} {s['name']} | 现价:{s.get('current_price')} 触发:{s.get('trigger_price')} 风险:{s.get('risk_pct')}% 分数:{s.get('total_score')}")
        _send_paper_notify(f"【{PROJECT_NAME}｜接近买入触发】", "\n".join(lines))
    return triggered


def _close_paper_position(pos: Dict[str, Any], sell_price: float, exit_reason: str, exit_note: str = "") -> Dict[str, Any]:
    sell_price = round(sell_price * (1 - SELL_SLIPPAGE_PCT), 3)
    buy_price = _to_float(pos.get("buy_price"), 0) or 0
    shares = _to_int(pos.get("shares"), 0)
    gross = (sell_price - buy_price) * shares
    fees = buy_price * shares * COMMISSION_PCT + sell_price * shares * (COMMISSION_PCT + STAMP_TAX_PCT)
    pnl_amount = round(gross - fees, 2)
    return_pct = round((sell_price / buy_price - 1) * 100 if buy_price else 0, 2)
    risk_abs = max((buy_price - (_to_float(pos.get("stop_loss"), buy_price) or buy_price)), 0.01)
    r_multiple = round((sell_price - buy_price) / risk_abs, 2) if risk_abs else None
    hold_days = _holding_days(pos.get("buy_date"))
    row = dict(pos)
    row.update({
        "status": "closed",
        "sell_date": _today_str(),
        "sell_time": datetime.now().strftime("%H:%M:%S"),
        "sell_price": sell_price,
        "exit_reason": exit_reason,
        "exit_note": exit_note,
        "hold_days": hold_days,
        "return_pct": return_pct,
        "pnl_amount": pnl_amount,
        "fees": round(fees, 2),
        "r_multiple": r_multiple,
        "closed_at": _now_str(),
        "updated_at": _now_str(),
    })
    return row


def _holding_days(buy_date: Any) -> int:
    d = _parse_date(buy_date)
    if not d:
        return 0
    return max(0, (datetime.now().date() - d).days)


def track_paper_positions(update: bool = True) -> pl.DataFrame:
    positions = load_paper_positions(open_only=True)
    if positions.is_empty():
        return pl.DataFrame()

    rows = []
    closed = []
    updated_positions = []
    today = datetime.now().date()

    for pos in positions.to_dicts():
        code = str(pos.get("code", "")).zfill(6)
        q = _load_latest_quote(code)
        current = _to_float(q.get("price"), None)
        buy_price = _to_float(pos.get("buy_price"), 0) or 0
        stop_loss = _to_float(pos.get("stop_loss"), 0) or 0
        tp1 = _to_float(pos.get("take_profit_1"), None)
        tp2 = _to_float(pos.get("take_profit_2"), None)
        can_sell_date = _parse_date(pos.get("can_sell_date"))
        can_sell = bool(can_sell_date and today >= can_sell_date)
        hold_days = _holding_days(pos.get("buy_date"))

        if current is None or current <= 0:
            action = "NO_DATA"; reason = "无法获取价格"; ret = None
            updated_positions.append(pos)
        else:
            ret = round((current / buy_price - 1) * 100 if buy_price else 0, 2)
            high = max(_to_float(pos.get("highest_price_after_buy"), buy_price) or buy_price, current)
            low = min(_to_float(pos.get("lowest_price_after_buy"), buy_price) or buy_price, current)
            pos["highest_price_after_buy"] = high
            pos["lowest_price_after_buy"] = low
            pos["max_profit_pct"] = round((high / buy_price - 1) * 100, 2) if buy_price else None
            pos["max_drawdown_pct"] = round((low / buy_price - 1) * 100, 2) if buy_price else None
            pos["hold_days"] = hold_days
            pos["updated_at"] = _now_str()
            pos["t_plus_1_locked"] = not can_sell

            action = "HOLD"; reason = "继续持有"
            if current <= stop_loss:
                if can_sell:
                    action = "EXIT_STOP"; reason = "跌破止损，纸面退出"
                    closed_row = _close_paper_position(pos, current, "stop_loss", reason)
                    closed.append(closed_row)
                else:
                    action = "T1_STOP_WARN"; reason = "T+1锁定中，已跌破止损，明日优先处理"
                    updated_positions.append(pos)
            elif tp2 and current >= tp2 and can_sell:
                action = "TAKE_PROFIT_2"; reason = "达到目标2，纸面止盈退出"
                closed.append(_close_paper_position(pos, current, "take_profit_2", reason))
            elif tp1 and current >= tp1 and not bool(pos.get("target1_hit", False)):
                action = "REDUCE_TARGET1"; reason = "达到目标1，标记目标1并将止损上移到成本附近"
                pos["target1_hit"] = True
                pos["target1_hit_at"] = _now_str()
                pos["stop_loss"] = max(stop_loss, buy_price)
                updated_positions.append(pos)
            elif hold_days >= 20 and ret < 8 and can_sell:
                action = "TIME_EXIT"; reason = "持仓20天收益不足8%，纸面时间止损退出"
                closed.append(_close_paper_position(pos, current, "time_exit", reason))
            elif hold_days >= 10 and ret < 3:
                action = "TIME_EXIT_WARN"; reason = "持仓10天收益不足3%，效率偏低"
                updated_positions.append(pos)
            else:
                updated_positions.append(pos)

        rows.append({
            "paper_id": pos.get("paper_id"),
            "code": code,
            "name": pos.get("name"),
            "buy_date": pos.get("buy_date"),
            "buy_price": buy_price,
            "current_price": current,
            "return_pct": ret,
            "hold_days": hold_days,
            "can_sell_date": pos.get("can_sell_date"),
            "can_sell": can_sell,
            "stop_loss": stop_loss,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "action": action,
            "reason": reason,
            "quote_date": q.get("date"),
            "quote_time": q.get("time"),
        })

    # 重新写 open positions
    if update:
        all_old = load_paper_positions(open_only=False)
        # 保留非open历史行
        keep = pl.DataFrame()
        if not all_old.is_empty() and "status" in all_old.columns:
            keep = all_old.filter(pl.col("status") != "open")
        open_new = pl.DataFrame(updated_positions) if updated_positions else pl.DataFrame()
        out = keep
        if not open_new.is_empty():
            out = open_new if out.is_empty() else pl.concat([out, open_new], how="diagonal_relaxed")
        _write_parquet(out, PAPER_POSITIONS_PATH)

        if closed:
            journal = load_paper_journal()
            closed_df = pl.DataFrame(closed)
            j_out = closed_df if journal.is_empty() else pl.concat([journal, closed_df], how="diagonal_relaxed")
            _write_parquet(j_out, PAPER_JOURNAL_PATH)
            for c in closed:
                title = f"【{PROJECT_NAME}｜纸面交易退出】"
                content = f"{title}\n> {c.get('code')} {c.get('name')}\n> 原因：{c.get('exit_reason')}\n> 买入：{c.get('buy_price')}\n> 卖出：{c.get('sell_price')}\n> 收益：{c.get('return_pct')}%\n> R倍数：{c.get('r_multiple')}R\n> 持仓：{c.get('hold_days')}天"
                _send_paper_notify(title, content)

    report = pl.DataFrame(rows)
    return report


def paper_summary_stats() -> Dict[str, Any]:
    j = load_paper_journal()
    if j.is_empty():
        return {"total": 0, "win_rate": None, "avg_win": None, "avg_loss": None, "payoff": None}
    if "return_pct" not in j.columns:
        return {"total": len(j), "win_rate": None, "avg_win": None, "avg_loss": None, "payoff": None}
    rets = j.with_columns(pl.col("return_pct").cast(pl.Float64, strict=False))
    total = len(rets)
    wins = rets.filter(pl.col("return_pct") > 0)
    losses = rets.filter(pl.col("return_pct") <= 0)
    win_rate = round(len(wins) / total * 100, 2) if total else None
    avg_win = round(float(wins["return_pct"].mean()), 2) if len(wins) else None
    avg_loss = round(float(losses["return_pct"].mean()), 2) if len(losses) else None
    payoff = round(abs(avg_win / avg_loss), 2) if avg_win is not None and avg_loss not in [None, 0] else None
    avg_hold = round(float(rets["hold_days"].mean()), 2) if "hold_days" in rets.columns else None
    return {"total": total, "wins": len(wins), "losses": len(losses), "win_rate": win_rate, "avg_win": avg_win, "avg_loss": avg_loss, "payoff": payoff, "avg_hold_days": avg_hold}


def generate_paper_report() -> str:
    _ensure_dirs()
    report = track_paper_positions(update=True)
    stats = paper_summary_stats()
    lines = [f"# 纸面交易报告 - {_today_str()}", ""]
    if report.is_empty():
        lines.append("当前没有 open 纸面持仓。")
    else:
        lines.append(f"当前 open 纸面持仓：{len(report)} 只")
        for r in report.to_dicts():
            lines.append(f"- {r.get('code')} {r.get('name')} | 动作:{r.get('action')} | 原因:{r.get('reason')} | 现价:{r.get('current_price')} | 收益:{r.get('return_pct')}% | 持仓:{r.get('hold_days')}天 | T+1可卖:{r.get('can_sell')}")
    lines.append("")
    lines.append("## 纸面交易统计")
    lines.append(f"- 总交易：{stats.get('total')}")
    lines.append(f"- 胜率：{stats.get('win_rate')}")
    lines.append(f"- 平均盈利：{stats.get('avg_win')}")
    lines.append(f"- 平均亏损：{stats.get('avg_loss')}")
    lines.append(f"- 盈亏比：{stats.get('payoff')}")
    lines.append(f"- 平均持仓天数：{stats.get('avg_hold_days')}")
    content = "\n".join(lines)
    path = os.path.join(PAPER_REPORT_DIR, f"paper_report_{_today_str()}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return content


def delete_paper_position(code: str) -> int:
    code = str(code).zfill(6)
    df = load_paper_positions(open_only=False)
    if df.is_empty():
        return 0
    before = len(df)
    df = df.filter(~((pl.col("code").cast(pl.Utf8).str.zfill(6) == code) & (pl.col("status") == "open")))
    _write_parquet(df, PAPER_POSITIONS_PATH)
    return before - len(df)


__all__ = [
    "process_scan_results_for_paper",
    "classify_buy_trigger",
    "create_paper_position",
    "track_paper_positions",
    "generate_paper_report",
    "paper_summary_stats",
    "load_paper_positions",
    "load_paper_journal",
    "delete_paper_position",
]
