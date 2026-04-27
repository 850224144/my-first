# core/position_tracker.py
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional
import os
import uuid

import polars as pl


POSITIONS_PATH = "data/positions.parquet"
TRADE_JOURNAL_PATH = "data/trade_journal.parquet"
REPORT_DIR = "../data/reports"


def _ensure_dirs():
    os.makedirs("../data", exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _safe_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


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


def _get_db_connection():
    from core.data import get_db_connection

    return get_db_connection()


def _lookup_name(code: str) -> str:
    try:
        con = _get_db_connection()
        row = con.execute(
            "SELECT name FROM stock_basic WHERE code = ? LIMIT 1",
            [code],
        ).fetchone()
        con.close()

        if row and row[0]:
            return str(row[0])

    except Exception:
        pass

    return ""


def _load_trade_plan_row(code: str) -> Dict[str, Any]:
    path = "../data/trade_plan.parquet"

    if not os.path.exists(path):
        return {}

    try:
        df = pl.read_parquet(path)
        if df.is_empty() or "code" not in df.columns:
            return {}

        hit = df.filter(pl.col("code").cast(pl.Utf8) == str(code)).head(1)

        if hit.is_empty():
            return {}

        return hit.to_dicts()[0]

    except Exception:
        return {}


def _load_latest_quote(code: str) -> Dict[str, Any]:
    """
    优先读取 realtime_quote；
    失败则读取 stock_daily 最新收盘。
    """
    code = str(code)

    # 1. realtime_quote
    try:
        con = _get_db_connection()

        row = con.execute(
            """
            SELECT price, date, time
            FROM realtime_quote
            WHERE code = ?
            LIMIT 1
            """,
            [code],
        ).fetchone()

        con.close()

        if row:
            price = _safe_float(row[0], None)
            if price and price > 0:
                return {
                    "price": price,
                    "date": str(row[1]) if row[1] is not None else _today_str(),
                    "time": str(row[2]) if row[2] is not None else "",
                    "source": "realtime_quote",
                }

    except Exception:
        pass

    # 2. stock_daily
    try:
        con = _get_db_connection()

        row = con.execute(
            """
            SELECT close, date
            FROM stock_daily
            WHERE code = ?
            ORDER BY date DESC
            LIMIT 1
            """,
            [code],
        ).fetchone()

        con.close()

        if row:
            price = _safe_float(row[0], None)
            if price and price > 0:
                return {
                    "price": price,
                    "date": str(row[1]),
                    "time": "",
                    "source": "stock_daily",
                }

    except Exception:
        pass

    return {
        "price": None,
        "date": "",
        "time": "",
        "source": "none",
    }


def _load_daily_history(code: str, bars: int = 80) -> pl.DataFrame:
    try:
        con = _get_db_connection()

        rows = con.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM stock_daily
            WHERE code = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            [str(code), bars],
        ).fetchall()

        cols = [x[0] for x in con.description]
        con.close()

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(rows, schema=cols, orient="row")

        df = df.with_columns(
            [
                pl.col("date").cast(pl.Date, strict=False),
                pl.col("open").cast(pl.Float64, strict=False),
                pl.col("high").cast(pl.Float64, strict=False),
                pl.col("low").cast(pl.Float64, strict=False),
                pl.col("close").cast(pl.Float64, strict=False),
                pl.col("volume").cast(pl.Float64, strict=False),
                pl.col("amount").cast(pl.Float64, strict=False),
            ]
        )

        return df.sort("date")

    except Exception:
        return pl.DataFrame()


def _parse_date(s: Any) -> Optional[date]:
    try:
        if isinstance(s, date):
            return s
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _holding_days(buy_date: Any) -> int:
    d = _parse_date(buy_date)
    if not d:
        return 0

    return max(0, (datetime.now().date() - d).days)


def _calc_history_stats(code: str, buy_date: Any, buy_price: float) -> Dict[str, Any]:
    """
    计算买入后的最大浮盈/最大回撤等。
    """
    df = _load_daily_history(code, bars=200)

    if df.is_empty():
        return {
            "ma10": None,
            "ma20": None,
            "max_profit_pct": None,
            "max_drawdown_pct": None,
            "trend_break": False,
        }

    if len(df) >= 20:
        df = df.with_columns(
            [
                pl.col("close").rolling_mean(10).alias("ma10"),
                pl.col("close").rolling_mean(20).alias("ma20"),
            ]
        )
    else:
        df = df.with_columns(
            [
                pl.lit(None).alias("ma10"),
                pl.lit(None).alias("ma20"),
            ]
        )

    last = df.tail(1).to_dicts()[0]
    ma10 = _safe_float(last.get("ma10"), None)
    ma20 = _safe_float(last.get("ma20"), None)
    close = _safe_float(last.get("close"), None)

    buy_d = _parse_date(buy_date)

    if buy_d:
        after = df.filter(pl.col("date") >= buy_d)
    else:
        after = df

    if after.is_empty() or not buy_price:
        max_profit_pct = None
        max_drawdown_pct = None
    else:
        high_max = _safe_float(after["high"].max(), None)
        low_min = _safe_float(after["low"].min(), None)

        max_profit_pct = (
            (high_max / buy_price - 1) * 100
            if high_max and buy_price
            else None
        )

        max_drawdown_pct = (
            (low_min / buy_price - 1) * 100
            if low_min and buy_price
            else None
        )

    trend_break = False
    if close and ma10:
        trend_break = close < ma10

    return {
        "ma10": ma10,
        "ma20": ma20,
        "max_profit_pct": max_profit_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "trend_break": trend_break,
    }


def load_positions(open_only: bool = True) -> pl.DataFrame:
    df = _read_parquet(POSITIONS_PATH)

    if df.is_empty():
        return df

    if open_only and "status" in df.columns:
        df = df.filter(pl.col("status") == "open")

    return df


def save_positions(df: pl.DataFrame):
    _write_parquet(df, POSITIONS_PATH)


def add_position(
    code: str,
    buy_price: float,
    shares: int,
    buy_date: Optional[str] = None,
    stop_loss: Optional[float] = None,
    take_profit_1: Optional[float] = None,
    take_profit_2: Optional[float] = None,
    name: str = "",
    note: str = "",
    force: bool = False,
) -> Dict[str, Any]:
    """
    手动录入持仓。
    后续可由券商接口自动同步，但当前先保守手动录入。
    """
    _ensure_dirs()

    code = str(code).zfill(6)
    name = name or _lookup_name(code)
    buy_date = buy_date or _today_str()

    buy_price = float(buy_price)
    shares = int(shares)

    if buy_price <= 0 or shares <= 0:
        raise ValueError("buy_price 和 shares 必须大于 0")

    old = load_positions(open_only=False)

    if not old.is_empty() and "code" in old.columns and "status" in old.columns:
        exists = old.filter(
            (pl.col("code").cast(pl.Utf8) == code)
            & (pl.col("status") == "open")
        )

        if not exists.is_empty() and not force:
            raise ValueError(f"{code} 已存在 open 持仓。如需覆盖，请加 --force")

    # 如果没有手动传止损/目标，优先从 trade_plan 继承
    plan = _load_trade_plan_row(code)

    if stop_loss is None:
        stop_loss = _safe_float(plan.get("stop_loss"), None)

    if take_profit_1 is None:
        take_profit_1 = _safe_float(plan.get("take_profit_1"), None)

    if take_profit_2 is None:
        take_profit_2 = _safe_float(plan.get("take_profit_2"), None)

    # 如果 trade_plan 也没有，就给一个保守默认值
    if stop_loss is None:
        stop_loss = round(buy_price * 0.92, 2)

    if take_profit_1 is None:
        risk_abs = buy_price - stop_loss
        take_profit_1 = round(buy_price + risk_abs * 1.5, 2)

    if take_profit_2 is None:
        risk_abs = buy_price - stop_loss
        take_profit_2 = round(buy_price + risk_abs * 2.5, 2)

    initial_risk_pct = (buy_price - stop_loss) / buy_price * 100

    row = {
        "position_id": str(uuid.uuid4())[:8],
        "code": code,
        "name": name,
        "status": "open",
        "buy_date": buy_date,
        "buy_price": buy_price,
        "shares": shares,
        "cost_amount": round(buy_price * shares, 2),
        "stop_loss": float(stop_loss),
        "take_profit_1": float(take_profit_1),
        "take_profit_2": float(take_profit_2),
        "initial_risk_pct": round(initial_risk_pct, 2),
        "target1_hit": False,
        "target2_hit": False,
        "target1_hit_at": "",
        "target2_hit_at": "",
        "created_at": _now_str(),
        "updated_at": _now_str(),
        "exit_date": "",
        "exit_price": None,
        "exit_reason": "",
        "pnl_pct": None,
        "pnl_amount": None,
        "note": note,
    }

    new_df = pl.DataFrame([row])

    if old.is_empty():
        out = new_df
    else:
        if force and "code" in old.columns and "status" in old.columns:
            old = old.filter(
                ~(
                    (pl.col("code").cast(pl.Utf8) == code)
                    & (pl.col("status") == "open")
                )
            )

        out = pl.concat([old, new_df], how="diagonal_relaxed")

    save_positions(out)

    return row


def _decide_position_action(pos: Dict[str, Any]) -> Dict[str, Any]:
    code = str(pos.get("code", "")).zfill(6)

    buy_price = _safe_float(pos.get("buy_price"), 0) or 0
    stop_loss = _safe_float(pos.get("stop_loss"), 0) or 0
    tp1 = _safe_float(pos.get("take_profit_1"), None)
    tp2 = _safe_float(pos.get("take_profit_2"), None)

    target1_hit = bool(pos.get("target1_hit", False))
    target2_hit = bool(pos.get("target2_hit", False))

    quote = _load_latest_quote(code)
    current_price = _safe_float(quote.get("price"), None)

    hold_days = _holding_days(pos.get("buy_date"))
    hist = _calc_history_stats(code, pos.get("buy_date"), buy_price)

    if current_price is None or current_price <= 0:
        return {
            "code": code,
            "name": pos.get("name", ""),
            "action": "NO_DATA",
            "reason": "无法获取当前价格",
            "current_price": None,
            "return_pct": None,
            "hold_days": hold_days,
            "source": quote.get("source"),
            **hist,
        }

    return_pct = (current_price / buy_price - 1) * 100 if buy_price else 0

    action = "HOLD"
    reason = "继续持有"
    priority = 10

    # 1. 硬止损
    if stop_loss and current_price <= stop_loss:
        action = "EXIT_STOP"
        reason = f"跌破止损价 {stop_loss}"
        priority = 100

    # 2. 目标2
    elif tp2 and current_price >= tp2:
        action = "TAKE_PROFIT_2"
        reason = f"达到目标2 {tp2}，建议继续止盈/清仓"
        priority = 90

    # 3. 目标1
    elif tp1 and current_price >= tp1 and not target1_hit:
        action = "REDUCE_TARGET1"
        reason = f"达到目标1 {tp1}，建议减仓并将止损上移到成本附近"
        priority = 80

    # 4. 趋势破坏
    elif hist.get("trend_break") and return_pct < 3:
        action = "TREND_BREAK_WARN"
        reason = "跌破 MA10 且收益不明显，注意趋势转弱"
        priority = 60

    # 5. 时间止损
    elif hold_days >= 20 and return_pct < 8:
        action = "TIME_EXIT_WARN"
        reason = "持仓超过20天且收益不足8%，考虑退出或降仓"
        priority = 55

    elif hold_days >= 10 and return_pct < 3:
        action = "TIME_WARN"
        reason = "持仓超过10天仍未有效脱离成本区"
        priority = 50

    # 6. 正常持有
    else:
        action = "HOLD"
        reason = "继续持有，按计划跟踪"
        priority = 10

    risk_to_stop_pct = (
        (current_price - stop_loss) / current_price * 100
        if current_price and stop_loss
        else None
    )

    return {
        "position_id": pos.get("position_id", ""),
        "code": code,
        "name": pos.get("name", ""),
        "buy_date": pos.get("buy_date", ""),
        "buy_price": buy_price,
        "shares": _safe_int(pos.get("shares"), 0),
        "stop_loss": stop_loss,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "current_price": round(current_price, 2),
        "quote_date": quote.get("date", ""),
        "quote_time": quote.get("time", ""),
        "source": quote.get("source", ""),
        "return_pct": round(return_pct, 2),
        "pnl_amount": round((current_price - buy_price) * _safe_int(pos.get("shares"), 0), 2),
        "hold_days": hold_days,
        "risk_to_stop_pct": round(risk_to_stop_pct, 2) if risk_to_stop_pct is not None else None,
        "action": action,
        "reason": reason,
        "priority": priority,
        "target1_hit": target1_hit,
        "target2_hit": target2_hit,
        "ma10": hist.get("ma10"),
        "ma20": hist.get("ma20"),
        "max_profit_pct": hist.get("max_profit_pct"),
        "max_drawdown_pct": hist.get("max_drawdown_pct"),
        "trend_break": hist.get("trend_break"),
    }


def track_positions(update: bool = True) -> pl.DataFrame:
    """
    跟踪 open 持仓。
    update=True 时，会自动标记 target1/target2 是否触发，但不会自动关闭持仓。
    """
    positions = load_positions(open_only=True)

    if positions.is_empty():
        return pl.DataFrame()

    rows = []
    pos_rows = positions.to_dicts()

    for pos in pos_rows:
        rows.append(_decide_position_action(pos))

    report = pl.DataFrame(rows)

    if update and not report.is_empty():
        all_pos = load_positions(open_only=False)

        if not all_pos.is_empty():
            updated = all_pos.to_dicts()
            today = _now_str()

            report_map = {
                str(x.get("position_id")): x
                for x in report.to_dicts()
            }

            for p in updated:
                pid = str(p.get("position_id"))
                if pid not in report_map:
                    continue

                r = report_map[pid]

                if r.get("action") == "REDUCE_TARGET1" and not bool(p.get("target1_hit", False)):
                    p["target1_hit"] = True
                    p["target1_hit_at"] = today
                    # 达到目标1后，建议把止损上移到成本价，先不自动改死，给提示更稳。
                    p["updated_at"] = today

                if r.get("action") == "TAKE_PROFIT_2" and not bool(p.get("target2_hit", False)):
                    p["target2_hit"] = True
                    p["target2_hit_at"] = today
                    p["updated_at"] = today

            save_positions(pl.DataFrame(updated))

    return report.sort(["priority", "return_pct"], descending=[True, True])


def close_position(
    code: str,
    exit_price: float,
    exit_reason: str = "manual_close",
    exit_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    手动关闭持仓，并写入 trade_journal。
    """
    _ensure_dirs()

    code = str(code).zfill(6)
    exit_date = exit_date or _today_str()
    exit_price = float(exit_price)

    all_pos = load_positions(open_only=False)

    if all_pos.is_empty():
        raise ValueError("当前没有持仓记录")

    rows = all_pos.to_dicts()

    target = None
    for p in rows:
        if str(p.get("code")).zfill(6) == code and p.get("status") == "open":
            target = p
            break

    if target is None:
        raise ValueError(f"未找到 {code} 的 open 持仓")

    buy_price = _safe_float(target.get("buy_price"), 0) or 0
    shares = _safe_int(target.get("shares"), 0)

    pnl_pct = (exit_price / buy_price - 1) * 100 if buy_price else 0
    pnl_amount = (exit_price - buy_price) * shares

    closed_row = dict(target)
    closed_row.update(
        {
            "status": "closed",
            "exit_date": exit_date,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl_pct": round(pnl_pct, 2),
            "pnl_amount": round(pnl_amount, 2),
            "updated_at": _now_str(),
        }
    )

    new_rows = []
    for p in rows:
        if p.get("position_id") == target.get("position_id"):
            new_rows.append(closed_row)
        else:
            new_rows.append(p)

    save_positions(pl.DataFrame(new_rows))

    journal = _read_parquet(TRADE_JOURNAL_PATH)
    journal_row = dict(closed_row)
    journal_row["journal_at"] = _now_str()

    if journal.is_empty():
        out = pl.DataFrame([journal_row])
    else:
        out = pl.concat([journal, pl.DataFrame([journal_row])], how="diagonal_relaxed")

    _write_parquet(out, TRADE_JOURNAL_PATH)

    return closed_row


def generate_position_report() -> str:
    """
    生成持仓跟踪报告。
    """
    _ensure_dirs()

    report = track_positions(update=True)

    lines: List[str] = []
    lines.append(f"# 持仓跟踪报告 - {_today_str()}")
    lines.append("")

    if report.is_empty():
        lines.append("当前没有 open 持仓。")
    else:
        lines.append(f"当前 open 持仓：{len(report)} 只")
        lines.append("")

        for row in report.to_dicts():
            lines.append(
                f"- {row.get('code')} {row.get('name', '')} | "
                f"动作:{row.get('action')} | 原因:{row.get('reason')} | "
                f"现价:{row.get('current_price')} | "
                f"收益:{row.get('return_pct')}% | "
                f"持仓:{row.get('hold_days')}天 | "
                f"止损:{row.get('stop_loss')} | "
                f"目标1:{row.get('take_profit_1')} | "
                f"目标2:{row.get('take_profit_2')}"
            )

    path = os.path.join(REPORT_DIR, f"position_report_{_today_str()}.md")

    content = "\n".join(lines)

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return content


__all__ = [
    "add_position",
    "load_positions",
    "track_positions",
    "close_position",
    "generate_position_report",
]