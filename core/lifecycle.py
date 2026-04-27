# core/lifecycle.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import os

import polars as pl


WATCHLIST_PATH = "data/watchlist.parquet"
TRADE_PLAN_PATH = "data/trade_plan.parquet"
REPORT_DIR = "data/reports"


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)


def _nested_get(item: Dict[str, Any], key: str, default=None):
    if not isinstance(item, dict):
        return default

    v = item.get(key)
    if v not in [None, ""]:
        return v

    score_detail = item.get("score_detail")
    if isinstance(score_detail, dict):
        v = score_detail.get(key)
        if v not in [None, ""]:
            return v

    plan = item.get("plan")
    if isinstance(plan, dict):
        v = plan.get(key)
        if v not in [None, ""]:
            return v

    return default


def _to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _to_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return ",".join([str(x) for x in v])
    if isinstance(v, dict):
        return str(v)
    return str(v)


def _read_parquet(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        return pl.DataFrame()

    try:
        return pl.read_parquet(path)
    except Exception:
        return pl.DataFrame()


def _status_from_result(item: Dict[str, Any], mode: str) -> str:
    action = str(_nested_get(item, "action", ""))
    signal = str(_nested_get(item, "signal", ""))
    risk = _to_float(_nested_get(item, "risk_pct", None), None)

    if action == "放弃":
        return "rejected"

    if risk is not None and risk > 8:
        return "high_risk"

    if mode == "tail_confirm":
        if action in {"可执行", "可轻仓执行", "轻仓观察", "观察为主"}:
            return "confirmed"
        if signal in {"confirm", "strong_observe"}:
            return "confirmed"

    if action in {"等待确认", "等待突破"}:
        return "waiting"

    return "active"


def _result_to_row(
    item: Dict[str, Any],
    mode: str,
    market_state: Optional[Dict[str, Any]] = None,
    first_seen_at: Optional[str] = None,
) -> Dict[str, Any]:
    code = str(item.get("code", ""))
    name = str(item.get("name") or "")

    status = _status_from_result(item, mode)

    return {
        "code": code,
        "name": name,
        "first_seen_at": first_seen_at or _now_str(),
        "last_seen_at": _now_str(),
        "date": _today_str(),
        "mode": mode,
        "market_state": (market_state or {}).get("state", ""),
        "signal": _to_str(_nested_get(item, "signal", "")),
        "total_score": _to_float(_nested_get(item, "total_score", _nested_get(item, "score", None)), None),
        "trend_score": _to_float(_nested_get(item, "trend_score", None), None),
        "pullback_score": _to_float(_nested_get(item, "pullback_score", None), None),
        "stabilize_score": _to_float(_nested_get(item, "stabilize_score", None), None),
        "confirm_score": _to_float(_nested_get(item, "confirm_score", None), None),
        "risk_pct": _to_float(_nested_get(item, "risk_pct", None), None),
        "risk_level": _to_str(_nested_get(item, "risk_level", "")),
        "action": _to_str(_nested_get(item, "action", "")),
        "entry_type": _to_str(_nested_get(item, "entry_type", "")),
        "entry_price": _to_float(_nested_get(item, "entry_price", None), None),
        "trigger_price": _to_float(_nested_get(item, "trigger_price", None), None),
        "stop_loss": _to_float(_nested_get(item, "stop_loss", None), None),
        "take_profit_1": _to_float(_nested_get(item, "take_profit_1", None), None),
        "take_profit_2": _to_float(_nested_get(item, "take_profit_2", None), None),
        "position_suggestion": _to_str(_nested_get(item, "position_suggestion", "")),
        "warnings": _to_str(_nested_get(item, "warnings", [])),
        "veto": bool(_nested_get(item, "veto", False)),
        "veto_reasons": _to_str(_nested_get(item, "veto_reasons", [])),
        "invalid_condition": _to_str(_nested_get(item, "invalid_condition", "")),
        "note": _to_str(_nested_get(item, "note", "")),
        "status": status,
    }


def save_watchlist(
    results: List[Dict[str, Any]],
    mode: str = "observe",
    market_state: Optional[Dict[str, Any]] = None,
) -> pl.DataFrame:
    """
    保存 / 更新观察池。
    observe：写入 active / waiting / high_risk
    tail_confirm：更新 confirmed / waiting / high_risk
    """
    _ensure_dirs()

    old = _read_parquet(WATCHLIST_PATH)

    old_first_seen = {}
    if not old.is_empty() and "code" in old.columns and "first_seen_at" in old.columns:
        for row in old.select(["code", "first_seen_at"]).iter_rows(named=True):
            old_first_seen[str(row["code"])] = row.get("first_seen_at")

    rows = []
    for item in results:
        code = str(item.get("code", ""))
        if not code:
            continue

        rows.append(
            _result_to_row(
                item=item,
                mode=mode,
                market_state=market_state,
                first_seen_at=old_first_seen.get(code),
            )
        )

    if not rows:
        return old

    new_df = pl.DataFrame(rows)

    if old.is_empty():
        out = new_df
    else:
        new_codes = set(new_df["code"].cast(pl.Utf8).to_list())
        old_keep = old.filter(~pl.col("code").cast(pl.Utf8).is_in(list(new_codes)))
        out = pl.concat([old_keep, new_df], how="diagonal_relaxed")

    out = out.sort(["status", "total_score"], descending=[False, True])
    out.write_parquet(WATCHLIST_PATH)

    return out


def load_watchlist(active_only: bool = True) -> pl.DataFrame:
    df = _read_parquet(WATCHLIST_PATH)

    if df.is_empty():
        return df

    if active_only and "status" in df.columns:
        df = df.filter(
            pl.col("status").is_in(["active", "waiting", "confirmed", "high_risk"])
        )

    return df


def load_watchlist_codes(active_only: bool = True) -> List[str]:
    df = load_watchlist(active_only=active_only)

    if df.is_empty() or "code" not in df.columns:
        return []

    return df["code"].cast(pl.Utf8).unique().to_list()


def save_trade_plan(
    results: List[Dict[str, Any]],
    mode: str = "after_close",
    market_state: Optional[Dict[str, Any]] = None,
) -> pl.DataFrame:
    """
    保存明日交易计划。
    只保存风险可接受、不是纯高风险观察的票。
    """
    _ensure_dirs()

    rows = []

    for item in results:
        code = str(item.get("code", ""))
        if not code:
            continue

        action = str(_nested_get(item, "action", ""))
        risk = _to_float(_nested_get(item, "risk_pct", None), None)

        # trade_plan 不保存风险过高或明确不建议开仓的票
        if risk is None or risk > 8:
            continue

        if action in {"只观察", "放弃"}:
            continue

        row = _result_to_row(
            item=item,
            mode=mode,
            market_state=market_state,
        )

        row["plan_date"] = _today_str()
        rows.append(row)

    if not rows:
        df = pl.DataFrame()
        df.write_parquet(TRADE_PLAN_PATH)
        return df

    df = pl.DataFrame(rows)
    df = df.sort(["risk_pct", "total_score"], descending=[False, True])
    df.write_parquet(TRADE_PLAN_PATH)

    return df


def load_trade_plan() -> pl.DataFrame:
    return _read_parquet(TRADE_PLAN_PATH)


def generate_daily_report() -> str:
    """
    生成日报 Markdown。
    """
    _ensure_dirs()

    watchlist = load_watchlist(active_only=False)
    trade_plan = load_trade_plan()

    lines: List[str] = []
    lines.append(f"# A股交易日报 - {_today_str()}")
    lines.append("")
    lines.append("## 一、观察池概况")

    if watchlist.is_empty():
        lines.append("今日暂无观察池记录。")
    else:
        total = len(watchlist)
        lines.append(f"- 观察池总数：{total}")

        if "status" in watchlist.columns:
            status_count = watchlist.group_by("status").len().sort("len", descending=True)
            lines.append("")
            lines.append("### 状态分布")
            for row in status_count.iter_rows(named=True):
                lines.append(f"- {row['status']}：{row['len']}")

        show_cols = [
            c for c in [
                "code", "name", "status", "action", "signal",
                "total_score", "risk_pct", "trigger_price", "stop_loss"
            ]
            if c in watchlist.columns
        ]

        lines.append("")
        lines.append("### 观察池 Top 10")
        top = watchlist.sort("total_score", descending=True).head(10)
        for row in top.select(show_cols).iter_rows(named=True):
            lines.append(
                f"- {row.get('code')} {row.get('name', '')} | "
                f"状态:{row.get('status', '')} | 操作:{row.get('action', '')} | "
                f"分数:{row.get('total_score', '')} | 风险:{row.get('risk_pct', '')}% | "
                f"触发:{row.get('trigger_price', '')} | 止损:{row.get('stop_loss', '')}"
            )

    lines.append("")
    lines.append("## 二、明日交易计划")

    if trade_plan.is_empty():
        lines.append("当前没有风险合适的明日交易计划。")
    else:
        lines.append(f"- 明日计划数量：{len(trade_plan)}")
        top = trade_plan.head(20)

        for row in top.iter_rows(named=True):
            lines.append(
                f"- {row.get('code')} {row.get('name', '')} | "
                f"操作:{row.get('action', '')} | "
                f"类型:{row.get('entry_type', '')} | "
                f"分数:{row.get('total_score', '')} | "
                f"风险:{row.get('risk_pct', '')}% | "
                f"触发:{row.get('trigger_price', '')} | "
                f"止损:{row.get('stop_loss', '')} | "
                f"仓位:{row.get('position_suggestion', '')}"
            )

    lines.append("")
    lines.append("## 三、执行提醒")
    lines.append("- observe 只代表观察候选，不代表买入。")
    lines.append("- tail_confirm 才接近交易确认。")
    lines.append("- 风险超过 8% 的候选不作为可执行计划。")
    lines.append("- 当天过热、量能未确认、未突破触发价，不追。")

    content = "\n".join(lines)

    path = os.path.join(REPORT_DIR, f"daily_report_{_today_str()}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

    return content


__all__ = [
    "save_watchlist",
    "load_watchlist",
    "load_watchlist_codes",
    "save_trade_plan",
    "load_trade_plan",
    "generate_daily_report",
]