# core/alert.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import os

try:
    import requests
except Exception:
    requests = None


# ============================================================
# 推送 / 打印模块
#
# 目标：
# 1. 兼容 run_scan.py 当前返回结构
# 2. 兼容 score_detail / plan 嵌套结构
# 3. 按“交易可执行性”排序，而不是只按总分排序
# 4. observe 模式展示为“观察候选”，避免误导成买入信号
# ============================================================


def _safe_get(d: Any, key: str, default=None):
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def _nested_get(item: Dict[str, Any], key: str, default=None):
    """
    兼容三种结构：
    1. item[key]
    2. item["score_detail"][key]
    3. item["plan"][key]
    """
    if isinstance(item, dict):
        v = item.get(key)
        if v not in [None, ""]:
            return v

    score_detail = item.get("score_detail") if isinstance(item, dict) else None
    if isinstance(score_detail, dict):
        v = score_detail.get(key)
        if v not in [None, ""]:
            return v

    plan = item.get("plan") if isinstance(item, dict) else None
    if isinstance(plan, dict):
        v = plan.get(key)
        if v not in [None, ""]:
            return v

    return default


def _to_float(v, default=0.0):
    try:
        if v is None or v == "-":
            return default
        return float(v)
    except Exception:
        return default


def _fmt(v, default="-"):
    if v is None:
        return default

    if v == "":
        return default

    if isinstance(v, float):
        return f"{v:.2f}"

    return str(v)


def _lookup_names(codes: List[str]) -> Dict[str, str]:
    """
    从 stock_basic 查询股票名称。
    """
    if not codes:
        return {}

    try:
        from core.data import get_db_connection

        con = get_db_connection()
        placeholders = ",".join(["?"] * len(codes))

        rows = con.execute(
            f"""
            SELECT code, name
            FROM stock_basic
            WHERE code IN ({placeholders})
            """,
            codes,
        ).fetchall()

        con.close()

        return {str(code): str(name) for code, name in rows}

    except Exception:
        return {}


def _format_market(market_state: Optional[Dict[str, Any]]) -> str:
    if not market_state:
        return ""

    state = market_state.get("state", "-")
    message = market_state.get("message") or ""

    if message:
        return f"大盘：{state} | {message}"

    return f"大盘：{state}"


def _rank_key(item: Dict[str, Any]) -> float:
    """
    推送排序逻辑：
    交易可执行性 > 风险 > 总分

    避免总分高但“风险过高 / 不建议开仓”的股票排在最前。
    """
    total_score = _to_float(
        _nested_get(item, "total_score", _nested_get(item, "score", 0)),
        0,
    )

    risk_pct = _to_float(_nested_get(item, "risk_pct", 99), 99)
    action = str(_nested_get(item, "action", ""))
    signal = str(_nested_get(item, "signal", ""))

    warnings = _nested_get(item, "warnings", [])
    if warnings is None:
        warnings = []
    if not isinstance(warnings, list):
        warnings = [str(warnings)]

    veto = bool(_nested_get(item, "veto", False))

    action_score_map = {
        "可执行": 600,
        "可轻仓执行": 500,
        "轻仓观察": 400,
        "观察为主": 300,
        "等待突破": 250,
        "等待确认": 200,
        "只观察": 100,
        "放弃": -100,
    }

    action_score = action_score_map.get(action, 100)

    signal_bonus = {
        "confirm": 80,
        "strong_observe": 50,
        "observe": 30,
        "weak_observe": 10,
        "ignore": 0,
    }.get(signal, 0)

    risk_penalty = 0
    if risk_pct > 8:
        risk_penalty += 180
    elif risk_pct > 5:
        risk_penalty += 80
    elif risk_pct > 3:
        risk_penalty += 20

    warn_penalty = 0

    if "too_hot_today" in warnings:
        warn_penalty += 80

    if "volume_not_confirm" in warnings:
        warn_penalty += 50

    if "volatility_not_contracting" in warnings:
        warn_penalty += 30

    if "lows_not_rising" in warnings:
        warn_penalty += 30

    if "stabilize_volume_high" in warnings:
        warn_penalty += 30

    if veto:
        warn_penalty += 300

    return action_score + signal_bonus + total_score - risk_penalty - warn_penalty


def _format_warning_text(warnings: Any, limit: int = 4) -> str:
    if not warnings:
        return ""

    if isinstance(warnings, list):
        return ", ".join([str(x) for x in warnings[:limit]])

    return str(warnings)


def _short_note(note: Any, limit: int = 4) -> str:
    if not note:
        return "仅作为规则系统生成的交易计划，不代表确定性买入建议。"

    if not isinstance(note, str):
        return str(note)

    parts = [x for x in note.split("；") if x]
    if not parts:
        return note

    return "；".join(parts[:limit])


def format_results(
    results: List[Dict[str, Any]],
    market_state: Optional[Dict[str, Any]] = None,
    mode: str = "observe",
    top_n: int = 10,
) -> str:
    """
    格式化推送内容。
    """
    title_map = {
        "observe": "盘中观察候选",
        "tail_confirm": "尾盘确认信号",
        "after_close": "收盘复盘信号",
    }

    title = title_map.get(mode, "交易信号")

    lines: List[str] = []
    lines.append(f"【{title}】")

    market_text = _format_market(market_state)
    if market_text:
        lines.append(market_text)

    if not results:
        lines.append("今日无符合条件的股票。")
        return "\n".join(lines)

    # 关键：按交易可执行性排序，而不是只按总分。
    results = sorted(results, key=_rank_key, reverse=True)

    codes = [str(x.get("code", "")) for x in results if x.get("code")]
    name_map = _lookup_names(codes)

    actionable_actions = {"可执行", "可轻仓执行", "轻仓观察", "观察为主"}
    actionable = [
        x
        for x in results
        if str(_nested_get(x, "action", "")) in actionable_actions
        and _to_float(_nested_get(x, "risk_pct", 99), 99) <= 8
    ]

    if actionable:
        lines.append(f"可重点跟踪：{len(actionable)} 只；总候选：{len(results)} 只")
    else:
        lines.append(f"当前无可执行买点，仅展示观察候选；总候选：{len(results)} 只")

    lines.append(f"展示前 {min(top_n, len(results))} 只：")
    lines.append("----------------------------------------")

    for i, item in enumerate(results[:top_n], start=1):
        code = str(item.get("code", ""))
        name = item.get("name") or name_map.get(code) or "未知"

        signal = _nested_get(item, "signal", "-")
        total_score = _nested_get(item, "total_score", _nested_get(item, "score", "-"))

        trend_score = _nested_get(item, "trend_score", 0)
        pullback_score = _nested_get(item, "pullback_score", 0)
        stabilize_score = _nested_get(item, "stabilize_score", 0)
        confirm_score = _nested_get(item, "confirm_score", 0)

        action = _nested_get(item, "action", "-")
        entry_type = _nested_get(item, "entry_type", "-")
        risk_level = _nested_get(item, "risk_level", "-")

        entry_price = _nested_get(item, "entry_price", "-")
        trigger_price = _nested_get(item, "trigger_price", "-")
        stop_loss = _nested_get(item, "stop_loss", "-")
        take_profit_1 = _nested_get(item, "take_profit_1", "-")
        take_profit_2 = _nested_get(item, "take_profit_2", "-")
        position = _nested_get(item, "position_suggestion", "-")
        risk_pct = _nested_get(item, "risk_pct", "-")

        warnings = _nested_get(item, "warnings", [])
        veto_reasons = _nested_get(item, "veto_reasons", [])
        note = _nested_get(
            item,
            "note",
            "仅作为规则系统生成的交易计划，不代表确定性买入建议。",
        )

        lines.append(f"{i}.  {code} | {name}")
        lines.append(
            f"   操作:{action} | 类型:{entry_type} | "
            f"风险:{_fmt(risk_pct)}%（{risk_level}）"
        )
        lines.append(
            f"   信号:{signal} | 总分:{total_score} | "
            f"趋势:{trend_score} 回调:{pullback_score} "
            f"企稳:{stabilize_score} 确认:{confirm_score}"
        )
        lines.append(
            f"   买入:{_fmt(entry_price)} | 触发:{_fmt(trigger_price)} | "
            f"止损:{_fmt(stop_loss)} | 仓位:{_fmt(position)}"
        )

        risk_num = _to_float(risk_pct, 99)

        if risk_num <= 8:
            lines.append(
                f"   目标1:{_fmt(take_profit_1)} | 目标2:{_fmt(take_profit_2)}"
            )
        else:
            lines.append("   目标:风险过高，暂不作为交易目标参考")

        warning_text = _format_warning_text(warnings, limit=4)
        if warning_text:
            lines.append(f"   提醒:{warning_text}")

        if veto_reasons:
            if isinstance(veto_reasons, list):
                lines.append(f"   否决原因:{', '.join([str(x) for x in veto_reasons[:4]])}")
            else:
                lines.append(f"   否决原因:{veto_reasons}")

        lines.append(f"   备注:{_short_note(note, limit=4)}")
        lines.append("")

    return "\n".join(lines).rstrip()


def _send_wechat(webhook: str, content: str):
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送")

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
        },
    }

    r = requests.post(webhook, json=payload, timeout=10)
    r.raise_for_status()
    return r.text


def _send_dingtalk(webhook: str, content: str):
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送")

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "A股扫描信号",
            "text": content,
        },
    }

    r = requests.post(webhook, json=payload, timeout=10)
    r.raise_for_status()
    return r.text


def push_results(
    results: List[Dict[str, Any]],
    market_state: Optional[Dict[str, Any]] = None,
    mode: str = "observe",
    webhook: str = "",
    platform: str = "wechat",
    top_n: int = 10,
):
    """
    推送扫描结果。
    没有 webhook 时只打印到控制台。
    """
    content = format_results(
        results=results,
        market_state=market_state,
        mode=mode,
        top_n=top_n,
    )

    print("\n" + "=" * 70)
    print("📤 推送内容")
    print("=" * 70)
    print(content)
    print("=" * 70)

    webhook = webhook or os.getenv("WECHAT_WEBHOOK", "")

    if not webhook:
        return content

    try:
        if platform == "dingtalk":
            return _send_dingtalk(webhook, content)

        return _send_wechat(webhook, content)

    except Exception as e:
        print(f"⚠️ 推送失败：{e}")
        return content


# 兼容可能存在的旧调用名
def send_alert(
    results: List[Dict[str, Any]],
    market_state: Optional[Dict[str, Any]] = None,
    mode: str = "observe",
    webhook: str = "",
    platform: str = "wechat",
):
    return push_results(
        results=results,
        market_state=market_state,
        mode=mode,
        webhook=webhook,
        platform=platform,
    )


__all__ = [
    "format_results",
    "push_results",
    "send_alert",
]