# core/alert.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import os

try:
    import requests
except Exception:
    requests = None


def _nested_get(item: Dict[str, Any], key: str, default=None):
    if isinstance(item, dict):
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


def _to_float(v, default=0.0):
    try:
        if v is None or v == "-":
            return default
        return float(v)
    except Exception:
        return default


def _fmt(v, default="-"):
    if v is None or v == "":
        return default
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def _lookup_names(codes: List[str]) -> Dict[str, str]:
    if not codes:
        return {}
    try:
        from core.data import get_db_connection
        con = get_db_connection()
        placeholders = ",".join(["?"] * len(codes))
        rows = con.execute(
            f"SELECT code, name FROM stock_basic WHERE code IN ({placeholders})",
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
    return f"大盘：{state} | {message}" if message else f"大盘：{state}"


def _format_quote_status(market_state: Optional[Dict[str, Any]]) -> str:
    if not market_state:
        return ""
    qs = market_state.get("quote_status") or market_state.get("realtime_status") or {}
    if not isinstance(qs, dict) or not qs:
        return ""
    requested = qs.get("requested", "-")
    success = qs.get("success", "-")
    rate = qs.get("success_rate")
    fresh = qs.get("fresh", "-")
    stale = qs.get("stale", "-")
    newest = qs.get("newest_quote_time") or "-"
    rate_text = f"{float(rate) * 100:.2f}%" if isinstance(rate, (float, int)) else "-"
    return f"行情快照：{newest} | 实时行情：{success}/{requested} 成功率:{rate_text} fresh:{fresh} stale:{stale}"


def _rank_key(item: Dict[str, Any]) -> float:
    total_score = _to_float(_nested_get(item, "total_score", _nested_get(item, "score", 0)), 0)
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
    signal_bonus = {"confirm": 80, "strong_observe": 50, "observe": 30, "weak_observe": 10}.get(signal, 0)

    risk_penalty = 0
    if risk_pct > 8:
        risk_penalty += 180
    elif risk_pct > 5:
        risk_penalty += 80
    elif risk_pct > 3:
        risk_penalty += 20

    warn_penalty = 0
    for key, penalty in [
        ("too_hot_today", 80),
        ("volume_not_confirm", 50),
        ("volatility_not_contracting", 30),
        ("lows_not_rising", 30),
        ("stabilize_volume_high", 30),
    ]:
        if key in warnings:
            warn_penalty += penalty
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
    return "；".join(parts[:limit]) if parts else note


def format_results(
    results: List[Dict[str, Any]],
    market_state: Optional[Dict[str, Any]] = None,
    mode: str = "observe",
    top_n: int = 10,
) -> str:
    title_map = {
        "observe": "盘中观察候选",
        "watchlist_refresh": "观察池刷新",
        "tail_confirm": "尾盘确认信号",
        "after_close": "收盘复盘信号",
    }
    title = title_map.get(mode, "交易信号")
    lines: List[str] = [f"【{title}】"]

    market_text = _format_market(market_state)
    if market_text:
        lines.append(market_text)
    quote_text = _format_quote_status(market_state)
    if quote_text:
        lines.append(quote_text)

    if not results:
        lines.append("今日无符合条件的股票。")
        return "\n".join(lines)

    results = sorted(results, key=_rank_key, reverse=True)
    codes = [str(x.get("code", "")) for x in results if x.get("code")]
    name_map = _lookup_names(codes)

    actionable_actions = {"可执行", "可轻仓执行", "轻仓观察", "观察为主"}
    actionable = [
        x for x in results
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
        note = _nested_get(item, "note", "仅作为规则系统生成的交易计划，不代表确定性买入建议。")

        lines.append(f"{i}.  {code} | {name}")
        lines.append(f"   操作:{action} | 类型:{entry_type} | 风险:{_fmt(risk_pct)}%（{risk_level}）")
        lines.append(f"   信号:{signal} | 总分:{total_score} | 趋势:{trend_score} 回调:{pullback_score} 企稳:{stabilize_score} 确认:{confirm_score}")
        lines.append(f"   买入:{_fmt(entry_price)} | 触发:{_fmt(trigger_price)} | 止损:{_fmt(stop_loss)} | 仓位:{_fmt(position)}")
        risk_num = _to_float(risk_pct, 99)
        if risk_num <= 8:
            lines.append(f"   目标1:{_fmt(take_profit_1)} | 目标2:{_fmt(take_profit_2)}")
        else:
            lines.append("   目标:风险过高，暂不作为交易目标参考")
        warning_text = _format_warning_text(warnings)
        if warning_text:
            lines.append(f"   提醒:{warning_text}")
        if veto_reasons:
            if isinstance(veto_reasons, list):
                lines.append(f"   否决原因:{', '.join([str(x) for x in veto_reasons[:4]])}")
            else:
                lines.append(f"   否决原因:{veto_reasons}")
        lines.append(f"   备注:{_short_note(note)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _send_dingtalk(webhook: str, content: str):
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送")
    payload = {"msgtype": "markdown", "markdown": {"title": "A股扫描信号", "text": content}}
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
    content = format_results(results=results, market_state=market_state, mode=mode, top_n=top_n)

    if platform == "dingtalk":
        print("\n" + "=" * 70)
        print("📤 钉钉通知 | TRADE_SCAN")
        print("=" * 70)
        print(content)
        print("=" * 70)
        webhook = webhook or os.getenv("WECHAT_WEBHOOK", "")
        if not webhook:
            return content
        try:
            return _send_dingtalk(webhook, content)
        except Exception as e:
            print(f"⚠️ 推送失败：{e}")
            return content

    try:
        from core.notify import send_markdown
        return send_markdown(content, webhook=webhook, category="TRADE_SCAN", title="", print_content=True)
    except Exception as e:
        print(f"⚠️ 企业微信统一推送失败，回退只打印：{e}")
        print("\n" + "=" * 70)
        print("📤 推送内容")
        print("=" * 70)
        print(content)
        print("=" * 70)
        return content


def send_alert(results, market_state=None, mode="observe", webhook="", platform="wechat"):
    return push_results(results, market_state=market_state, mode=mode, webhook=webhook, platform=platform)


__all__ = ["format_results", "push_results", "send_alert"]
