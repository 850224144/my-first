# core/notify.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
import os

try:
    import requests
except Exception:  # pragma: no cover
    requests = None


DEFAULT_WECHAT_WEBHOOK = os.getenv(
    "WECHAT_WEBHOOK",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b",
)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_webhook(webhook: str = "") -> str:
    return webhook or os.getenv("WECHAT_WEBHOOK", "") or DEFAULT_WECHAT_WEBHOOK


def _send_wechat_markdown(webhook: str, content: str, timeout: int = 10) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送企业微信")
    if not webhook:
        return {"ok": False, "reason": "webhook_empty"}

    payload = {
        "msgtype": "markdown",
        "markdown": {"content": content},
    }
    resp = requests.post(webhook, json=payload, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text}


def send_markdown(
    content: str,
    webhook: str = "",
    category: str = "INFO",
    title: str = "A股系统通知",
    print_content: bool = True,
) -> Dict[str, Any]:
    """
    企业微信统一推送入口。

    category 用于分级：
    - TRADE_SCAN：交易候选 / 观察池 / 尾盘确认 / 收盘复盘
    - POSITION：持仓风控
    - DAILY_REPORT：交易日报
    - SYSTEM_ERROR：系统异常 / 任务失败 / 任务超时
    - SYSTEM_INFO：系统信息
    """
    webhook = get_webhook(webhook)

    final_content = content
    if title and not content.startswith("【"):
        final_content = f"【{title}】\n{content}"

    if print_content:
        print("\n" + "=" * 70)
        print(f"📤 企业微信通知 | {category}")
        print("=" * 70)
        print(final_content)
        print("=" * 70)

    if not webhook:
        print("⚠️ WECHAT_WEBHOOK 为空，跳过企业微信推送")
        return {"ok": False, "reason": "webhook_empty"}

    try:
        return _send_wechat_markdown(webhook, final_content)
    except Exception as e:
        print(f"⚠️ 企业微信推送失败：{e}")
        return {"ok": False, "reason": str(e)}


def notify_system_event(
    title: str,
    message: str,
    level: str = "WARN",
    job_name: str = "",
    extra: Optional[Dict[str, Any]] = None,
    webhook: str = "",
) -> Dict[str, Any]:
    level_icon = {
        "INFO": "ℹ️",
        "WARN": "⚠️",
        "ERROR": "❌",
        "CRITICAL": "🚨",
        "SUCCESS": "✅",
    }.get(level.upper(), "⚠️")

    lines = [f"【系统异常通知】{level_icon} {title}"]
    lines.append(f"> 时间：{_now_str()}")
    if job_name:
        lines.append(f"> 任务：{job_name}")
    lines.append(f"> 级别：{level.upper()}")
    lines.append("")
    lines.append(message)

    if extra:
        lines.append("")
        lines.append("**附加信息**")
        for k, v in extra.items():
            lines.append(f"> {k}：{v}")

    return send_markdown(
        "\n".join(lines),
        webhook=webhook,
        category="SYSTEM_ERROR" if level.upper() in {"ERROR", "CRITICAL"} else "SYSTEM_INFO",
        title="",
    )


def notify_daily_report(content: str, webhook: str = "") -> Dict[str, Any]:
    """推送日报摘要。内容较长时截断，完整日报仍保存在 data/reports。"""
    max_len = 3600
    body = content
    if len(body) > max_len:
        body = body[:max_len] + "\n\n...\n日报内容较长，完整文件请查看 data/reports/"

    if not body.startswith("#"):
        body = f"# A股交易日报\n\n{body}"

    return send_markdown(
        body,
        webhook=webhook,
        category="DAILY_REPORT",
        title="",
    )


def notify_position_report(content: str, webhook: str = "") -> Dict[str, Any]:
    """推送持仓风控报告。"""
    max_len = 3600
    body = content
    if len(body) > max_len:
        body = body[:max_len] + "\n\n...\n持仓报告内容较长，完整文件请查看 data/reports/"

    if not body.startswith("#"):
        body = f"# 持仓跟踪报告\n\n{body}"

    return send_markdown(
        body,
        webhook=webhook,
        category="POSITION",
        title="",
    )


__all__ = [
    "DEFAULT_WECHAT_WEBHOOK",
    "get_webhook",
    "send_markdown",
    "notify_system_event",
    "notify_daily_report",
    "notify_position_report",
]
