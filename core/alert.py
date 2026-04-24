# -*- coding: utf-8 -*-
"""
core/alert.py

分级推送：
- 观察信号
- 尾盘确认信号
- 收盘确认信号
- 风险/空仓信号
"""

from __future__ import annotations

import json
from typing import List, Dict, Any, Optional

import requests


def send_wechat(content: str, webhook_url: Optional[str]) -> bool:
    if not webhook_url:
        return False
    try:
        data = {"msgtype": "text", "text": {"content": content}}
        r = requests.post(webhook_url, data=json.dumps(data, ensure_ascii=False), headers={"Content-Type": "application/json"}, timeout=8)
        result = r.json()
        if result.get("errcode") == 0:
            return True
        print(f"⚠️ 企业微信推送失败: {result}")
        return False
    except Exception as e:
        print(f"⚠️ 企业微信推送异常: {e}")
        return False


def send_dingtalk(content: str, webhook_url: Optional[str]) -> bool:
    if not webhook_url:
        return False
    try:
        data = {"msgtype": "text", "text": {"content": content}}
        r = requests.post(webhook_url, data=json.dumps(data, ensure_ascii=False), headers={"Content-Type": "application/json"}, timeout=8)
        return r.status_code == 200
    except Exception as e:
        print(f"⚠️ 钉钉推送异常: {e}")
        return False


def _signal_title(scan_mode: str) -> str:
    if scan_mode == "tail_confirm":
        return "【尾盘确认信号】"
    if scan_mode == "after_close":
        return "【收盘确认/复盘信号】"
    return "【盘中观察信号】"


def build_content(
    results: List[Dict[str, Any]],
    market: Optional[Dict[str, Any]] = None,
    top_n: int = 10,
    scan_mode: str = "observe",
) -> str:
    title = _signal_title(scan_mode)
    lines = [title]

    if market:
        lines.append(f"大盘：{market.get('state_cn', '-') } | {market.get('reason', '')}")

    if not results:
        lines.append("今日无符合条件的股票。")
        if market and not market.get("allow_new_position", True):
            lines.append("当前状态不开新仓。")
        return "\n".join(lines)

    lines.append(f"共 {len(results)} 只，展示前 {min(top_n, len(results))} 只：")
    lines.append("-" * 40)

    for i, r in enumerate(results[:top_n], 1):
        scores = r.get("scores", {})
        plan = r.get("plan", {})
        reasons = r.get("reasons", [])
        model_prob = r.get("model_prob", None)
        model_text = f" | 模型排序概率:{model_prob:.2f}" if isinstance(model_prob, (int, float)) and model_prob > 0 else ""

        lines.extend([
            f"{i}. {r.get('name', '')} {r.get('code', '')} | {r.get('industry', '未知')}",
            f"   信号:{r.get('signal')} | 总分:{r.get('score')} | 趋势:{scores.get('trend', 0)} 回调:{scores.get('pullback', 0)} 企稳:{scores.get('stable', 0)} 确认:{scores.get('confirm', 0)}{model_text}",
            f"   买入:{plan.get('buy', '-')} | 止损:{plan.get('stop', '-')} | 仓位:{plan.get('position', '-')} | 目标1:{plan.get('target1', '-')}",
            f"   备注:{plan.get('note', '')}",
        ])
        if reasons:
            lines.append(f"   提醒:{'；'.join(reasons[:2])}")
        lines.append("")

    return "\n".join(lines)


def push_results(
    results: List[Dict[str, Any]],
    webhook_url: Optional[str] = None,
    platform: str = "wechat",
    market: Optional[Dict[str, Any]] = None,
    scan_mode: str = "observe",
    top_n: int = 10,
) -> bool:
    content = build_content(results, market=market, top_n=top_n, scan_mode=scan_mode)

    print("\n" + "=" * 70)
    print("📤 推送内容")
    print("=" * 70)
    print(content)
    print("=" * 70)

    if not webhook_url:
        return False

    if platform == "wechat":
        success = send_wechat(content, webhook_url)
    elif platform == "dingtalk":
        success = send_dingtalk(content, webhook_url)
    else:
        raise ValueError("platform 只能是 wechat 或 dingtalk")

    print("✅ 推送成功" if success else "❌ 推送失败")
    return success
