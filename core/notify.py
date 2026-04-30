# core/notify.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import json
import os
import re
import time

try:
    import requests
except Exception:  # pragma: no cover
    requests = None


PROJECT_NOTIFY_NAME = os.getenv("PROJECT_NOTIFY_NAME", "A股二买交易助手")

DEFAULT_WECHAT_WEBHOOK = os.getenv(
    "WECHAT_WEBHOOK",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b",
)

# 企业微信 markdown.content 标称 4096，但实际要按 UTF-8 bytes 控制。
# 中文 1 个字通常 3 bytes；再加上标题、换行、JSON 传输，必须留足余量。
# 默认单页内容压到 2000 bytes 以内，远低于 4096，避免 40058。
WECHAT_MARKDOWN_SAFE_BYTES = int(os.getenv("WECHAT_MARKDOWN_SAFE_BYTES", "2000"))
WECHAT_TEXT_SAFE_BYTES = int(os.getenv("WECHAT_TEXT_SAFE_BYTES", "1800"))

# 多页推送延迟，禁止瞬间连发。
WECHAT_NOTIFY_DELAY_SECONDS = max(1.2, float(os.getenv("WECHAT_NOTIFY_DELAY_SECONDS", "1.8")))
# 跨进程限流：不同脚本连续发消息时也至少间隔这么久。
WECHAT_GLOBAL_MIN_INTERVAL = max(1.0, float(os.getenv("WECHAT_GLOBAL_MIN_INTERVAL", "1.3")))
NOTIFY_STATE_DIR = os.getenv("NOTIFY_STATE_DIR", "data/notify_state")
LAST_SEND_FILE = os.path.join(NOTIFY_STATE_DIR, "last_wechat_send.json")

CATEGORY_DISPLAY = {
    "TRADE_SCAN": "交易信号",
    "TRADE_SIGNAL": "交易信号",
    "WATCHLIST": "观察池刷新",
    "TAIL_CONFIRM": "尾盘确认",
    "AFTER_CLOSE": "收盘复盘",
    "BUY_TRIGGER": "买入触发",
    "STRONG_BUY_TRIGGER": "强买入触发",
    "NEAR_TRIGGER": "接近触发",
    "PAPER_TRADE": "纸面交易",
    "PAPER_BUY": "纸面买入",
    "PAPER_EXIT": "纸面退出",
    "POSITION": "持仓风控",
    "DAILY_REPORT": "交易日报",
    "SYSTEM_ERROR": "系统异常",
    "SYSTEM_INFO": "系统信息",
    "INFO": "系统信息",
}

# 企业微信展示时，把策略内部英文 key 补上中文解释。
# 这里做在通知统一入口，所以扫描报告、买入触发、纸面交易、持仓、日报、异常都生效。
WARNING_CN_MAP = {
    "no_breakout": "未突破触发价",
    "near_breakout": "接近突破",
    "volume_not_confirm": "量能未确认",
    "rise_without_volume": "上涨缺量",
    "kline_quality_weak": "K线质量偏弱",
    "today_pct_weak": "当日表现偏弱",
    "too_hot_today": "当日过热",
    "trend_too_hot": "趋势过热",
    "pullback_too_fast": "回调过快",
    "pullback_too_shallow": "回调过浅",
    "pullback_days_not_ideal": "回调天数不理想",
    "pullback_volume_too_high": "回调量能偏高",
    "volatility_not_contracting": "波动未明显收敛",
    "lows_not_rising": "低点抬高不足",
    "stabilize_volume_high": "企稳量能偏高",
    "ma60_slight_break": "轻微跌破MA60",
    "risk_too_high": "风险过高",
    "market_weak": "大盘弱势",
    "risk_off": "风险关闭",
    "data_stale": "行情过期",
    "realtime_failed": "实时行情失败",
    "realtime_low_success_rate": "实时行情成功率过低",
    "price_too_far_from_trigger": "价格远离触发价",
    "already_open": "已有持仓",
    "already_paper_open": "已有纸面持仓",
    "cooldown_skip": "冷却期跳过",
    "t_plus_1_locked": "T+1锁定不可卖",
    "trend_break": "趋势破坏",
    "trend_break_warn": "趋势破坏预警",
    "time_warn": "时间预警",
    "time_exit_warn": "时间止损预警",
    "time_exit": "时间止损",
    "target1_hit": "达到目标1",
    "target2_hit": "达到目标2",
    "take_profit_1": "目标1止盈",
    "take_profit_2": "目标2止盈",
    "exit_stop": "触发止损",
    "stop_loss": "止损",
    "strong_observe": "强观察",
    "observe": "观察",
    "confirm": "确认",
    "waiting": "等待确认",
    "confirmed": "已确认",
    "high_risk": "高风险",
    "rejected": "已剔除",
    "expired": "已过期",
}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _byte_len(text: str) -> int:
    return len(str(text or "").encode("utf-8"))


def _cut_utf8_bytes(text: str, max_bytes: int) -> str:
    """按 UTF-8 byte 安全截断，不截断半个中文。"""
    text = str(text or "")
    if _byte_len(text) <= max_bytes:
        return text
    out: List[str] = []
    used = 0
    for ch in text:
        b = _byte_len(ch)
        if used + b > max_bytes:
            break
        out.append(ch)
        used += b
    return "".join(out)


def get_webhook(webhook: str = "") -> str:
    return webhook or os.getenv("WECHAT_WEBHOOK", "") or DEFAULT_WECHAT_WEBHOOK


def _category_name(category: str) -> str:
    return CATEGORY_DISPLAY.get(str(category or "INFO").upper(), str(category or "通知"))


def _project_title(category: str = "INFO", title: str = "", page: int = 0, total: int = 0) -> str:
    title = (title or "").strip()
    if title.startswith("【") and title.endswith("】"):
        base = title
    else:
        cat = _category_name(category)
        if title:
            base = f"【{PROJECT_NOTIFY_NAME}｜{cat}｜{title}】"
        else:
            base = f"【{PROJECT_NOTIFY_NAME}｜{cat}】"

    if total and total > 1:
        suffix = f"｜第{page}/{total}页"
        if base.endswith("】"):
            return base[:-1] + suffix + "】"
        return base + suffix
    return base


def _strip_existing_project_prefix(content: str) -> str:
    if not content:
        return ""
    lines = str(content).splitlines()
    if lines and lines[0].startswith(f"【{PROJECT_NOTIFY_NAME}｜"):
        return "\n".join(lines[1:]).lstrip()
    return str(content)


def _translate_warning_keys(text: str) -> str:
    """把内部英文提醒 key 补充中文说明。"""
    text = str(text or "")
    if not text:
        return text

    # 长 key 先替换，避免局部误替换。
    for key in sorted(WARNING_CN_MAP.keys(), key=len, reverse=True):
        cn = WARNING_CN_MAP[key]
        # 不重复替换已经是 中文(key) 的内容。
        pattern = rf"(?<![A-Za-z0-9_\(]){re.escape(key)}(?![A-Za-z0-9_\)])"
        text = re.sub(pattern, f"{cn}({key})", text)
    return text


def _compress_common_phrases(text: str) -> str:
    """压缩企业微信里反复出现的长句，减少 byte 长度。"""
    replacements = {
        "操作建议：": "建议：",
        "入场类型：": "类型：",
        "风险等级：": "风险：",
        "结构止损距离过远，当前不适合直接开仓。": "止损远，不适合直接开仓。",
        "风险偏高，只适合小仓位观察。": "风险偏高，仅小仓观察。",
        "量能未确认，突破有效性不足。": "量能未确认。",
        "当日涨幅偏大，避免追高，等待回踩或次日确认。": "当日偏热，不追。",
        "波动未明显收敛，可能仍在震荡。": "波动未收敛。",
        "低点抬高不充分，止跌结构一般。": "低点抬高不足。",
        "仅作为规则系统生成的交易计划，不代表确定性买入建议。": "系统计划，仅供复盘。",
    }
    out = str(text or "")
    for a, b in replacements.items():
        out = out.replace(a, b)
    return out


def _simplify_markdown(content: str) -> str:
    """
    企业微信全项目通知统一处理：
    - 按 byte 安全拆分前，先简化 Markdown。
    - 不用代码块、不用表格；保留简单加粗和分行。
    - 英文提醒 key 自动补中文。
    """
    text = str(content or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = _strip_existing_project_prefix(text)
    text = _translate_warning_keys(text)
    text = _compress_common_phrases(text)

    out: List[str] = []
    in_code_block = False
    blank_count = 0

    for raw in text.split("\n"):
        line = raw.rstrip()
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            # 代码块内容按普通文本保留，但不保留 ```。
            line = line.replace("`", "")
            stripped = line.strip()
        else:
            line = line.replace("`", "")
            stripped = line.strip()

        # 删除表格分隔行：|---|---| 或 :---:
        if re.fullmatch(r"[\|\s:\-]+", stripped or ""):
            continue

        # Markdown 表格行转简单分隔。
        if stripped.count("|") >= 2:
            parts = [p.strip() for p in stripped.strip("|").split("|") if p.strip()]
            if parts:
                line = " / ".join(parts)
                stripped = line.strip()

        # 标题转加粗。
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            line = f"**{m.group(2).strip()}**"
            stripped = line.strip()

        # 引用 > 转普通行。
        if line.lstrip().startswith(">"):
            line = line.lstrip()[1:].strip()
            stripped = line.strip()

        # 删除过多列表缩进。
        line = re.sub(r"^\s{2,}[-*]\s+", "- ", line)

        if not stripped:
            blank_count += 1
            if blank_count > 1:
                continue
        else:
            blank_count = 0

        out.append(line)

    return "\n".join(out).strip()


def _split_line_by_bytes(line: str, max_bytes: int) -> List[str]:
    line = str(line or "")
    if _byte_len(line) <= max_bytes:
        return [line]
    pieces: List[str] = []
    current: List[str] = []
    used = 0
    for ch in line:
        b = _byte_len(ch)
        if current and used + b > max_bytes:
            pieces.append("".join(current))
            current = []
            used = 0
        current.append(ch)
        used += b
    if current:
        pieces.append("".join(current))
    return pieces


def _split_body_by_bytes(body: str, header_max_bytes: int, safe_bytes: int) -> List[str]:
    # 预留标题、换行、服务端处理余量。
    body_limit = max(500, safe_bytes - header_max_bytes - 120)
    chunks: List[str] = []
    current: List[str] = []
    current_bytes = 0

    for line in str(body or "").splitlines():
        # 单行也必须按 bytes 硬切。
        for piece in _split_line_by_bytes(line, body_limit):
            piece_bytes = _byte_len(piece) + 1  # 换行
            if current and current_bytes + piece_bytes > body_limit:
                chunks.append("\n".join(current).strip())
                current = []
                current_bytes = 0
            current.append(piece)
            current_bytes += piece_bytes

    if current:
        chunks.append("\n".join(current).strip())

    return [c for c in chunks if c]


def _build_pages(content: str, category: str = "INFO", title: str = "") -> List[str]:
    body = _simplify_markdown(content or "")
    sample_title = _project_title(category, title, page=99, total=99)
    chunks = _split_body_by_bytes(
        body,
        header_max_bytes=_byte_len(sample_title) + 2,
        safe_bytes=WECHAT_MARKDOWN_SAFE_BYTES,
    )
    if not chunks:
        chunks = ["无内容。"]

    # 第一次得到 total 后，重新按真实页码标题再校验一次。
    pages: List[str] = []
    total = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        header = _project_title(category, title, page=idx, total=total)
        max_body_bytes = max(300, WECHAT_MARKDOWN_SAFE_BYTES - _byte_len(header) - 2)

        if _byte_len(chunk) > max_body_bytes:
            sub_pieces = _split_line_by_bytes(chunk, max_body_bytes)
        else:
            sub_pieces = [chunk]

        for sub in sub_pieces:
            page = f"{header}\n{sub}".strip()
            if _byte_len(page) > WECHAT_MARKDOWN_SAFE_BYTES:
                # 最后保险：按 bytes 截断，绝不让单页超限。
                page = _cut_utf8_bytes(page, WECHAT_MARKDOWN_SAFE_BYTES)
            pages.append(page)

    # 如果二次切分导致页数变化，重新补正确页码。
    if len(pages) != total:
        rebuilt: List[str] = []
        total2 = len(pages)
        for i, old_page in enumerate(pages, start=1):
            body_lines = old_page.split("\n", 1)
            page_body = body_lines[1] if len(body_lines) > 1 else old_page
            header = _project_title(category, title, page=i, total=total2)
            max_body_bytes = max(300, WECHAT_MARKDOWN_SAFE_BYTES - _byte_len(header) - 2)
            page_body = _cut_utf8_bytes(page_body, max_body_bytes)
            rebuilt.append(f"{header}\n{page_body}".strip())
        pages = rebuilt

    return pages


def _ensure_state_dir() -> None:
    os.makedirs(NOTIFY_STATE_DIR, exist_ok=True)


def _wait_global_rate_limit() -> None:
    _ensure_state_dir()
    now = time.time()
    last = 0.0
    try:
        if os.path.exists(LAST_SEND_FILE):
            with open(LAST_SEND_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                last = float(data.get("ts") or 0.0)
    except Exception:
        last = 0.0

    wait = WECHAT_GLOBAL_MIN_INTERVAL - (now - last)
    if wait > 0:
        time.sleep(wait)


def _mark_global_sent() -> None:
    _ensure_state_dir()
    try:
        tmp = LAST_SEND_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "time": _now_str()}, f, ensure_ascii=False)
        os.replace(tmp, LAST_SEND_FILE)
    except Exception:
        pass


def _post_json_utf8(webhook: str, payload: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送企业微信")
    if not webhook:
        return {"ok": False, "reason": "webhook_empty"}

    # 不用 requests.post(json=payload)，避免 ensure_ascii=True 把中文转成 \uXXXX 导致请求体膨胀。
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}
    resp = requests.post(webhook, data=data, headers=headers, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text}


def _send_wechat_markdown(webhook: str, content: str, timeout: int = 10) -> Dict[str, Any]:
    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    result = _post_json_utf8(webhook, payload, timeout=timeout)
    if isinstance(result, dict):
        result["_bytes"] = _byte_len(content)
    return result


def _send_wechat_text(webhook: str, content: str, timeout: int = 10) -> Dict[str, Any]:
    # text 兜底同样按 bytes 截断。
    content = _cut_utf8_bytes(str(content or ""), WECHAT_TEXT_SAFE_BYTES)
    payload = {"msgtype": "text", "text": {"content": content}}
    result = _post_json_utf8(webhook, payload, timeout=timeout)
    if isinstance(result, dict):
        result["_bytes"] = _byte_len(content)
    return result


def _is_wechat_ok(result: Dict[str, Any]) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("errcode") == 0:
        return True
    if result.get("ok") is True:
        return True
    return False


def send_markdown(
    content: str,
    webhook: str = "",
    category: str = "INFO",
    title: str = "",
    print_content: bool = True,
    **_: Any,
) -> Dict[str, Any]:
    """
    全项目企业微信统一入口。

    适用：扫描报告、观察池刷新、尾盘确认、买入触发、纸面交易、持仓风控、日报、系统异常。

    关键保护：
    - 严格按 UTF-8 bytes 拆分，不按字符数。
    - 默认单页 <= 2000 bytes，远低于企业微信 4096 限制。
    - 简化 Markdown，不用表格/代码块。
    - 英文提醒 key 自动中文化。
    - 多页限流延迟。
    - Markdown 返回 40058 时，自动 text 兜底。
    """
    webhook = get_webhook(webhook)
    category = str(category or "INFO").upper()
    pages = _build_pages(content=content or "", category=category, title=title or "")

    if print_content:
        print("\n" + "=" * 70)
        print(f"📤 企业微信通知 | {PROJECT_NOTIFY_NAME} | {category} | pages={len(pages)}")
        print("=" * 70)
        for i, page in enumerate(pages, start=1):
            preview = page if _byte_len(page) <= 1800 else _cut_utf8_bytes(page, 1800) + "\n...（日志预览截断，实际按分页推送）"
            print(f"----- page {i}/{len(pages)} | chars={len(page)} | bytes={_byte_len(page)} -----")
            print(preview)
        print("=" * 70)

    if not webhook:
        print("⚠️ WECHAT_WEBHOOK 为空，跳过企业微信推送")
        return {"ok": False, "reason": "webhook_empty", "pages": len(pages)}

    results: List[Dict[str, Any]] = []
    for i, page in enumerate(pages, start=1):
        _wait_global_rate_limit()
        page_bytes = _byte_len(page)
        try:
            result = _send_wechat_markdown(webhook, page)
            if isinstance(result, dict) and result.get("errcode") not in (None, 0):
                print(
                    f"⚠️ 企业微信 markdown 返回异常 page={i}/{len(pages)} "
                    f"chars={len(page)} bytes={page_bytes}：{result}"
                )
                fallback = _send_wechat_text(webhook, page)
                result = {"markdown_result": result, "text_fallback_result": fallback, "_bytes": page_bytes}
            results.append(result)
            _mark_global_sent()
            print(f"✅ 企业微信返回 page={i}/{len(pages)} chars={len(page)} bytes={page_bytes}：{result}")
        except Exception as e:
            err = {"ok": False, "reason": str(e), "page": i, "chars": len(page), "bytes": page_bytes}
            results.append(err)
            print(f"⚠️ 企业微信推送失败 page={i}/{len(pages)} chars={len(page)} bytes={page_bytes}：{e}")

        if i < len(pages):
            time.sleep(WECHAT_NOTIFY_DELAY_SECONDS)

    ok = True
    for r in results:
        if isinstance(r, dict) and "text_fallback_result" in r:
            ok = ok and _is_wechat_ok(r.get("text_fallback_result", {}))
        else:
            ok = ok and _is_wechat_ok(r)
    return {"ok": ok, "pages": len(pages), "results": results}


def send_text(content: str, webhook: str = "", category: str = "INFO", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content, webhook=webhook, category=category, title=title, **kwargs)


def notify_system_event(
    title: str,
    message: str,
    level: str = "WARN",
    job_name: str = "",
    extra: Optional[Dict[str, Any]] = None,
    webhook: str = "",
    **kwargs: Any,
) -> Dict[str, Any]:
    level_icon = {
        "INFO": "ℹ️",
        "WARN": "⚠️",
        "ERROR": "❌",
        "CRITICAL": "🚨",
        "SUCCESS": "✅",
    }.get(str(level).upper(), "⚠️")

    lines = [f"**{level_icon} {title}**"]
    lines.append(f"时间：{_now_str()}")
    if job_name:
        lines.append(f"任务：{job_name}")
    lines.append(f"级别：{str(level).upper()}")
    lines.append("")
    lines.append(str(message or ""))

    if extra:
        lines.append("")
        lines.append("**附加信息**")
        for k, v in extra.items():
            lines.append(f"{k}：{v}")

    return send_markdown(
        "\n".join(lines),
        webhook=webhook,
        category="SYSTEM_ERROR" if str(level).upper() in {"ERROR", "CRITICAL"} else "SYSTEM_INFO",
        title="",
        **kwargs,
    )


def notify_daily_report(content: str, webhook: str = "", **kwargs: Any) -> Dict[str, Any]:
    body = content or "无日报内容。"
    if not body.startswith("#") and not body.startswith("**"):
        body = f"**A股交易日报**\n\n{body}"
    return send_markdown(body, webhook=webhook, category="DAILY_REPORT", title="", **kwargs)


def notify_position_report(content: str, webhook: str = "", **kwargs: Any) -> Dict[str, Any]:
    body = content or "当前没有持仓报告内容。"
    if not body.startswith("#") and not body.startswith("**"):
        body = f"**持仓跟踪报告**\n\n{body}"
    return send_markdown(body, webhook=webhook, category="POSITION", title="", **kwargs)


def notify_trade_scan(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无交易信号内容。", webhook=webhook, category="TRADE_SCAN", title=title, **kwargs)


def notify_buy_trigger(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无买入触发内容。", webhook=webhook, category="BUY_TRIGGER", title=title, **kwargs)


def notify_near_trigger(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无接近触发内容。", webhook=webhook, category="NEAR_TRIGGER", title=title, **kwargs)


def notify_paper_trade(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无纸面交易内容。", webhook=webhook, category="PAPER_TRADE", title=title, **kwargs)


def notify_paper_event(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return notify_paper_trade(content, webhook=webhook, title=title, **kwargs)


def notify_paper_buy(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无纸面买入内容。", webhook=webhook, category="PAPER_BUY", title=title, **kwargs)


def notify_paper_exit(content: str, webhook: str = "", title: str = "", **kwargs: Any) -> Dict[str, Any]:
    return send_markdown(content or "无纸面退出内容。", webhook=webhook, category="PAPER_EXIT", title=title, **kwargs)


# 兼容某些旧代码可能导入的名称。
send_wechat_markdown = send_markdown
send_wechat_text = send_text
wechat_markdown = send_markdown


__all__ = [
    "PROJECT_NOTIFY_NAME",
    "DEFAULT_WECHAT_WEBHOOK",
    "WECHAT_MARKDOWN_SAFE_BYTES",
    "WECHAT_TEXT_SAFE_BYTES",
    "WECHAT_NOTIFY_DELAY_SECONDS",
    "WECHAT_GLOBAL_MIN_INTERVAL",
    "get_webhook",
    "send_markdown",
    "send_text",
    "send_wechat_markdown",
    "send_wechat_text",
    "notify_system_event",
    "notify_daily_report",
    "notify_position_report",
    "notify_trade_scan",
    "notify_buy_trigger",
    "notify_near_trigger",
    "notify_paper_trade",
    "notify_paper_event",
    "notify_paper_buy",
    "notify_paper_exit",
]
