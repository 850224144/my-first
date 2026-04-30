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

# 企业微信 markdown 单条上限约 4096 字符。这里保守控制在 3000，避免中文/转义/标题导致 40058。
WECHAT_MARKDOWN_SAFE_LIMIT = int(os.getenv("WECHAT_MARKDOWN_SAFE_LIMIT", "3000"))
# 多页推送最小间隔，避免瞬间连发。
WECHAT_NOTIFY_DELAY_SECONDS = max(1.2, float(os.getenv("WECHAT_NOTIFY_DELAY_SECONDS", "1.5")))
# 跨进程限流：不同脚本连续发消息时也至少间隔这么久。
WECHAT_GLOBAL_MIN_INTERVAL = max(1.0, float(os.getenv("WECHAT_GLOBAL_MIN_INTERVAL", "1.2")))
NOTIFY_STATE_DIR = os.getenv("NOTIFY_STATE_DIR", "data/notify_state")
LAST_SEND_FILE = os.path.join(NOTIFY_STATE_DIR, "last_wechat_send.json")

CATEGORY_DISPLAY = {
    "TRADE_SCAN": "交易信号",
    "TRADE_SIGNAL": "交易信号",
    "WATCHLIST": "观察池刷新",
    "TAIL_CONFIRM": "尾盘确认",
    "AFTER_CLOSE": "收盘复盘",
    "BUY_TRIGGER": "买入触发",
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


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def _strip_other_bracket_title(content: str) -> str:
    """如果内容第一行是旧式【盘中观察候选】这类标题，保留但不重复复杂格式。"""
    if not content:
        return ""
    return str(content)


def _simplify_markdown(content: str) -> str:
    """
    企业微信 markdown 对长文、表格、代码块较敏感，容易报 40058。
    全项目通知统一经过这里：扫描报告、买入触发、纸面交易、持仓、日报、系统异常都会被处理。

    策略：
    - 去掉代码块/行内反引号
    - 表格转普通文本，删除表格分隔行
    - 标题转加粗
    - 引用转普通行
    - 连续空行压缩
    - 只保留简单加粗 + 分行
    """
    text = str(content or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = _strip_existing_project_prefix(text)
    text = _strip_other_bracket_title(text)

    out: List[str] = []
    in_code_block = False
    blank_count = 0

    for raw in text.split("\n"):
        line = raw.rstrip()
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue

        line = line.replace("`", "")
        stripped = line.strip()

        # 删除表格分隔行：|---|---| 或 :---:
        if re.fullmatch(r"[\|\s:\-]+", stripped or ""):
            continue

        # Markdown 表格行转成简单中文分隔，避免复杂 MD。
        if stripped.count("|") >= 2:
            parts = [p.strip() for p in stripped.strip("|").split("|") if p.strip()]
            if parts:
                line = " / ".join(parts)
                stripped = line.strip()

        # Markdown 标题转加粗。
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            line = f"**{m.group(2).strip()}**"
            stripped = line.strip()

        # 引用 > 转普通行。
        if line.lstrip().startswith(">"):
            line = line.lstrip()[1:].strip()
            stripped = line.strip()

        # 删除过多列表缩进，保留简单符号。
        line = re.sub(r"^\s{2,}[-*]\s+", "- ", line)

        if not stripped:
            blank_count += 1
            if blank_count > 1:
                continue
        else:
            blank_count = 0

        out.append(line)

    return "\n".join(out).strip()


def _split_long_line(line: str, limit: int) -> List[str]:
    line = str(line)
    if len(line) <= limit:
        return [line]
    return [line[i:i + limit] for i in range(0, len(line), limit)]


def _split_body_by_lines(body: str, title_budget: int, safe_limit: int) -> List[str]:
    # 再留 100 字余量，避免企业微信服务端转义后超过限制。
    body_limit = max(800, safe_limit - title_budget - 100)
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    for line in str(body or "").splitlines():
        # 单行过长先硬切。
        for piece in _split_long_line(line, body_limit):
            add_len = len(piece) + 1
            if current and current_len + add_len > body_limit:
                chunks.append("\n".join(current).strip())
                current = []
                current_len = 0
            current.append(piece)
            current_len += add_len

    if current:
        chunks.append("\n".join(current).strip())
    return [c for c in chunks if c]


def _build_pages(content: str, category: str = "INFO", title: str = "") -> List[str]:
    body = _simplify_markdown(content or "")
    sample_title = _project_title(category, title, page=99, total=99)
    chunks = _split_body_by_lines(body, title_budget=len(sample_title), safe_limit=WECHAT_MARKDOWN_SAFE_LIMIT)
    if not chunks:
        chunks = ["无内容。"]

    total = len(chunks)
    pages: List[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        header = _project_title(category, title, page=idx, total=total)
        page = f"{header}\n{chunk}".strip()

        # 双保险：如果标题 + 分块仍超限，继续硬切。
        if len(page) > WECHAT_MARKDOWN_SAFE_LIMIT:
            sub_limit = max(600, WECHAT_MARKDOWN_SAFE_LIMIT - len(header) - 100)
            subs = _split_long_line(chunk, sub_limit)
            for j, sub in enumerate(subs, start=1):
                sub_header = _project_title(category, f"{title} {idx}.{j}".strip(), page=idx, total=total)
                pages.append(f"{sub_header}\n{sub}".strip())
        else:
            pages.append(page)
    return pages


def _ensure_state_dir() -> None:
    os.makedirs(NOTIFY_STATE_DIR, exist_ok=True)


def _wait_global_rate_limit() -> None:
    """跨进程简单限流，避免多个任务连续瞬间推送。"""
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


def _send_wechat_markdown(webhook: str, content: str, timeout: int = 10) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送企业微信")
    if not webhook:
        return {"ok": False, "reason": "webhook_empty"}

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    resp = requests.post(webhook, json=payload, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text}


def _send_wechat_text(webhook: str, content: str, timeout: int = 10) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests 未安装，无法推送企业微信")
    if not webhook:
        return {"ok": False, "reason": "webhook_empty"}
    # text 消息也不要太长，企业微信文本通常也有长度限制。
    content = str(content or "")[:1800]
    payload = {"msgtype": "text", "text": {"content": content}}
    resp = requests.post(webhook, json=payload, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"ok": True, "text": resp.text}


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

    所有通知类型都会走这里：
    - 扫描报告 / 交易信号 / 观察池刷新 / 尾盘确认
    - 买入触发 / 接近触发
    - 纸面交易 / 纸面止损止盈
    - 持仓风控
    - 交易日报
    - 系统异常

    保护机制：
    1. 自动简化 Markdown，不用表格/代码块。
    2. 自动拆成多页，每页小于 WECHAT_MARKDOWN_SAFE_LIMIT。
    3. 多页之间默认延迟 >= 1.2 秒。
    4. 跨进程简单限流，禁止瞬间连发。
    5. 如果 markdown 被企业微信拒绝，单页自动降级成 text 兜底。
    """
    webhook = get_webhook(webhook)
    category = str(category or "INFO").upper()
    pages = _build_pages(content=content or "", category=category, title=title or "")

    if print_content:
        print("\n" + "=" * 70)
        print(f"📤 企业微信通知 | {PROJECT_NOTIFY_NAME} | {category} | pages={len(pages)}")
        print("=" * 70)
        for i, page in enumerate(pages, start=1):
            preview = page if len(page) <= 1500 else page[:1500] + "\n...（日志预览截断，实际按分页推送）"
            print(f"----- page {i}/{len(pages)} | chars={len(page)} -----")
            print(preview)
        print("=" * 70)

    if not webhook:
        print("⚠️ WECHAT_WEBHOOK 为空，跳过企业微信推送")
        return {"ok": False, "reason": "webhook_empty", "pages": len(pages)}

    results: List[Dict[str, Any]] = []
    for i, page in enumerate(pages, start=1):
        _wait_global_rate_limit()
        try:
            result = _send_wechat_markdown(webhook, page)
            # 如果企业微信返回参数错误，尝试 text 兜底，避免整条消息丢失。
            if isinstance(result, dict) and result.get("errcode") not in (None, 0):
                print(f"⚠️ 企业微信 markdown 返回异常 page={i}/{len(pages)} chars={len(page)}：{result}")
                fallback = _send_wechat_text(webhook, page)
                result = {"markdown_result": result, "text_fallback_result": fallback}
            results.append(result)
            _mark_global_sent()
            print(f"✅ 企业微信返回 page={i}/{len(pages)} chars={len(page)}：{result}")
        except Exception as e:
            err = {"ok": False, "reason": str(e), "page": i, "chars": len(page)}
            results.append(err)
            print(f"⚠️ 企业微信推送失败 page={i}/{len(pages)} chars={len(page)}：{e}")

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
    """统一走 send_markdown，避免不同路径遗漏分页和限流。"""
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


__all__ = [
    "PROJECT_NOTIFY_NAME",
    "DEFAULT_WECHAT_WEBHOOK",
    "WECHAT_MARKDOWN_SAFE_LIMIT",
    "WECHAT_NOTIFY_DELAY_SECONDS",
    "WECHAT_GLOBAL_MIN_INTERVAL",
    "get_webhook",
    "send_markdown",
    "send_text",
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
