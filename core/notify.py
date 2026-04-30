# core/notify.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
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

# 企业微信 markdown 单条上限约 4096 字符；这里留足安全余量。
WECHAT_MARKDOWN_SAFE_LIMIT = int(os.getenv("WECHAT_MARKDOWN_SAFE_LIMIT", "3300"))
WECHAT_NOTIFY_DELAY_SECONDS = float(os.getenv("WECHAT_NOTIFY_DELAY_SECONDS", "1.2"))

CATEGORY_DISPLAY = {
    "TRADE_SCAN": "交易信号",
    "POSITION": "持仓风控",
    "DAILY_REPORT": "交易日报",
    "SYSTEM_ERROR": "系统异常",
    "SYSTEM_INFO": "系统信息",
    "PAPER_TRADE": "纸面交易",
    "BUY_TRIGGER": "买入触发",
    "INFO": "系统信息",
}


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


def _category_name(category: str) -> str:
    return CATEGORY_DISPLAY.get(str(category).upper(), str(category) or "通知")


def _project_title(category: str, title: str = "", page: int = 0, total: int = 0) -> str:
    """
    统一企业微信标题。
    title 如果已经是 【...】 形式，则直接作为标题基底，避免重复包一层。
    """
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
    """避免内容本身已经带项目总标题时重复。"""
    if not content:
        return ""
    lines = content.splitlines()
    if lines and lines[0].startswith(f"【{PROJECT_NOTIFY_NAME}｜"):
        return "\n".join(lines[1:]).lstrip()
    return content


def _simplify_markdown(content: str) -> str:
    """
    企业微信 markdown 容易因为复杂格式/超长内容报 40058。
    这里统一做简化：
    - 去掉代码块和行内反引号
    - 表格行转成简单中文分隔行，分隔符行直接删除
    - 标题转成加粗
    - 引用符号去掉，保留文字
    - 控制空行数量
    """
    text = str(content or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\x00", "")

    out: List[str] = []
    in_code_block = False
    blank_count = 0

    for raw in text.split("\n"):
        line = raw.rstrip()
        stripped = line.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue

        # 代码块内保留纯文本，但不保留 markdown 代码格式。
        if in_code_block:
            line = line.replace("`", "")
        else:
            line = line.replace("`", "")

        stripped = line.strip()

        # markdown 表格分隔行，例如 |---|---| 或 ---|---
        if stripped and set(stripped.replace("|", "").replace(":", "").replace("-", "").replace(" ", "")) == set():
            continue
        if re.fullmatch(r"[\|\s:\-]+", stripped or ""):
            continue

        # 表格行改成简单中文斜杠分隔，避免企业微信解析异常。
        if stripped.count("|") >= 2:
            parts = [p.strip() for p in stripped.strip("|").split("|") if p.strip()]
            if parts:
                line = " / ".join(parts)

        # 标题转加粗，少用复杂 MD。
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            line = f"**{m.group(2).strip()}**"

        # 引用转普通缩进行。
        if line.lstrip().startswith(">"):
            line = line.lstrip()[1:].strip()

        # 控制空行，避免内容膨胀。
        if not line.strip():
            blank_count += 1
            if blank_count > 1:
                continue
        else:
            blank_count = 0

        out.append(line)

    return "\n".join(out).strip()


def _split_long_line(line: str, limit: int) -> List[str]:
    if len(line) <= limit:
        return [line]
    return [line[i:i + limit] for i in range(0, len(line), limit)]


def _split_body(body: str, title_budget: int, safe_limit: int) -> List[str]:
    body_limit = max(1000, safe_limit - title_budget - 30)
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0

    for line in body.splitlines():
        pieces = _split_long_line(line, body_limit)
        for piece in pieces:
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


def _build_pages(content: str, category: str, title: str = "") -> List[str]:
    body = _strip_existing_project_prefix(content or "")
    body = _simplify_markdown(body)

    # 估算标题长度，给分页留余量。
    sample_title = _project_title(category, title, page=99, total=99)
    chunks = _split_body(body, title_budget=len(sample_title), safe_limit=WECHAT_MARKDOWN_SAFE_LIMIT)
    if not chunks:
        chunks = ["无内容。"]

    total = len(chunks)
    pages: List[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        header = _project_title(category, title, page=idx, total=total)
        page = f"{header}\n{chunk}".strip()
        # 双保险：如果仍超过限制，继续硬切。
        if len(page) > WECHAT_MARKDOWN_SAFE_LIMIT:
            sub_limit = max(800, WECHAT_MARKDOWN_SAFE_LIMIT - len(header) - 30)
            for j, sub in enumerate(_split_long_line(chunk, sub_limit), start=1):
                sub_header = _project_title(category, f"{title} {idx}.{j}".strip(), page=idx, total=total)
                pages.append(f"{sub_header}\n{sub}".strip())
        else:
            pages.append(page)
    return pages


def send_markdown(
    content: str,
    webhook: str = "",
    category: str = "INFO",
    title: str = "",
    print_content: bool = True,
) -> Dict[str, Any]:
    """
    企业微信统一推送入口。

    关键保护：
    - 单条 markdown 控制在安全长度内，避免 4096 字符上限导致 40058。
    - 长文自动拆成多页，多次接口请求。
    - 多页之间默认延时 1.2 秒，避免瞬间连发。
    - 自动简化复杂 Markdown：不用表格/代码块，保留加粗和简单分行。
    """
    webhook = get_webhook(webhook)
    category = str(category or "INFO").upper()
    pages = _build_pages(content=content or "", category=category, title=title or "")

    if print_content:
        print("\n" + "=" * 70)
        print(f"📤 企业微信通知 | {PROJECT_NOTIFY_NAME} | {category} | pages={len(pages)}")
        print("=" * 70)
        for i, page in enumerate(pages, start=1):
            print(f"----- page {i}/{len(pages)} | chars={len(page)} -----")
            print(page)
        print("=" * 70)

    if not webhook:
        print("⚠️ WECHAT_WEBHOOK 为空，跳过企业微信推送")
        return {"ok": False, "reason": "webhook_empty", "pages": len(pages)}

    results: List[Dict[str, Any]] = []
    for i, page in enumerate(pages, start=1):
        try:
            result = _send_wechat_markdown(webhook, page)
            results.append(result)
            print(f"✅ 企业微信返回 page={i}/{len(pages)} chars={len(page)}：{result}")
        except Exception as e:
            err = {"ok": False, "reason": str(e), "page": i, "chars": len(page)}
            results.append(err)
            print(f"⚠️ 企业微信推送失败 page={i}/{len(pages)} chars={len(page)}：{e}")

        if i < len(pages):
            time.sleep(max(1.0, WECHAT_NOTIFY_DELAY_SECONDS))

    ok = all((r.get("errcode") == 0) or (r.get("ok") is True) for r in results if isinstance(r, dict))
    return {"ok": ok, "pages": len(pages), "results": results}


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

    lines = [f"**{level_icon} {title}**"]
    lines.append(f"时间：{_now_str()}")
    if job_name:
        lines.append(f"任务：{job_name}")
    lines.append(f"级别：{level.upper()}")
    lines.append("")
    lines.append(message)

    if extra:
        lines.append("")
        lines.append("**附加信息**")
        for k, v in extra.items():
            lines.append(f"{k}：{v}")

    return send_markdown(
        "\n".join(lines),
        webhook=webhook,
        category="SYSTEM_ERROR" if level.upper() in {"ERROR", "CRITICAL"} else "SYSTEM_INFO",
        title="",
    )


def notify_daily_report(content: str, webhook: str = "") -> Dict[str, Any]:
    body = content or "无日报内容。"
    if not body.startswith("#") and not body.startswith("**"):
        body = f"**A股交易日报**\n\n{body}"
    return send_markdown(body, webhook=webhook, category="DAILY_REPORT", title="")


def notify_position_report(content: str, webhook: str = "") -> Dict[str, Any]:
    body = content or "当前没有持仓报告内容。"
    if not body.startswith("#") and not body.startswith("**"):
        body = f"**持仓跟踪报告**\n\n{body}"
    return send_markdown(body, webhook=webhook, category="POSITION", title="")


__all__ = [
    "PROJECT_NOTIFY_NAME",
    "DEFAULT_WECHAT_WEBHOOK",
    "WECHAT_MARKDOWN_SAFE_LIMIT",
    "WECHAT_NOTIFY_DELAY_SECONDS",
    "get_webhook",
    "send_markdown",
    "notify_system_event",
    "notify_daily_report",
    "notify_position_report",
]
