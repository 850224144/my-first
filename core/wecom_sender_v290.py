"""
v2.9.0 企业微信发送入口。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import os
import time
import requests


def split_text_by_bytes_v290(text: str, max_bytes: int = 3500) -> List[str]:
    chunks = []
    cur = ""
    for line in text.splitlines():
        candidate = cur + ("\n" if cur else "") + line
        if len(candidate.encode("utf-8")) > max_bytes and cur:
            chunks.append(cur)
            cur = line
        else:
            cur = candidate
    if cur:
        chunks.append(cur)
    return chunks


def send_wecom_markdown_v290(
    *,
    webhook_url: Optional[str] = None,
    content: str,
    dry_run: bool = False,
    sleep_seconds: float = 0.8,
) -> Dict[str, Any]:
    webhook_url = webhook_url or os.getenv("WECOM_WEBHOOK_URL")
    chunks = split_text_by_bytes_v290(content)

    if dry_run or not webhook_url:
        return {
            "sent": False,
            "dry_run": True,
            "reason": "missing_webhook_or_dry_run",
            "chunks": len(chunks),
            "preview": chunks[:2],
        }

    results = []
    for i, chunk in enumerate(chunks, 1):
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": chunk,
            },
        }
        r = requests.post(webhook_url, json=payload, timeout=10)
        try:
            data = r.json()
        except Exception:
            data = {"status_code": r.status_code, "text": r.text[:500]}
        results.append(data)
        time.sleep(sleep_seconds)

    return {
        "sent": True,
        "chunks": len(chunks),
        "results": results,
    }
