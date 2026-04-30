"""
企业微信/通知去重模块。

目标：
- 同一交易日、同一股票、同一状态不重复提醒
- 状态升级/降级才提醒
"""

from __future__ import annotations

from typing import Optional, Tuple
import sqlite3
import datetime as dt
import hashlib


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS notification_state (
    trade_date TEXT,
    symbol TEXT,
    channel TEXT,
    last_status TEXT,
    last_key_hash TEXT,
    last_sent_at TEXT,
    PRIMARY KEY (trade_date, symbol, channel)
);
"""


def init_notify_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_SQL)
        conn.commit()


def _hash_key(message_key: str) -> str:
    return hashlib.sha256((message_key or "").encode("utf-8")).hexdigest()[:16]


def should_notify(
    db_path: str,
    *,
    trade_date: str,
    symbol: str,
    status: str,
    channel: str = "wecom",
    message_key: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    返回：是否应该发送、原因。
    """
    init_notify_db(db_path)
    key_hash = _hash_key(message_key or status)

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT last_status, last_key_hash
            FROM notification_state
            WHERE trade_date=? AND symbol=? AND channel=?
            """,
            (trade_date, symbol, channel),
        ).fetchone()

    if row is None:
        return True, "首次提醒"

    last_status, last_key_hash = row
    if last_status != status:
        return True, f"状态变化：{last_status} -> {status}"

    if last_key_hash != key_hash:
        return True, "同状态但关键信息变化"

    return False, "同一状态已提醒，跳过"


def record_notification(
    db_path: str,
    *,
    trade_date: str,
    symbol: str,
    status: str,
    channel: str = "wecom",
    message_key: Optional[str] = None,
) -> None:
    init_notify_db(db_path)
    key_hash = _hash_key(message_key or status)
    now = dt.datetime.now().isoformat(timespec="seconds")

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO notification_state(trade_date, symbol, channel, last_status, last_key_hash, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_date, symbol, channel)
            DO UPDATE SET
                last_status=excluded.last_status,
                last_key_hash=excluded.last_key_hash,
                last_sent_at=excluded.last_sent_at
            """,
            (trade_date, symbol, channel, status, key_hash, now),
        )
        conn.commit()
