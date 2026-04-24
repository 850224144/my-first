# -*- coding: utf-8 -*-
"""
core/logger.py

统一日志模块：
- 控制台：只输出关键进度和错误
- 文件日志：记录详细调试信息
- reject CSV：记录股票被过滤/跳过的原因
- raw bad rows JSONL：记录数据源返回的异常原始行
- reject summary：每次运行后汇总过滤原因，方便快速定位问题
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sys
import threading
from collections import Counter
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional, Any
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")
LOG_DIR = "logs"
LOGGER_NAME = "a_stock"

_reject_lock = threading.Lock()
_raw_lock = threading.Lock()
_reject_file: Optional[str] = None
_raw_bad_rows_file: Optional[str] = None
_logger_configured = False


def now_str() -> str:
    return datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now(CN_TZ).strftime("%Y%m%d")


def setup_logger(
    log_dir: str = LOG_DIR,
    level: str = "INFO",
    log_file: Optional[str] = None,
    console: bool = True,
    reject_file: Optional[str] = None,
    raw_bad_rows_file: Optional[str] = None,
) -> logging.Logger:
    """初始化日志。重复调用不会重复添加 handler。"""
    global _logger_configured, _reject_file, _raw_bad_rows_file

    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # 避免重复 handler
    if not _logger_configured:
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(getattr(logging, level.upper(), logging.INFO))
            ch.setFormatter(fmt)
            logger.addHandler(ch)

        if log_file is None:
            log_file = os.path.join(log_dir, f"scan_{today_str()}.log")
        fh = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        _logger_configured = True

    if reject_file is None:
        reject_file = os.path.join(log_dir, f"rejects_{today_str()}.csv")
    _reject_file = reject_file
    _init_reject_file(_reject_file)

    if raw_bad_rows_file is None:
        raw_bad_rows_file = os.path.join(log_dir, f"raw_bad_rows_{today_str()}.jsonl")
    _raw_bad_rows_file = raw_bad_rows_file

    logger.info("日志初始化完成：log_dir=%s, reject_file=%s, raw_bad_rows_file=%s", log_dir, _reject_file, _raw_bad_rows_file)
    return logger


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        setup_logger()
    return logger


def get_reject_file() -> str:
    global _reject_file
    if _reject_file is None:
        setup_logger()
    return _reject_file or os.path.join(LOG_DIR, f"rejects_{today_str()}.csv")


def get_raw_bad_rows_file() -> str:
    global _raw_bad_rows_file
    if _raw_bad_rows_file is None:
        setup_logger()
    return _raw_bad_rows_file or os.path.join(LOG_DIR, f"raw_bad_rows_{today_str()}.jsonl")


def _init_reject_file(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with _reject_lock:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["time", "code", "name", "stage", "reason", "detail"])


def log_reject(
    code: str,
    stage: str,
    reason: str,
    detail: Any = "",
    name: str = "",
) -> None:
    """记录股票被过滤/跳过原因到 CSV，不刷屏。"""
    path = get_reject_file()
    try:
        with _reject_lock:
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow([now_str(), code, name, stage, reason, str(detail)])
    except Exception as e:
        get_logger().debug("写入 reject 日志失败：%s", e)


def log_source_failure(code: str, source: str, reason: str, detail: Any = "") -> None:
    """记录数据源失败，写入 debug 日志 + reject CSV。"""
    get_logger().debug("数据源失败 | code=%s | source=%s | reason=%s | detail=%s", code, source, reason, detail)
    log_reject(code=code, stage=f"source:{source}", reason=reason, detail=detail)


def log_raw_bad_row(code: str, source: str, row: Any, reason: str) -> None:
    """记录无法进入主行情表的原始异常行。"""
    path = get_raw_bad_rows_file()
    record = {
        "time": now_str(),
        "code": code,
        "source": source,
        "reason": reason,
        "row": row,
    }
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with _raw_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        get_logger().debug("raw bad row | code=%s | source=%s | reason=%s | row=%s", code, source, reason, row)
    except Exception as e:
        get_logger().debug("写入 raw bad rows 日志失败：%s", e)


def log_exception(msg: str, exc: Exception, level: int = logging.DEBUG) -> None:
    get_logger().log(level, "%s | %s", msg, exc, exc_info=(level <= logging.DEBUG))


def summarize_rejects(top_n: int = 20, print_console: bool = True) -> Counter:
    """汇总 rejects_YYYYMMDD.csv 中的 reason，运行结束时打印。"""
    path = get_reject_file()
    counter: Counter = Counter()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return counter
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                reason = row.get("reason") or "unknown"
                counter[reason] += 1
    except Exception as e:
        get_logger().debug("汇总 reject 日志失败：%s", e)
        return counter

    if counter:
        lines = ["过滤/跳过原因统计："]
        for reason, count in counter.most_common(top_n):
            lines.append(f"  {reason:<30} {count}")
        text = "\n".join(lines)
        get_logger().info("\n%s", text)
        if print_console:
            print("\n" + text)
    return counter
