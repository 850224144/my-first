#!/usr/bin/env python3
"""
刷新交易日历脚本
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from core.trading_calendar import refresh_trading_calendar
    trading_days = refresh_trading_calendar()
    print(f"交易日历刷新完成，共 {len(trading_days)} 个交易日")
    print("最近10个交易日:", trading_days[-10:])
except Exception as e:
    print(f"刷新交易日历失败: {e}")
    sys.exit(1)