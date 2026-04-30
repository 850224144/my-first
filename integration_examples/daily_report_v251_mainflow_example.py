"""
日报接入示例。
"""

from core.state_store_v251 import fetch_today_final_signals
from core.daily_stats_v250 import build_v250_daily_section


def append_v251_daily_report_from_db(report_text, *, trade_date, db_path="./data/trading_state.db"):
    rows = fetch_today_final_signals(db_path=db_path, trade_date=trade_date)
    section = build_v250_daily_section(rows)
    return report_text + "\n\n" + section
