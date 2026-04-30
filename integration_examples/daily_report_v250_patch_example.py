"""
daily_report v2.5.0 接入示例。
"""

from core.daily_stats_v250 import build_v250_daily_section


def append_v250_daily_report(report_text, final_candidates):
    section = build_v250_daily_section(final_candidates)
    return report_text + "\n\n" + section
