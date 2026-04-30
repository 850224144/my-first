"""
v2.6.1 tail_confirm 接入示例。

核心变化：
tail_confirm 不再全量读取 watchlist.parquet，而是优先读取 watchlist_tail_focus.parquet。
"""

from core.tail_focus_loader_v261 import load_tail_focus_v261
from core.pipeline_v252 import process_tail_candidate_v252


def run_tail_confirm_with_tail_focus_v261(*, trade_date, trading_days=None, send_wecom_func=None):
    candidates = load_tail_focus_v261("data/watchlist_tail_focus.parquet")
    results = []

    for c in candidates:
        out = process_tail_candidate_v252(
            c,
            trade_date=trade_date,
            trading_days=trading_days,
            db_path="data/trading_state.db",
            persist=True,
            allow_demo=False,
        )
        results.append(out)

        if send_wecom_func and out.get("wecom_message"):
            send_wecom_func(out["wecom_message"])

    return results
