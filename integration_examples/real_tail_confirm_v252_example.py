"""
真实 tail_confirm 接入示例。

重点：
- 使用 process_tail_candidate_v252
- 默认禁止 demo 数据写入
- 结果写入 data/trading_state.db
"""

from core.pipeline_v252 import process_tail_candidate_v252


def handle_real_tail_confirm_candidates_v252(candidates, *, trade_date, trading_days=None, send_wecom_func=None):
    results = []
    open_plans = []

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

        if out.get("open_recheck_plan"):
            open_plans.append(out["open_recheck_plan"])

        if send_wecom_func and out.get("wecom_message"):
            send_wecom_func(out["wecom_message"])

    # TODO: open_plans 写回 data/trade_plan.parquet
    return results, open_plans
