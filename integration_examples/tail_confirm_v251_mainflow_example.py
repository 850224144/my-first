"""
tail_confirm 主流程接入示例。

把 process_tail_candidate_v251 接到你现有 tail_confirm 最终候选处理处。
"""

from core.pipeline_v251 import process_tail_candidate_v251
from core.notify_dedupe import should_notify, record_notification


def handle_tail_confirm_candidates_v251(
    candidates,
    *,
    trade_date,
    trading_days=None,
    strategy_config=None,
    db_path="./data/trading_state.db",
    send_wecom_func=None,
):
    results = []
    open_plans = []

    for c in candidates:
        out = process_tail_candidate_v251(
            c,
            trade_date=trade_date,
            trading_days=trading_days,
            strategy_config=strategy_config,
            db_path=db_path,
            persist=True,
        )
        results.append(out)

        if out.get("open_recheck_plan"):
            open_plans.append(out["open_recheck_plan"])

        if send_wecom_func:
            symbol = out.get("symbol") or out.get("code")
            status = out.get("signal_status")
            ok, reason = should_notify(
                db_path,
                trade_date=trade_date,
                symbol=symbol,
                status=status,
                channel="wecom",
                message_key=out.get("wecom_message", "")[:300],
            )
            if ok:
                send_wecom_func(out["wecom_message"])
                record_notification(
                    db_path,
                    trade_date=trade_date,
                    symbol=symbol,
                    status=status,
                    channel="wecom",
                    message_key=out.get("wecom_message", "")[:300],
                )

    # TODO: 将 open_plans 合并写入 data/trade_plan.parquet
    return results, open_plans
