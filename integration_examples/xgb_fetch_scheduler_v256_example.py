"""
选股宝 core_pools 调度接入示例。

建议接入：
- 09:15 preflight 后拉一次
- 14:20 watchlist_refresh 前拉一次
- 22:30 night_cache_expand 拉历史/当天缓存

不要高频拉，避免接口压力。
"""

def register_xgb_fetch_jobs(scheduler):
    scheduler.add_job(
        func=fetch_xgb_core_pools_job,
        trigger="cron",
        hour=9,
        minute=16,
        id="fetch_xgb_core_pools_0916",
        replace_existing=True,
    )
    scheduler.add_job(
        func=fetch_xgb_core_pools_job,
        trigger="cron",
        hour=14,
        minute=18,
        id="fetch_xgb_core_pools_1418",
        replace_existing=True,
    )


def fetch_xgb_core_pools_job():
    from core.xgb_client import XGBClient
    from core.xgb_cache import XGBCache
    from core.xgb_core_pools_v255 import CORE_POOLS
    import datetime as dt

    trade_date = dt.date.today().isoformat()
    client = XGBClient()
    cache = XGBCache(root="data/xgb")

    for pool in CORE_POOLS:
        cache.get_pool(client, pool, trade_date=trade_date, allow_stale=True)
