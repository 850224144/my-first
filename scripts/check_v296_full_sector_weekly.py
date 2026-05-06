#!/usr/bin/env python3
from pathlib import Path
import sys
import inspect

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    import pandas as pd
    from core.sector import filter_universe_by_strong_sector
    from core.weekly import filter_by_weekly_trend

    s1 = str(inspect.signature(filter_universe_by_strong_sector))
    s2 = str(inspect.signature(filter_by_weekly_trend))

    print("【v2.9.6 full sector/weekly Series 安全检查】")
    print(f"filter_universe_by_strong_sector signature: {s1}")
    print(f"filter_by_weekly_trend signature: {s2}")

    ok1 = "market_state" in s1 and "kwargs" in s1
    ok2 = "strict" in s2 and "kwargs" in s2

    print(f"- sector accepts market_state + kwargs: {ok1}")
    print(f"- weekly accepts strict + kwargs: {ok2}")

    if not ok1 or not ok2:
        print("签名检查未通过。")
        sys.exit(1)

    candidates_df = pd.DataFrame([
        {"code": "600000.SH", "name": "测试股"},
        {"code": "000001.SZ", "name": "测试股2"},
    ])

    pool_df = pd.DataFrame([
        {"symbol": "600000.SH", "surge_reason": "AI算力", "limit_up_days": 2, "first_limit_up_time": "09:35"},
    ])

    # DataFrame pool
    sector_result = filter_universe_by_strong_sector(
        candidates_df,
        core_pools={"limit_up": pool_df, "continuous_limit_up": pd.DataFrame(), "strong_stock": []},
        market_state="STRONG",
        unknown_future_param=True,
    )
    assert isinstance(sector_result, dict)
    assert set(sector_result.keys()) == {"passed", "rejected", "scored"}

    # 列字典 + Series
    sector_result2 = filter_universe_by_strong_sector(
        {"code": pd.Series(["600000.SH", "000001.SZ"]), "name": pd.Series(["A", "B"])},
        core_pools={"limit_up": {"symbol": pd.Series(["600000.SH"]), "surge_reason": pd.Series(["AI算力"]), "limit_up_days": pd.Series([2])}},
        market_state="STRONG",
    )
    assert isinstance(sector_result2, dict)

    bars_df = pd.DataFrame([
        {"trade_date": f"2026-01-{i:02d}", "open": 10+i*0.01, "high": 10+i*0.02, "low": 10, "close": 10+i*0.01, "volume": 1000, "amount": 100000}
        for i in range(1, 29)
    ])
    weekly_candidates = [{"code": "600000.SH", "daily_bars": bars_df}]
    weekly_result = filter_by_weekly_trend(
        weekly_candidates,
        strict=True,
        unknown_future_param=True,
    )
    assert isinstance(weekly_result, dict)
    assert set(weekly_result.keys()) == {"passed", "rejected", "scored"}

    weekly_candidates2 = {"code": pd.Series(["600000.SH"]), "daily_bars": [bars_df]}
    weekly_result2 = filter_by_weekly_trend(weekly_candidates2, strict=True)
    assert isinstance(weekly_result2, dict)

    print("- DataFrame compatibility: OK")
    print("- Series / column-dict compatibility: OK")
    print("")
    print("检查通过。")
    print("下一步：")
    print("python run_scan.py --mode observe --workers 1")

if __name__ == "__main__":
    main()
