#!/usr/bin/env python3
from pathlib import Path
import sys
import inspect

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.sector import filter_universe_by_strong_sector
    from core.weekly import filter_by_weekly_trend

    s1 = str(inspect.signature(filter_universe_by_strong_sector))
    s2 = str(inspect.signature(filter_by_weekly_trend))

    print("【v2.9.4 full sector/weekly 检查】")
    print(f"filter_universe_by_strong_sector signature: {s1}")
    print(f"filter_by_weekly_trend signature: {s2}")

    ok1 = "market_state" in s1 and "kwargs" in s1
    ok2 = "strict" in s2 and "kwargs" in s2

    print(f"- sector accepts market_state + kwargs: {ok1}")
    print(f"- weekly accepts strict + kwargs: {ok2}")

    if not ok1 or not ok2:
        print("检查未通过。")
        sys.exit(1)

    r1 = filter_universe_by_strong_sector([], market_state="NORMAL", unknown_future_param=True)
    r2 = filter_by_weekly_trend([], strict=True, unknown_future_param=True)

    assert isinstance(r1, dict)
    assert isinstance(r2, dict)
    assert set(r1.keys()) == {"passed", "rejected", "scored"}
    assert set(r2.keys()) == {"passed", "rejected", "scored"}

    print("")
    print("检查通过。")
    print("下一步：")
    print("python run_scheduler.py --run-once observe_afternoon")

if __name__ == "__main__":
    main()
