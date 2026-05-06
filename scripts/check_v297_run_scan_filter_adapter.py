#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]

def check_file(path: Path) -> bool:
    if not path.exists():
        print(f"- {path}: 不存在，跳过")
        return True

    s = path.read_text(encoding="utf-8")
    checks = {
        "has_helper": "_filter_result_to_df_v297" in s and "_df_is_empty_v297" in s,
        "sector_adapter": 'filter_universe_by_strong_sector(universe_df' in s and '_filter_result_to_df_v297(filtered, prefer="passed")' in s,
        "weekly_adapter": 'filter_by_weekly_trend(universe_df' in s and '_filter_result_to_df_v297(filtered, prefer="passed")' in s,
        "empty_adapter": "if _df_is_empty_v297(filtered):" in s,
    }

    print(f"【检查 {path.relative_to(ROOT)}】")
    ok = True
    for k, v in checks.items():
        print(f"- {k}: {v}")
        ok = ok and v
    return ok

def main():
    ok1 = check_file(ROOT / "run_scan.py")
    ok2 = check_file(ROOT / "core" / "run_scan.py")

    if not (ok1 and ok2):
        print("")
        print("检查未通过。")
        sys.exit(1)

    print("")
    print("检查通过。")
    print("下一步：")
    print("python run_scan.py --mode observe --workers 1")

if __name__ == "__main__":
    main()
