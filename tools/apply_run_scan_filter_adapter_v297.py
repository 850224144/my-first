#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import sys
import datetime as dt

HELPER_MARKER = "# === v2.9.7 filter result adapter ==="

HELPER = """
# === v2.9.7 filter result adapter ===
def _filter_result_to_df_v297(result, *, prefer: str = "passed"):
    \"""
    过滤模块可能返回：
    1. polars.DataFrame
    2. pandas.DataFrame
    3. {"passed": [...], "rejected": [...], "scored": [...]}

    run_scan 主流程使用 .is_empty()，所以 dict 需要转回 polars.DataFrame。
    \"""
    if result is None:
        return None

    if isinstance(result, dict):
        rows = result.get(prefer)
        if rows is None:
            rows = result.get("passed")
        if rows is None:
            rows = result.get("scored")
        if rows is None:
            rows = []

        try:
            import polars as pl
            return pl.DataFrame(rows)
        except Exception:
            return rows

    return result


def _df_is_empty_v297(df) -> bool:
    if df is None:
        return True

    is_empty_attr = getattr(df, "is_empty", None)
    if callable(is_empty_attr):
        try:
            return bool(is_empty_attr())
        except Exception:
            pass
    elif is_empty_attr is not None:
        try:
            return bool(is_empty_attr)
        except Exception:
            pass

    empty_attr = getattr(df, "empty", None)
    if empty_attr is not None:
        try:
            return bool(empty_attr)
        except Exception:
            pass

    try:
        return len(df) == 0
    except Exception:
        return False
# === end v2.9.7 filter result adapter ===
"""


def backup(path: Path) -> Path:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_name(f"{path.name}.bak_v297_{ts}")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak


def insert_helper(text: str) -> str:
    if HELPER_MARKER in text:
        return text

    idx = text.find("\ndef ")
    if idx != -1:
        return text[:idx] + "\n\n" + HELPER.strip() + "\n" + text[idx:]

    return HELPER.strip() + "\n\n" + text


def patch_filter_blocks(text: str) -> str:
    old_sector = 'filtered = filter_universe_by_strong_sector(universe_df, market_state=market_state, strict=strict_sector)\n        if filtered is None or filtered.is_empty():'
    new_sector = 'filtered = filter_universe_by_strong_sector(universe_df, market_state=market_state, strict=strict_sector)\n        filtered = _filter_result_to_df_v297(filtered, prefer="passed")\n        if _df_is_empty_v297(filtered):'

    old_weekly = 'filtered = filter_by_weekly_trend(universe_df, strict=strict_weekly)\n        if filtered is None or filtered.is_empty():'
    new_weekly = 'filtered = filter_by_weekly_trend(universe_df, strict=strict_weekly)\n        filtered = _filter_result_to_df_v297(filtered, prefer="passed")\n        if _df_is_empty_v297(filtered):'

    changed = False
    if old_sector in text:
        text = text.replace(old_sector, new_sector)
        changed = True
    if old_weekly in text:
        text = text.replace(old_weekly, new_weekly)
        changed = True

    # 如果已经替换过函数调用，但还没替换 empty 判断，做宽松兜底
    if 'filter_universe_by_strong_sector(universe_df, market_state=market_state, strict=strict_sector)' in text:
        target = 'filtered = filter_universe_by_strong_sector(universe_df, market_state=market_state, strict=strict_sector)'
        replacement = target + '\n        filtered = _filter_result_to_df_v297(filtered, prefer="passed")'
        if replacement not in text:
            text = text.replace(target, replacement)
            changed = True

    if 'filter_by_weekly_trend(universe_df, strict=strict_weekly)' in text:
        target = 'filtered = filter_by_weekly_trend(universe_df, strict=strict_weekly)'
        replacement = target + '\n        filtered = _filter_result_to_df_v297(filtered, prefer="passed")'
        if replacement not in text:
            text = text.replace(target, replacement)
            changed = True

    text2 = text.replace('if filtered is None or filtered.is_empty():', 'if _df_is_empty_v297(filtered):')
    if text2 != text:
        text = text2
        changed = True

    return text


def patch_file(path: Path) -> None:
    if not path.exists():
        print(f"跳过，不存在：{path}")
        return

    text = path.read_text(encoding="utf-8")
    if HELPER_MARKER in text and "_filter_result_to_df_v297(filtered" in text and "_df_is_empty_v297(filtered)" in text:
        print(f"已存在 v2.9.7 适配，跳过：{path}")
        return

    bak = backup(path)
    text = insert_helper(text)
    text = patch_filter_blocks(text)
    path.write_text(text, encoding="utf-8")
    print(f"已备份：{bak}")
    print(f"已修补：{path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: apply_run_scan_filter_adapter_v297.py <PROJECT_ROOT>")
        sys.exit(2)

    root = Path(sys.argv[1]).resolve()

    patch_file(root / "run_scan.py")
    patch_file(root / "core" / "run_scan.py")

    print("")
    print("v2.9.7-fixed run_scan filter result adapter done.")


if __name__ == "__main__":
    main()
