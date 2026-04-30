#!/usr/bin/env python3
"""
专门诊断当前 watchlist 为什么没有尾盘买点。
只读，不写库，不建仓。
"""

from pathlib import Path
import sys
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255
from core.tail_candidate_diagnostics_v256 import format_tail_diagnosis_report


def main():
    trade_date = dt.date.today().isoformat()
    report = preview_final_signals_from_watchlist_v255(
        watchlist_path=ROOT / "data" / "watchlist.parquet",
        duckdb_path=ROOT / "data" / "stock_data.duckdb",
        xgb_cache_root=ROOT / "data" / "xgb",
        trade_date=trade_date,
        limit=120,
        allow_xgb_fetch=False,
    )
    print(format_tail_diagnosis_report(report["results"]))
    print("")
    print("解释：")
    print("- risk_too_high 多：说明止损距离太远，不应该放松 risk_pct<=8。")
    print("- price_not_triggered 多：说明观察池可以留，但尾盘不能买。")
    print("- sector_missing/yuanjun_missing 多：说明需要先获取选股宝 core_pools。")


if __name__ == "__main__":
    main()
