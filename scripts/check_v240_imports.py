#!/usr/bin/env python3
"""
v2.4.0 导入检查脚本。
运行：
python scripts/check_v240_imports.py
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.data_normalizer import normalize_symbol, normalize_amount_to_yuan
    from core.xgb_client import XGBClient
    from core.xgb_cache import XGBCache
    from core.sector import filter_universe_by_strong_sector, score_sector_for_stock
    from core.weekly import filter_by_weekly_trend, score_weekly_trend
    from core.yuanjun import score_yuanjun
    from core.signal_engine import build_observation_signal

    assert normalize_symbol("sh600519") == "600519.SH"
    assert normalize_symbol("000001") == "000001.SZ"
    assert normalize_amount_to_yuan(3.2, unit="wan_yuan") == 32000

    print("v2.4.0 imports OK")
    print("symbol normalize OK")
    print("unit normalize OK")
    print("legacy import functions OK")

if __name__ == "__main__":
    main()
