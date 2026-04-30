#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.xgb_probe_v257 import probe_xgb_pool_variants, summarize_probe
    from core.sector_hot_fallback_v257 import inspect_sector_hot, apply_sector_hot_fallback
    from core.watchlist_pipeline_v257 import preview_watchlist_with_sector_fallback_v257

    c = {"symbol": "600000.SH", "sector_score": 50, "risk_flags": []}
    out = apply_sector_hot_fallback(c, path=ROOT / "data" / "sector_hot.parquet")
    assert "sector_score" in out

    print("v2.5.7 imports OK")
    print("xgb probe import OK")
    print("sector_hot fallback import OK")

if __name__ == "__main__":
    main()
