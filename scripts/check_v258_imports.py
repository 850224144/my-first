#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.xgb_live_pools_v258 import fetch_xgb_live_pool, live_pools_report
    from core.xgb_pool_enricher_v258 import build_xgb_symbol_index, enrich_candidate_with_xgb_pools_v258
    from core.watchlist_pipeline_v258 import preview_watchlist_with_xgb_live_v258

    sample_pools = {
        "limit_up": [{
            "stock_chi_name": "测试股",
            "limit_up_days": 2,
            "m_days_n_boards_boards": 2,
            "change_percent": 0.1,
            "surge_reason": {
                "symbol": "600000.SH",
                "stock_reason": "测试原因",
                "related_plates": [{"plate_name": "测试题材"}],
            },
        }],
        "strong_stock": [],
        "continuous_limit_up": [],
        "yesterday_limit_up": [],
        "limit_up_broken": [],
        "limit_down": [],
    }
    idx = build_xgb_symbol_index(sample_pools)
    assert "600000.SH" in idx
    c = {"symbol": "600000.SH", "sector_score": 50, "leader_score": 50, "yuanjun_score": 50}
    out = enrich_candidate_with_xgb_pools_v258(c, sample_pools)
    assert out["xgb_pool_matched"] is True
    assert out["sector_score"] > 50

    print("v2.5.8 imports OK")
    print("xgb live pool parser OK")
    print("xgb candidate enricher OK")

if __name__ == "__main__":
    main()
