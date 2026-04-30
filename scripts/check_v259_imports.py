#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.xgb_pool_enricher_v259 import clean_stale_missing_flags, enrich_candidate_with_xgb_pools_v259
    from core.tail_candidate_diagnostics_v259 import diagnose_tail_candidates_v259
    from core.watchlist_pipeline_v259 import preview_watchlist_with_xgb_clean_v259

    c = {
        "symbol": "600000.SH",
        "risk_flags": ["板块数据缺失(sector_data_missing)", "风险比例过高(risk_pct_too_high)"],
    }
    cleaned = clean_stale_missing_flags(c)
    assert "sector_data_missing" not in str(cleaned.get("risk_flags"))
    assert "risk_pct_too_high" in str(cleaned.get("risk_flags"))

    pools = {
        "limit_up": [{
            "stock_chi_name": "测试股",
            "limit_up_days": 2,
            "m_days_n_boards_boards": 2,
            "change_percent": 0.1,
            "surge_reason": {
                "symbol": "600000.SH",
                "related_plates": [{"plate_name": "测试题材"}],
            },
        }],
        "strong_stock": [],
        "continuous_limit_up": [],
        "yesterday_limit_up": [],
        "limit_up_broken": [],
        "limit_down": [],
    }
    out = enrich_candidate_with_xgb_pools_v259(c, pools)
    assert out["xgb_pool_matched"] is True
    assert "sector_data_missing" not in str(out.get("risk_flags"))

    d = diagnose_tail_candidates_v259([out])
    assert d["xgb_matched_count"] == 1

    print("v2.5.9 imports OK")
    print("stale missing flag cleanup OK")
    print("xgb enrich v259 OK")
    print("tail diagnosis v259 OK")

if __name__ == "__main__":
    main()
