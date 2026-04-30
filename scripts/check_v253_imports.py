#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.watchlist_candidate_v253 import build_candidate_from_watchlist_row
    from core.duckdb_daily_loader_v253 import inspect_duckdb
    from core.score_enricher_v253 import enrich_candidate_scores_v253
    from core.watchlist_pipeline_v253 import build_candidates_from_watchlist_v253

    row = {
        "code": "603019",
        "name": "中科曙光",
        "total_score": 82,
        "risk_pct": 5.5,
        "entry_price": 36.1,
        "trigger_price": 36.5,
        "stop_loss": 34.5,
        "take_profit_1": 40,
        "take_profit_2": 45,
    }
    c = build_candidate_from_watchlist_row(row)
    assert c["symbol"] == "603019.SH"
    assert c["daily_2buy_score"] == 82
    assert c["fresh_quote"] is False

    e = enrich_candidate_scores_v253(c, daily_bars=None, core_pools=None)
    assert "weekly_score" in e
    assert "sector_score" in e
    assert "yuanjun_score" in e

    print("v2.5.3 imports OK")
    print("watchlist candidate mapping OK")
    print("safe default score enrichment OK")

if __name__ == "__main__":
    main()
