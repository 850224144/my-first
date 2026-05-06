#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.realtime_quote_loader_v254 import inspect_realtime_quote_table, load_realtime_quote_map_from_duckdb
    from core.score_enricher_v254 import enrich_candidate_scores_v254
    from core.watchlist_pipeline_v254 import preview_final_signals_from_watchlist_v254

    c = {
        "symbol": "603019.SH",
        "daily_2buy_score": 82,
        "risk_pct": 5.5,
        "current_price": 36.1,
        "trigger_price": 36.5,
        "fresh_quote": True,
    }
    e = enrich_candidate_scores_v254(c, daily_bars=None, core_pools=None)
    assert e["sector_score"] == 50.0
    assert e["yuanjun_score"] == 50.0
    assert not any("no_sector_follow" in x for x in e.get("risk_flags", []))

    print("v2.5.4 imports OK")
    print("realtime quote loader import OK")
    print("yuanjun missing-data downgrade OK")

if __name__ == "__main__":
    main()
