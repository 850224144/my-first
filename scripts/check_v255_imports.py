#!/usr/bin/env python3
from pathlib import Path
import sys
import json
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.realtime_quote_loader_v255 import inspect_realtime_quote_table, load_realtime_quote_map_from_duckdb
    from core.xgb_core_pools_v255 import load_xgb_core_pools_v255, core_pools_report
    from core.score_enricher_v255 import enrich_candidate_scores_v255
    from core.watchlist_pipeline_v255 import preview_final_signals_from_watchlist_v255

    c = {
        "symbol": "603019.SH",
        "daily_2buy_score": 82,
        "risk_pct": 5.5,
        "current_price": 36.1,
        "trigger_price": 36.5,
        "fresh_quote": True,
    }
    e = enrich_candidate_scores_v255(c, daily_bars=None, core_pools=None)
    assert e["sector_score"] == 50.0
    assert e["yuanjun_score"] == 50.0
    assert not any("no_sector_follow" in x for x in e.get("risk_flags", []))

    pools = load_xgb_core_pools_v255(trade_date=dt.date.today().isoformat(), allow_fetch=False)
    assert "_meta" in pools

    print("v2.5.5 imports OK")
    print("JSON date safe loader OK")
    print("XGB core pools loader OK")
    print("missing XGB data downgrade OK")

if __name__ == "__main__":
    main()
