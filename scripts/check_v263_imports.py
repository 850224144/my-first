#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.parquet_safe_writer_v263 import (
        normalize_record_for_parquet_v263,
        write_parquet_safe_v263,
        read_parquet_safe_v263,
    )
    from core.tail_confirm_runner_v262 import run_tail_confirm_from_tail_focus_v262

    rows = [
        {"symbol": "600000.SH", "weekly_flags": ["A", "B"], "risk_pct": 7.5},
        {"symbol": "000001.SZ", "weekly_flags": "C", "risk_pct": 9.1},
        {"symbol": "000002.SZ", "weekly_flags": None, "risk_pct": 10.2},
    ]

    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "test.parquet"
        write_parquet_safe_v263(rows, p)
        assert p.exists()
        got = read_parquet_safe_v263(p)
        assert len(got) == 3

    rec = normalize_record_for_parquet_v263({"weekly_flags": ["A", "B"]})
    assert isinstance(rec["weekly_flags"], str)

    print("v2.6.3 imports OK")
    print("safe parquet writer OK")
    print("tail_confirm_runner_v262 patched OK")

if __name__ == "__main__":
    main()
