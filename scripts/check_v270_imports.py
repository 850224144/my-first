#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main():
    from core.scheduler_tail_jobs_v270 import (
        ensure_job_table_v270,
        already_ran_v270,
        record_job_v270,
        is_weekend_v270,
    )

    with tempfile.TemporaryDirectory() as d:
        ensure_job_table_v270(d)
        assert already_ran_v270(d, "2026-04-30", "job") is False
        record_job_v270(
            d,
            trade_date="2026-04-30",
            job_name="job",
            status="success",
            started_at="s",
            finished_at="f",
            result={"ok": True},
        )
        assert already_ran_v270(d, "2026-04-30", "job") is True

    assert is_weekend_v270("2026-05-02") is True

    print("v2.7.0 imports OK")
    print("scheduler state table OK")
    print("job guard OK")

if __name__ == "__main__":
    main()
