#!/usr/bin/env python3
"""
执行 v2.5.0 SQLite 迁移。

用法：
python scripts/run_v250_migration.py path/to/your.db
"""

from pathlib import Path
import sqlite3
import sys


def main():
    if len(sys.argv) < 2:
        print("用法：python scripts/run_v250_migration.py path/to/your.db")
        raise SystemExit(1)

    db_path = Path(sys.argv[1])
    sql_path = Path(__file__).resolve().parents[1] / "migrations" / "v2_5_0_sqlite.sql"

    if not sql_path.exists():
        raise FileNotFoundError(sql_path)

    sql = sql_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(sql)
        conn.commit()

    print(f"v2.5.0 migration OK: {db_path}")


if __name__ == "__main__":
    main()
