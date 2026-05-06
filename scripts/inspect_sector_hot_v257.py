#!/usr/bin/env python3
from pathlib import Path
import sys
import json
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.sector_hot_fallback_v257 import inspect_sector_hot

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)

def main():
    path = ROOT / "data" / "sector_hot.parquet"
    info = inspect_sector_hot(path)
    print("【sector_hot.parquet 检查】")
    print(dumps(info))

if __name__ == "__main__":
    main()
