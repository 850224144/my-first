#!/usr/bin/env python3
"""
清理开发/增量包残留。
默认只打印建议，不直接删除。
加 --apply 才会移动/删除。

用法：
python scripts/cleanup_dev_artifacts_v252.py
python scripts/cleanup_dev_artifacts_v252.py --apply
"""

from pathlib import Path
import sys
import shutil

ROOT = Path(__file__).resolve().parents[1]
APPLY = "--apply" in sys.argv

PACKAGE_NAMES = [
    "v2_4_0_yuanjun_increment",
    "v2_5_0_signal_open_increment",
    "v2_5_1_mainflow_safe_increment",
    "v2_5_2_realdata_guard_increment",
]

ZIP_NAMES = [x + ".zip" for x in PACKAGE_NAMES]


def main():
    backup = ROOT / "packages_backup"
    print(f"项目目录: {ROOT}")
    print(f"执行模式: {'apply' if APPLY else 'dry-run'}")
    print("")

    for name in PACKAGE_NAMES + ZIP_NAMES:
        p = ROOT / name
        if not p.exists():
            continue
        print(f"发现增量包残留: {p}")
        if APPLY:
            backup.mkdir(exist_ok=True)
            dst = backup / p.name
            if dst.exists():
                if dst.is_dir():
                    shutil.rmtree(dst)
                else:
                    dst.unlink()
            shutil.move(str(p), str(dst))
            print(f"已移动到: {dst}")

    pyc_dirs = list(ROOT.rglob("__pycache__"))
    pyc_files = list(ROOT.rglob("*.pyc"))
    print(f"发现 __pycache__ 目录: {len(pyc_dirs)}")
    print(f"发现 .pyc 文件: {len(pyc_files)}")

    if APPLY:
        for d in pyc_dirs:
            shutil.rmtree(d, ignore_errors=True)
        for f in pyc_files:
            try:
                f.unlink()
            except FileNotFoundError:
                pass
        print("已清理 __pycache__ 和 .pyc")

    if not APPLY:
        print("")
        print("未执行删除/移动。确认后可执行：")
        print("python scripts/cleanup_dev_artifacts_v252.py --apply")


if __name__ == "__main__":
    main()
