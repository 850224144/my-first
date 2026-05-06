#!/usr/bin/env python3
"""
检查项目真实数据文件。
只读，不修改。
"""

from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.project_data_adapter_v252 import inspect_project_real_data


def main():
    report = inspect_project_real_data(ROOT)

    print("【v2.5.2 真实数据检查】")
    print(f"项目目录: {report['root']}")
    print("")

    print("文件状态:")
    for name, info in report["file_status"].items():
        print(f"- {name}: exists={info['exists']} size={info['size']} path={info['path']}")

    print("")
    for key in ["watchlist", "trade_plan", "positions"]:
        info = report[key]
        print(f"{key}:")
        if not info.get("exists"):
            print("- 不存在")
            continue
        print(f"- rows: {info.get('rows')}")
        print(f"- columns: {info.get('columns')}")
        print("")

    print("trading_state:")
    db = report["trading_state"]
    if not db.get("exists"):
        print("- 不存在")
    else:
        print(f"- tables: {db.get('tables')}")
        print(f"- counts: {db.get('counts')}")
        print(f"- demo_hits: {db.get('demo_hits')}")

    print("")
    print("watchlist 接入字段覆盖:")
    print(json.dumps(report["watchlist_candidate_field_report"], ensure_ascii=False, indent=2))

    missing = report["watchlist_candidate_field_report"].get("missing_fields") or []
    if missing:
        print("")
        print("提示：watchlist 当前缺少部分 v2.5.1 接入字段。")
        print("这不一定是错误，说明 tail_confirm 前还需要补齐评分字段：")
        for f in missing:
            print(f"- {f}")

    print("")
    print("检查完成。")


if __name__ == "__main__":
    main()
