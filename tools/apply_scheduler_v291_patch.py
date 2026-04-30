#!/usr/bin/env python3
from pathlib import Path
import sys
import datetime as dt
import re

def backup(path: Path) -> Path:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = path.with_name(f"{path.name}.bak_v291_{ts}")
    bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return bak

def find_line_after_key(text: str, key: str) -> re.Match:
    patterns = [
        rf'(\s*"{re.escape(key)}"\s*:\s*\[[^\n]*\],\n)',
        rf"(\s*'{re.escape(key)}'\s*:\s*\[[^\n]*\],\n)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m
    raise RuntimeError(f"找不到 COMMANDS 项：{key}")

def insert_command_after(text: str, after_key: str, new_key: str, line: str) -> str:
    if f'"{new_key}"' in text or f"'{new_key}'" in text:
        return text
    m = find_line_after_key(text, after_key)
    return text[:m.end()] + line + "\n" + text[m.end():]

def replace_tail_command(text: str) -> str:
    old = '"tail_confirm": [["run_scan.py", "--mode", "tail_confirm", "--workers", "1"]],'
    new = '"tail_confirm": [["scripts/run_v270_jobs_once.py", "--job", "tail"]],'
    if old in text:
        return text.replace(old, new)

    old2 = "'tail_confirm': [['run_scan.py', '--mode', 'tail_confirm', '--workers', '1']],"
    new2 = "'tail_confirm': [['scripts/run_v270_jobs_once.py', '--job', 'tail']],"
    if old2 in text:
        return text.replace(old2, new2)

    if '"tail_confirm": [["scripts/run_v270_jobs_once.py", "--job", "tail"]]' in text:
        return text

    raise RuntimeError("没有找到旧 tail_confirm 命令，未修改。")

def insert_after_line(text: str, anchor: str, line: str) -> str:
    if line in text:
        return text
    idx = text.find(anchor)
    if idx < 0:
        raise RuntimeError(f"找不到锚点：{anchor}")
    end = text.find("\n", idx)
    return text[:end+1] + line + "\n" + text[end+1:]

def insert_before_line(text: str, anchor: str, line: str) -> str:
    if line in text:
        return text
    idx = text.find(anchor)
    if idx < 0:
        raise RuntimeError(f"找不到锚点：{anchor}")
    return text[:idx] + line + "\n" + text[idx:]

def main():
    root = Path(sys.argv[1]).resolve()
    target = root / "run_scheduler.py"
    if not target.exists():
        raise FileNotFoundError(target)

    s = target.read_text(encoding="utf-8")
    bak = backup(target)

    s = replace_tail_command(s)

    s = insert_command_after(
        s, "tail_confirm", "observe_gate_v270",
        '    "observe_gate_v270": [["scripts/run_v270_jobs_once.py", "--job", "observe"]],'
    )
    s = insert_command_after(
        s, "tail_confirm", "buy_bridge_v280",
        '    "buy_bridge_v280": [["scripts/build_buy_bridge_v280.py"]],'
    )
    s = insert_command_after(
        s, "daily_report", "daily_report_v290_build",
        '    "daily_report_v290_build": [["scripts/build_daily_report_v290.py"]],'
    )

    s = insert_after_line(
        s,
        '    add_job(scheduler, "watchlist_refresh_1420", 14, 20, misfire_grace_time=1800)',
        '    add_job(scheduler, "observe_gate_v270", 14, 40, misfire_grace_time=1800)'
    )
    s = insert_after_line(
        s,
        '    add_job(scheduler, "tail_confirm", 14, 50, misfire_grace_time=1800)',
        '    add_job(scheduler, "buy_bridge_v280", 14, 52, misfire_grace_time=1800)'
    )
    s = insert_before_line(
        s,
        '    add_job(scheduler, "daily_report", 20, 30, misfire_grace_time=3600)',
        '    add_job(scheduler, "daily_report_v290_build", 20, 25, misfire_grace_time=3600)'
    )

    # 补打印，不强求
    if 'print("- 14:40 observe_gate_v270")' not in s and 'print("- 14:50 tail_confirm")' in s:
        s = s.replace(
            '    print("- 14:50 tail_confirm")',
            '    print("- 14:40 observe_gate_v270")\n    print("- 14:50 tail_confirm")\n    print("- 14:52 buy_bridge_v280")'
        )
    if 'print("- 20:25 daily_report_v290_build")' not in s and 'print("- 20:30 daily_report")' in s:
        s = s.replace(
            '    print("- 20:30 daily_report")',
            '    print("- 20:25 daily_report_v290_build")\n    print("- 20:30 daily_report")'
        )

    target.write_text(s, encoding="utf-8")
    print(f"已备份: {bak}")
    print(f"已修补: {target}")
    print("未修改 core/run_scheduler.py / notify.py / 企业微信配置。")

if __name__ == "__main__":
    main()
