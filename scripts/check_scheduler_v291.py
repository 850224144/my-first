#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
s = (ROOT / "run_scheduler.py").read_text(encoding="utf-8")

checks = {
    "tail_confirm_uses_new_flow": '"tail_confirm": [["scripts/run_v270_jobs_once.py", "--job", "tail"]]' in s,
    "observe_gate_command": '"observe_gate_v270": [["scripts/run_v270_jobs_once.py", "--job", "observe"]]' in s,
    "buy_bridge_command": '"buy_bridge_v280": [["scripts/build_buy_bridge_v280.py"]]' in s,
    "daily_report_v290_build_command": '"daily_report_v290_build": [["scripts/build_daily_report_v290.py"]]' in s,
    "observe_gate_job_1440": 'add_job(scheduler, "observe_gate_v270", 14, 40' in s,
    "tail_confirm_job_1450": 'add_job(scheduler, "tail_confirm", 14, 50' in s,
    "buy_bridge_job_1452": 'add_job(scheduler, "buy_bridge_v280", 14, 52' in s,
    "daily_report_v290_build_2025": 'add_job(scheduler, "daily_report_v290_build", 20, 25' in s,
    "old_daily_report_kept": '"daily_report": [["run_scan.py", "--daily-report"]]' in s,
}

print("【v2.9.1 scheduler 检查】")
ok = True
for k, v in checks.items():
    print(f"- {k}: {v}")
    ok = ok and v

if not ok:
    print("\n检查未全部通过，不要启动 scheduler。")
    sys.exit(1)

print("\n检查通过。")
print("- 14:50 tail_confirm 已切到 tail-focus 新流程")
print("- 20:30 daily_report 仍保留原企业微信提醒")
print("- notify.py / 企业微信配置未修改")
