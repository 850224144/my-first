# tools/apply_planS_patch.py
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def patch_text(path: Path, transform):
    text = path.read_text(encoding="utf-8")
    new = transform(text)
    if new != text:
        path.write_text(new, encoding="utf-8")
        print(f"patched: {path}")
    else:
        print(f"unchanged: {path}")


def patch_run_scan():
    path = ROOT / "run_scan.py"
    def tr(s: str) -> str:
        if "from core.paper_trader import process_scan_results_for_paper" not in s:
            s = s.replace("from core.alert import push_results", "from core.alert import push_results\nfrom core.paper_trader import process_scan_results_for_paper")
        # 在 run_scan 返回 results 前或 main push 前都可能可用；优先在 run_scan 生命周期持久化之后、return results 前插入
        marker = "    return results\n\n\ndef print_market_state"
        inject = """    # ===== 方案S：纸面交易买入触发/T+1账本 =====\n    try:\n        process_scan_results_for_paper(results, market_state=market_state, mode=mode)\n    except Exception as e:\n        logger.warning(f\"纸面交易触发处理失败：{e}\", exc_info=True)\n\n    return results\n\n\ndef print_market_state"""
        if "process_scan_results_for_paper(results, market_state=market_state, mode=mode)" not in s and marker in s:
            s = s.replace(marker, inject)
        # daily_report 增加纸面交易摘要，保守替换 main 中 daily_report 分支
        if "from core.paper_trader import generate_paper_report" not in s:
            s = s.replace("from core.paper_trader import process_scan_results_for_paper", "from core.paper_trader import process_scan_results_for_paper, generate_paper_report")
        old = """    if args.daily_report:\n        content = generate_daily_report()\n        print(content)\n        return"""
        new = """    if args.daily_report:\n        content = generate_daily_report()\n        print(content)\n        try:\n            paper_content = generate_paper_report()\n            print(\"\\n\" + paper_content)\n        except Exception as e:\n            logger.warning(f\"生成纸面交易报告失败：{e}\", exc_info=True)\n        return"""
        if old in s and "生成纸面交易报告失败" not in s:
            s = s.replace(old, new)
        return s
    patch_text(path, tr)


def patch_run_scheduler():
    path = ROOT / "run_scheduler.py"
    def tr(s: str) -> str:
        # 添加任务命令
        if '"paper_track_midday"' not in s:
            s = s.replace('"track_positions_midday": [\n        ["run_positions.py", "--track"],\n    ],', '"track_positions_midday": [\n        ["run_positions.py", "--track"],\n    ],\n    "paper_track_midday": [\n        ["run_paper.py", "--track"],\n    ],')
            s = s.replace('"track_positions_tail": [\n        ["run_positions.py", "--track"],\n    ],', '"track_positions_tail": [\n        ["run_positions.py", "--track"],\n    ],\n    "paper_track_tail": [\n        ["run_paper.py", "--track"],\n    ],')
            s = s.replace('"track_positions_evening": [\n        ["run_positions.py", "--track"],\n    ],', '"track_positions_evening": [\n        ["run_positions.py", "--track"],\n    ],\n    "paper_track_evening": [\n        ["run_paper.py", "--track"],\n    ],')
        if '"paper_track_midday": 5 * 60' not in s:
            s = s.replace('"track_positions_midday": 5 * 60,', '"track_positions_midday": 5 * 60,\n    "paper_track_midday": 5 * 60,')
            s = s.replace('"track_positions_tail": 5 * 60,', '"track_positions_tail": 5 * 60,\n    "paper_track_tail": 5 * 60,')
            s = s.replace('"track_positions_evening": 5 * 60,', '"track_positions_evening": 5 * 60,\n    "paper_track_evening": 5 * 60,')
        # 添加 job_heartbeat 函数
        if "def job_heartbeat(" not in s:
            anchor = "def heartbeat():"
            idx = s.find(anchor)
            if idx != -1:
                # 插在 heartbeat 函数后比较麻烦，用下一个 def add_job 或 def build_scheduler 前插入
                insert_before = s.find("def add_job(")
                if insert_before == -1:
                    insert_before = s.find("def build_scheduler(")
                func = '''\n\ndef job_heartbeat(job_name: str, stage: str = "before"):\n    """任务级心跳：任务执行前1分钟/开始前写入并打印，证明调度器正常触发。"""\n    ensure_dirs()\n    try:\n        pid = os.getpid()\n        lock_pid = _read_scheduler_lock()\n    except Exception:\n        pid = os.getpid()\n        lock_pid = ""\n    line = f"[{now_str()}] job_heartbeat stage={stage} job={job_name} pid={pid} lock_pid={lock_pid}"\n    try:\n        with open(HEARTBEAT_LOG, "a", encoding="utf-8") as f:\n            f.write(line + "\\n")\n    except Exception:\n        pass\n    print(line)\n'''
                if insert_before != -1:
                    s = s[:insert_before] + func + "\n" + s[insert_before:]
        # run_job 中添加任务前心跳
        if "job_heartbeat(job_name, stage=\"before\")" not in s:
            s = s.replace("with job_lock(job_name):", "with job_lock(job_name):\n            job_heartbeat(job_name, stage=\"before\")")
        # 60分钟心跳：已有 heartbeat_trading 是 hour 9-15 minute 5，保持；没有就提示不处理
        # build_scheduler 添加纸面任务
        if 'add_job(scheduler, "paper_track_midday"' not in s:
            s = s.replace('add_job(scheduler, "track_positions_midday", 11, 30, misfire_grace_time=1800)', 'add_job(scheduler, "track_positions_midday", 11, 30, misfire_grace_time=1800)\n    add_job(scheduler, "paper_track_midday", 11, 31, misfire_grace_time=1800)')
            s = s.replace('add_job(scheduler, "track_positions_tail", 14, 55, misfire_grace_time=1800)', 'add_job(scheduler, "track_positions_tail", 14, 55, misfire_grace_time=1800)\n    add_job(scheduler, "paper_track_tail", 14, 56, misfire_grace_time=1800)')
            s = s.replace('add_job(scheduler, "track_positions_evening", 20, 0, misfire_grace_time=3600)', 'add_job(scheduler, "track_positions_evening", 20, 0, misfire_grace_time=3600)\n    add_job(scheduler, "paper_track_evening", 20, 1, misfire_grace_time=3600)')
        # print_jobs加展示
        if "11:31 paper_track_midday" not in s:
            s = s.replace('print("- 11:30 track_positions_midday")', 'print("- 11:30 track_positions_midday")\n    print("- 11:31 paper_track_midday")')
            s = s.replace('print("- 14:55 track_positions_tail")', 'print("- 14:55 track_positions_tail")\n    print("- 14:56 paper_track_tail")')
            s = s.replace('print("- 20:00 track_positions_evening")', 'print("- 20:00 track_positions_evening")\n    print("- 20:01 paper_track_evening")')
        return s
    patch_text(path, tr)


def main():
    patch_run_scan()
    patch_run_scheduler()
    print("方案S补丁应用完成。")


if __name__ == "__main__":
    main()
