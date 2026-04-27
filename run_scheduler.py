# run_scheduler.py
from __future__ import annotations

import argparse
import atexit
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    print("缺少 APScheduler，请先执行：pip install apscheduler")
    raise


PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

LOG_DIR = PROJECT_ROOT / "logs" / "scheduler"
STATE_DIR = PROJECT_ROOT / "data" / "scheduler_state"

TIMEZONE = "Asia/Shanghai"

SCHEDULER_LOCK_FILE = STATE_DIR / "scheduler.lock"


JOB_COMMANDS: Dict[str, List[List[str]]] = {
    "preflight": [
        ["run_scan.py", "--coverage"],
    ],

    "observe_morning": [
        ["run_scan.py", "--mode", "observe", "--workers", "1"],
    ],

    "observe_midday": [
        ["run_scan.py", "--mode", "observe", "--workers", "1"],
    ],

    "tail_confirm": [
        ["run_scan.py", "--mode", "tail_confirm", "--workers", "1"],
    ],

    "after_close": [
        ["run_scan.py", "--build-daily-cache", "--daily-limit", "300", "--daily-workers", "1"],
        ["run_scan.py", "--build-universe", "--workers", "1"],
        ["run_scan.py", "--mode", "after_close", "--workers", "1"],
    ],

    "daily_report": [
        ["run_scan.py", "--daily-report"],
    ],

    "night_cache_expand": [
        ["run_scan.py", "--build-daily-cache", "--daily-limit", "300", "--daily-workers", "1"],
    ],

    # 方案 M：持仓跟踪
    "track_positions_morning": [
        ["run_positions.py", "--track"],
    ],
    "track_positions_midday": [
        ["run_positions.py", "--track"],
    ],
    "track_positions_tail": [
        ["run_positions.py", "--track"],
    ],
    "track_positions_evening": [
        ["run_positions.py", "--track"],
    ],
}


JOB_TIMEOUTS = {
    "preflight": 5 * 60,
    "observe_morning": 15 * 60,
    "observe_midday": 15 * 60,
    "tail_confirm": 10 * 60,
    "after_close": 60 * 60,
    "daily_report": 10 * 60,
    "night_cache_expand": 60 * 60,
    "track_positions_morning": 5 * 60,
    "track_positions_midday": 5 * 60,
    "track_positions_tail": 5 * 60,
    "track_positions_evening": 5 * 60,
}


def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def date_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def log_path(job_name: str) -> Path:
    return LOG_DIR / f"{date_str()}_{job_name}.log"


def write_line(path: Path, text: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _read_scheduler_lock() -> Optional[int]:
    if not SCHEDULER_LOCK_FILE.exists():
        return None

    try:
        text = SCHEDULER_LOCK_FILE.read_text(encoding="utf-8")
        for part in text.replace("\n", " ").split():
            if part.startswith("pid="):
                return int(part.replace("pid=", "").strip())
    except Exception:
        return None

    return None


def _remove_scheduler_lock():
    try:
        if SCHEDULER_LOCK_FILE.exists():
            SCHEDULER_LOCK_FILE.unlink()
    except Exception:
        pass


def _kill_pid(pid: int, wait_seconds: int = 5):
    if not _pid_alive(pid):
        return

    print(f"[{now_str()}] 正在停止旧调度器 pid={pid} ...")

    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass

    start = time.time()
    while time.time() - start < wait_seconds:
        if not _pid_alive(pid):
            print(f"[{now_str()}] 旧调度器已退出 pid={pid}")
            return
        time.sleep(0.5)

    if _pid_alive(pid):
        print(f"[{now_str()}] 旧调度器未退出，强制 kill -9 pid={pid}")
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def acquire_scheduler_singleton(replace: bool = False):
    """
    调度器级别单例锁。

    默认行为：
    - 如果已有调度器进程存活，新进程直接退出。
    --replace：
    - 先停止旧调度器，再启动当前调度器。
    """
    ensure_dirs()

    old_pid = _read_scheduler_lock()

    if old_pid and _pid_alive(old_pid):
        if old_pid == os.getpid():
            return

        if replace:
            _kill_pid(old_pid)
            _remove_scheduler_lock()
        else:
            print("=" * 80)
            print("已有 run_scheduler.py 正在运行，本次启动自动退出。")
            print(f"旧进程 PID：{old_pid}")
            print("")
            print("查看进程：")
            print("ps aux | grep run_scheduler.py | grep -v grep")
            print("")
            print("如需重启调度器，请执行：")
            print("python run_scheduler.py --replace")
            print("=" * 80)
            sys.exit(0)

    # 如果 lock 残留但 pid 不存在，清理
    if old_pid and not _pid_alive(old_pid):
        _remove_scheduler_lock()

    try:
        fd = os.open(str(SCHEDULER_LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(
            fd,
            f"pid={os.getpid()} started_at={now_str()} project={PROJECT_ROOT}\n".encode("utf-8"),
        )
        os.close(fd)
    except FileExistsError:
        # 极小概率并发启动
        pid = _read_scheduler_lock()
        if pid and _pid_alive(pid):
            print(f"已有调度器正在运行 pid={pid}，本次启动退出。")
            sys.exit(0)
        _remove_scheduler_lock()
        return acquire_scheduler_singleton(replace=replace)

    atexit.register(_remove_scheduler_lock)


def _handle_exit_signal(signum, frame):
    print(f"[{now_str()}] 收到退出信号 {signum}，清理 scheduler.lock")
    _remove_scheduler_lock()
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_exit_signal)
signal.signal(signal.SIGINT, _handle_exit_signal)


@contextmanager
def job_lock(job_name: str, stale_seconds: int = 7200):
    """
    任务级别锁，防止同一个任务重叠执行。
    """
    ensure_dirs()

    lock_file = STATE_DIR / f"{job_name}.lock"

    if lock_file.exists():
        age = time.time() - lock_file.stat().st_mtime

        if age < stale_seconds:
            raise RuntimeError(f"任务 {job_name} 正在运行，跳过本次触发。lock={lock_file}")

        try:
            lock_file.unlink()
        except Exception:
            raise RuntimeError(f"任务 {job_name} 存在过期锁但无法删除：{lock_file}")

    fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)

    try:
        os.write(fd, f"pid={os.getpid()} started_at={now_str()}\n".encode("utf-8"))
        yield
    finally:
        try:
            os.close(fd)
        except Exception:
            pass

        try:
            lock_file.unlink()
        except Exception:
            pass


def run_one_command(job_name: str, cmd: List[str], timeout: Optional[int] = None) -> int:
    ensure_dirs()

    path = log_path(job_name)
    full_cmd = [PYTHON] + cmd

    write_line(path, "")
    write_line(path, "=" * 80)
    write_line(path, f"[{now_str()}] START COMMAND: {' '.join(full_cmd)}")
    write_line(path, f"[{now_str()}] TIMEOUT: {timeout}")
    write_line(path, "=" * 80)

    print(f"[{now_str()}] {job_name} -> {' '.join(full_cmd)}")

    try:
        with open(path, "a", encoding="utf-8") as f:
            proc = subprocess.run(
                full_cmd,
                cwd=str(PROJECT_ROOT),
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout,
            )

        write_line(path, f"[{now_str()}] END COMMAND returncode={proc.returncode}")
        return proc.returncode

    except subprocess.TimeoutExpired:
        write_line(path, f"[{now_str()}] COMMAND TIMEOUT, killed: {' '.join(full_cmd)}")
        print(f"[{now_str()}] 任务超时，已终止子任务：{job_name}")
        return 124

    except Exception as e:
        write_line(path, f"[{now_str()}] COMMAND ERROR: {e}")
        print(f"[{now_str()}] 任务异常：{job_name} | {e}")
        return 1


def run_job(job_name: str):
    ensure_dirs()

    if job_name not in JOB_COMMANDS:
        print(f"未知任务：{job_name}")
        return

    path = log_path(job_name)

    try:
        with job_lock(job_name):
            write_line(path, "")
            write_line(path, "#" * 80)
            write_line(path, f"[{now_str()}] JOB START: {job_name}")
            write_line(path, "#" * 80)

            commands = JOB_COMMANDS[job_name]
            timeout = JOB_TIMEOUTS.get(job_name)

            for cmd in commands:
                rc = run_one_command(job_name, cmd, timeout=timeout)

                if rc != 0:
                    write_line(path, f"[{now_str()}] JOB FAILED: {job_name}, command={cmd}, rc={rc}")
                    print(f"[{now_str()}] 任务失败：{job_name}, rc={rc}")
                    return

            write_line(path, f"[{now_str()}] JOB DONE: {job_name}")
            print(f"[{now_str()}] 任务完成：{job_name}")

    except Exception as e:
        write_line(path, f"[{now_str()}] JOB SKIPPED/ERROR: {job_name} | {e}")
        print(f"[{now_str()}] 任务跳过/异常：{job_name} | {e}")


def add_job(
    scheduler: BlockingScheduler,
    job_name: str,
    hour: int,
    minute: int,
    misfire_grace_time: int = 600,
):
    scheduler.add_job(
        run_job,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=hour,
            minute=minute,
            timezone=TIMEZONE,
        ),
        args=[job_name],
        id=job_name,
        name=job_name,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=misfire_grace_time,
    )


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    add_job(scheduler, "preflight", 9, 15, misfire_grace_time=900)
    add_job(scheduler, "observe_morning", 9, 35, misfire_grace_time=900)
    add_job(scheduler, "track_positions_morning", 9, 45, misfire_grace_time=900)

    add_job(scheduler, "observe_midday", 11, 25, misfire_grace_time=900)
    add_job(scheduler, "track_positions_midday", 11, 30, misfire_grace_time=900)

    add_job(scheduler, "tail_confirm", 14, 50, misfire_grace_time=900)
    add_job(scheduler, "track_positions_tail", 14, 55, misfire_grace_time=900)

    add_job(scheduler, "after_close", 17, 30, misfire_grace_time=3600)

    add_job(scheduler, "track_positions_evening", 20, 0, misfire_grace_time=3600)
    add_job(scheduler, "daily_report", 20, 30, misfire_grace_time=3600)

    add_job(scheduler, "night_cache_expand", 22, 30, misfire_grace_time=3600)

    return scheduler


def print_jobs():
    print("可用任务：")
    for name, commands in JOB_COMMANDS.items():
        print(f"- {name}")
        for cmd in commands:
            print(f"  {PYTHON} {' '.join(cmd)}")

    print("")
    print("默认调度时间：")
    print("- 09:15 preflight")
    print("- 09:35 observe_morning")
    print("- 09:45 track_positions_morning")
    print("- 11:25 observe_midday")
    print("- 11:30 track_positions_midday")
    print("- 14:50 tail_confirm")
    print("- 14:55 track_positions_tail")
    print("- 17:30 after_close")
    print("- 20:00 track_positions_evening")
    print("- 20:30 daily_report")
    print("- 22:30 night_cache_expand")


def parse_args():
    parser = argparse.ArgumentParser(description="A股二买系统 APScheduler 自动调度器")

    parser.add_argument(
        "--run-once",
        choices=list(JOB_COMMANDS.keys()),
        help="立即执行某个任务一次，用于测试",
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="列出任务与命令",
    )

    parser.add_argument(
        "--replace",
        action="store_true",
        help="如果已有调度器在运行，先停止旧调度器，再启动当前调度器",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dirs()

    if args.list:
        print_jobs()
        return

    if args.run_once:
        run_job(args.run_once)
        return

    acquire_scheduler_singleton(replace=args.replace)

    print("=" * 80)
    print("A股二买系统自动调度器已启动")
    print(f"项目目录：{PROJECT_ROOT}")
    print(f"Python：{PYTHON}")
    print(f"时区：{TIMEZONE}")
    print(f"日志目录：{LOG_DIR}")
    print(f"单例锁：{SCHEDULER_LOCK_FILE}")
    print(f"PID：{os.getpid()}")
    print("=" * 80)
    print_jobs()
    print("=" * 80)
    print("按 Ctrl+C 停止调度器")
    print("=" * 80)

    scheduler = build_scheduler()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("收到 Ctrl+C，调度器已停止。")
    except SystemExit:
        print("调度器退出。")
    finally:
        _remove_scheduler_lock()


if __name__ == "__main__":
    main()