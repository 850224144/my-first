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
HEARTBEAT_LOG = PROJECT_ROOT / "logs" / "scheduler_heartbeat.log"
TIMEZONE = "Asia/Shanghai"
SCHEDULER_LOCK_FILE = STATE_DIR / "scheduler.lock"
SCHEDULER_CONFIG_PATH = PROJECT_ROOT / "config" / "scheduler_jobs.json"

# 你提供的企业微信 webhook。不要提交到公开仓库。
DEFAULT_WECHAT_WEBHOOK = os.getenv(
    "WECHAT_WEBHOOK",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b",
)

def load_scheduler_config() -> Dict[str, object]:
    default = {
        "timezone": TIMEZONE,
        "jobs": [
            {
                "name": "preflight",
                "schedule": {"kind": "cron", "expr": "15 9 * * 1-5"},
                "commands": [["run_scan.py", "--coverage"]],
                "timeoutSeconds": 300,
            },
            {
                "name": "observe_morning",
                "schedule": {"kind": "cron", "expr": "45 9 * * 1-5"},
                "commands": [["run_scan.py", "--mode", "observe", "--workers", "1"]],
                "timeoutSeconds": 1200,
            },
            {
                "name": "watchlist_refresh_1030",
                "schedule": {"kind": "cron", "expr": "30 10 * * 1-5"},
                "commands": [["run_scan.py", "--watchlist-refresh", "--workers", "1"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "watchlist_refresh_1120",
                "schedule": {"kind": "cron", "expr": "20 11 * * 1-5"},
                "commands": [["run_scan.py", "--watchlist-refresh", "--workers", "1"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "track_positions_midday",
                "schedule": {"kind": "cron", "expr": "30 11 * * 1-5"},
                "commands": [["run_positions.py", "--track"]],
                "timeoutSeconds": 300,
            },
            {
                "name": "observe_afternoon",
                "schedule": {"kind": "cron", "expr": "20 13 * * 1-5"},
                "commands": [["run_scan.py", "--mode", "observe", "--workers", "1"]],
                "timeoutSeconds": 1200,
            },
            {
                "name": "watchlist_refresh_1420",
                "schedule": {"kind": "cron", "expr": "20 14 * * 1-5"},
                "commands": [["run_scan.py", "--watchlist-refresh", "--workers", "1"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "observe_gate_v270",
                "schedule": {"kind": "cron", "expr": "40 14 * * 1-5"},
                "commands": [["scripts/run_v270_jobs_once.py", "--job", "observe"]],
                "timeoutSeconds": 1800,
            },
            {
                "name": "tail_confirm",
                "schedule": {"kind": "cron", "expr": "50 14 * * 1-5"},
                "commands": [["scripts/run_v270_jobs_once.py", "--job", "tail"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "buy_bridge_v280",
                "schedule": {"kind": "cron", "expr": "52 14 * * 1-5"},
                "commands": [["scripts/build_buy_bridge_v280.py"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "track_positions_tail",
                "schedule": {"kind": "cron", "expr": "55 14 * * 1-5"},
                "commands": [["run_positions.py", "--track"]],
                "timeoutSeconds": 300,
            },
            {
                "name": "after_close",
                "schedule": {"kind": "cron", "expr": "30 17 * * 1-5"},
                "commands": [
                    ["run_scan.py", "--refresh-daily-existing", "--daily-limit", "1200", "--daily-workers", "1"],
                    ["run_scan.py", "--build-universe", "--workers", "1"],
                    ["run_scan.py", "--mode", "after_close", "--workers", "1"],
                ],
                "timeoutSeconds": 5400,
            },
            {
                "name": "track_positions_evening",
                "schedule": {"kind": "cron", "expr": "0 20 * * 1-5"},
                "commands": [["run_positions.py", "--track"]],
                "timeoutSeconds": 300,
            },
            {
                "name": "daily_report_v290_build",
                "schedule": {"kind": "cron", "expr": "25 20 * * 1-5"},
                "commands": [["scripts/build_daily_report_v290.py"]],
                "timeoutSeconds": 3600,
            },
            {
                "name": "daily_report",
                "schedule": {"kind": "cron", "expr": "30 20 * * 1-5"},
                "commands": [["run_scan.py", "--daily-report"]],
                "timeoutSeconds": 600,
            },
            {
                "name": "night_cache_expand",
                "schedule": {"kind": "cron", "expr": "30 22 * * 1-5"},
                "commands": [["run_scan.py", "--build-daily-cache", "--daily-limit", "300", "--daily-workers", "1"]],
                "timeoutSeconds": 3600,
            },
        ],
    }
    if SCHEDULER_CONFIG_PATH.exists():
        try:
            import json
            loaded = json.loads(SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                merged = dict(default)
                merged.update({k: v for k, v in loaded.items() if k != "jobs"})
                jobs = loaded.get("jobs")
                if isinstance(jobs, list) and jobs:
                    merged["jobs"] = jobs
                return merged
        except Exception as e:
            print(f"[{now_str()}] 读取调度配置失败，使用内置默认值：{e}", flush=True)
    return default




def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)

def rotate_logs(max_files: int = 100, max_days: int = 30):
    """日志轮转：删除旧日志文件"""
    import datetime as dt
    from datetime import datetime
    
    # 按文件数量轮转
    log_files = sorted(LOG_DIR.glob("*.log"))
    if len(log_files) > max_files:
        for old_file in log_files[:-max_files]:
            try:
                old_file.unlink()
                print(f"[{now_str()}] 删除旧日志文件: {old_file.name}")
            except:
                pass
    
    # 按时间轮转（30天前）
    cutoff_date = datetime.now() - dt.timedelta(days=max_days)
    for log_file in LOG_DIR.glob("*.log"):
        try:
            # 从文件名提取日期（格式：YYYY-MM-DD_jobname.log）
            filename = log_file.stem
            if "_" in filename:
                date_part = filename.split("_")[0]
                file_date = datetime.strptime(date_part, "%Y-%m-%d")
                if file_date < cutoff_date:
                    log_file.unlink()
                    print(f"[{now_str()}] 删除过期日志文件: {log_file.name}")
        except:
            pass


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


def heartbeat():
    ensure_dirs()
    pid = os.getpid()
    lock_pid = _read_scheduler_lock()
    line = f"[{now_str()}] heartbeat pid={pid} lock_pid={lock_pid} project={PROJECT_ROOT}"
    with open(HEARTBEAT_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def _remove_scheduler_lock():
    try:
        if SCHEDULER_LOCK_FILE.exists():
            SCHEDULER_LOCK_FILE.unlink()
    except Exception:
        pass


def _kill_pid(pid: int, wait_seconds: int = 5):
    if not _pid_alive(pid):
        return
    print(f"[{now_str()}] 正在停止旧调度器 pid={pid} ...", flush=True)
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        pass
    start = time.time()
    while time.time() - start < wait_seconds:
        if not _pid_alive(pid):
            print(f"[{now_str()}] 旧调度器已退出 pid={pid}", flush=True)
            return
        time.sleep(0.5)
    if _pid_alive(pid):
        print(f"[{now_str()}] 旧调度器未退出，强制 kill -9 pid={pid}", flush=True)
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def acquire_scheduler_singleton(replace: bool = False):
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
            print("如需重启调度器，请执行：python run_scheduler.py --replace")
            print("=" * 80)
            sys.exit(0)
    if old_pid and not _pid_alive(old_pid):
        _remove_scheduler_lock()
    try:
        fd = os.open(str(SCHEDULER_LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"pid={os.getpid()} started_at={now_str()} project={PROJECT_ROOT}\n".encode("utf-8"))
        os.close(fd)
    except FileExistsError:
        pid = _read_scheduler_lock()
        if pid and _pid_alive(pid):
            print(f"已有调度器正在运行 pid={pid}，本次启动退出。")
            sys.exit(0)
        _remove_scheduler_lock()
        return acquire_scheduler_singleton(replace=replace)
    atexit.register(_remove_scheduler_lock)


def _handle_exit_signal(signum, frame):
    print(f"[{now_str()}] 收到退出信号 {signum}，清理 scheduler.lock", flush=True)
    _remove_scheduler_lock()
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_exit_signal)
signal.signal(signal.SIGINT, _handle_exit_signal)


@contextmanager
def job_lock(job_name: str, stale_seconds: int = 7200):
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
    print(f"[{now_str()}] {job_name} -> {' '.join(full_cmd)}", flush=True)

    env = os.environ.copy()
    if DEFAULT_WECHAT_WEBHOOK:
        env["WECHAT_WEBHOOK"] = DEFAULT_WECHAT_WEBHOOK

    try:
        with open(path, "a", encoding="utf-8") as f:
            proc = subprocess.run(
                full_cmd,
                cwd=str(PROJECT_ROOT),
                stdin=subprocess.DEVNULL,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout,
                close_fds=True,
                env=env,
            )
        write_line(path, f"[{now_str()}] END COMMAND returncode={proc.returncode}")
        return proc.returncode
    except subprocess.TimeoutExpired:
        write_line(path, f"[{now_str()}] COMMAND TIMEOUT, killed: {' '.join(full_cmd)}")
        print(f"[{now_str()}] 任务超时，已终止子任务：{job_name}", flush=True)
        return 124
    except Exception as e:
        write_line(path, f"[{now_str()}] COMMAND ERROR: {e}")
        print(f"[{now_str()}] 任务异常：{job_name} | {e}", flush=True)
        return 1


def get_job_config(job_name: str):
    """从配置文件获取任务配置"""
    import json
    if not SCHEDULER_CONFIG_PATH.exists():
        return None, None
    
    try:
        config = json.loads(SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
        for job in config.get("jobs", []):
            if job.get("name") == job_name:
                commands = job.get("commands", [])
                timeout = job.get("timeoutSeconds")
                return commands, timeout
    except Exception:
        pass
    
    return None, None

def run_job(job_name: str):
    ensure_dirs()
    
    # 从配置文件获取任务配置
    commands, timeout = get_job_config(job_name)
    if not commands:
        print(f"未知任务或任务配置不存在：{job_name}")
        return
    
    path = log_path(job_name)
    try:
        with job_lock(job_name):
            job_heartbeat(job_name, stage="before")
            write_line(path, "")
            write_line(path, "#" * 80)
            write_line(path, f"[{now_str()}] JOB START: {job_name}")
            write_line(path, "#" * 80)
            
            for cmd in commands:
                rc = run_one_command(job_name, cmd, timeout=timeout)
                if rc != 0:
                    write_line(path, f"[{now_str()}] JOB FAILED: {job_name}, command={cmd}, rc={rc}")
                    print(f"[{now_str()}] 任务失败：{job_name}, rc={rc}", flush=True)
                    try:
                        from core.notify import notify_system_event
                        notify_system_event(
                            title="调度任务失败",
                            message=f"任务执行失败，已停止后续命令。\n\n命令：{cmd}\n返回码：{rc}\n日志：{path}",
                            level="ERROR",
                            job_name=job_name,
                            extra={"returncode": rc, "log": str(path)},
                        )
                    except Exception as notify_error:
                        write_line(path, f"[{now_str()}] NOTIFY FAILED: {notify_error}")
                    return
            write_line(path, f"[{now_str()}] JOB DONE: {job_name}")
            print(f"[{now_str()}] 任务完成：{job_name}", flush=True)
    except Exception as e:
        write_line(path, f"[{now_str()}] JOB SKIPPED/ERROR: {job_name} | {e}")
        print(f"[{now_str()}] 任务跳过/异常：{job_name} | {e}", flush=True)
        try:
            from core.notify import notify_system_event
            notify_system_event(
                title="调度任务异常或跳过",
                message=f"任务未正常执行。\n\n原因：{e}\n日志：{path}",
                level="WARN",
                job_name=job_name,
                extra={"error": str(e), "log": str(path)},
            )
        except Exception as notify_error:
            write_line(path, f"[{now_str()}] NOTIFY FAILED: {notify_error}")




def job_heartbeat(job_name: str, stage: str = "before"):
    """任务级心跳：任务执行前1分钟/开始前写入并打印，证明调度器正常触发。"""
    ensure_dirs()
    try:
        pid = os.getpid()
        lock_pid = _read_scheduler_lock()
    except Exception:
        pid = os.getpid()
        lock_pid = ""
    line = f"[{now_str()}] job_heartbeat stage={stage} job={job_name} pid={pid} lock_pid={lock_pid}"
    try:
        with open(HEARTBEAT_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line)

def _cron_from_expr(expr: str) -> CronTrigger:
    minute, hour, day, month, dow = expr.split()
    return CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow, timezone=TIMEZONE)


def add_job_from_config(scheduler: BlockingScheduler, job: Dict[str, object]):
    name = str(job.get("name", "")).strip()
    if not name:
        return
    schedule = job.get("schedule") or {}
    if not isinstance(schedule, dict):
        return
    kind = str(schedule.get("kind", "cron")).lower()
    if kind != "cron":
        print(f"[{now_str()}] 跳过不支持的调度类型：{name} schedule={schedule}", flush=True)
        return
    expr = str(schedule.get("expr", "")).strip()
    if not expr:
        return
    commands = job.get("commands") or []
    timeout = int(job.get("timeoutSeconds") or 0) or None
    scheduler.add_job(
        run_config_job,
        trigger=_cron_from_expr(expr),
        args=[name, commands, timeout],
        id=name,
        name=name,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=int(job.get("misfireGraceTime", 1800)),
    )


def run_config_job(job_name: str, commands: List[List[str]], timeout: Optional[int] = None):
    ensure_dirs()
    
    # 检查是否是交易日（对于盘中任务）
    # 盘中任务：preflight, observe_morning, watchlist_refresh_*, observe_afternoon, tail_confirm, track_positions_*
    if any(keyword in job_name for keyword in ["preflight", "observe", "watchlist", "tail", "track"]):
        if not is_trading_day():
            print(f"[{now_str()}] 今天不是交易日，跳过盘中任务：{job_name}", flush=True)
            return
    
    if not commands:
        print(f"[{now_str()}] 任务 {job_name} 没有可执行命令，跳过。", flush=True)
        return
    path = log_path(job_name)
    try:
        with job_lock(job_name):
            job_heartbeat(job_name, stage="before")
            write_line(path, "")
            write_line(path, "#" * 80)
            write_line(path, f"[{now_str()}] JOB START: {job_name}")
            write_line(path, "#" * 80)
            for cmd in commands:
                rc = run_one_command(job_name, cmd, timeout=timeout)
                if rc != 0:
                    write_line(path, f"[{now_str()}] JOB FAILED: {job_name}, command={cmd}, rc={rc}")
                    print(f"[{now_str()}] 任务失败：{job_name}, rc={rc}", flush=True)
                    try:
                        from core.notify import notify_system_event
                        notify_system_event(
                            title="调度任务失败",
                            message=f"任务执行失败，已停止后续命令。\n\n命令：{cmd}\n返回码：{rc}\n日志：{path}",
                            level="ERROR",
                            job_name=job_name,
                            extra={"returncode": rc, "log": str(path)},
                        )
                    except Exception as notify_error:
                        write_line(path, f"[{now_str()}] NOTIFY FAILED: {notify_error}")
                    return
            write_line(path, f"[{now_str()}] JOB DONE: {job_name}")
            print(f"[{now_str()}] 任务完成：{job_name}", flush=True)
    except Exception as e:
        write_line(path, f"[{now_str()}] JOB SKIPPED/ERROR: {job_name} | {e}")
        print(f"[{now_str()}] 任务跳过/异常：{job_name} | {e}", flush=True)
        try:
            from core.notify import notify_system_event
            notify_system_event(
                title="调度任务异常或跳过",
                message=f"任务未正常执行。\n\n原因：{e}\n日志：{path}",
                level="WARN",
                job_name=job_name,
                extra={"error": str(e), "log": str(path)},
            )
        except Exception as notify_error:
            write_line(path, f"[{now_str()}] NOTIFY FAILED: {notify_error}")


def build_scheduler() -> BlockingScheduler:
    cfg = load_scheduler_config()
    scheduler = BlockingScheduler(timezone=str(cfg.get("timezone") or TIMEZONE))

    scheduler.add_job(
        heartbeat,
        trigger=CronTrigger(day_of_week="mon-fri", hour="9-15", minute=5, timezone=TIMEZONE),
        id="heartbeat_trading",
        name="heartbeat_trading",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    scheduler.add_job(
        heartbeat,
        trigger=CronTrigger(day_of_week="mon-fri", hour="17,20,22", minute=5, timezone=TIMEZONE),
        id="heartbeat_evening",
        name="heartbeat_evening",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    for job in cfg.get("jobs", []):
        if isinstance(job, dict):
            add_job_from_config(scheduler, job)
    return scheduler


def has_today_watchlist() -> bool:
    try:
        import polars as pl
        path = PROJECT_ROOT / "data" / "watchlist.parquet"
        if not path.exists():
            return False
        df = pl.read_parquet(path)
        if df.is_empty():
            return False
        today = datetime.now().strftime("%Y-%m-%d")
        if "date" in df.columns:
            if not df.filter(pl.col("date").cast(pl.Utf8) == today).is_empty():
                return True
        if "last_seen_at" in df.columns:
            if not df.filter(pl.col("last_seen_at").cast(pl.Utf8).str.starts_with(today)).is_empty():
                return True
        return False
    except Exception:
        return False


def maybe_catch_up_on_start():
    now = datetime.now()
    if now.weekday() >= 5:
        return
    hhmm = now.hour * 100 + now.minute
    if 945 <= hhmm <= 1445:
        if not has_today_watchlist():
            print(f"[{now_str()}] 启动补跑：交易时间内且今日 watchlist 不存在，立即执行 observe_morning", flush=True)
            run_job("observe_morning")
        else:
            print(f"[{now_str()}] 今日 watchlist 已存在，启动时不补跑 observe", flush=True)


def print_jobs():
    cfg = load_scheduler_config()
    print("可用任务：")
    for job in cfg.get("jobs", []):
        if not isinstance(job, dict):
            continue
        name = job.get("name", "")
        schedule = (job.get("schedule") or {}).get("expr", "")
        commands = job.get("commands") or []
        timeout = job.get("timeoutSeconds", "-")
        print(f"- {name} | cron={schedule} | timeout={timeout}s")
        for cmd in commands:
            print(f"  {PYTHON} {' '.join(cmd)}")
    print("\n说明：")
    print(f"- 调度配置文件：{SCHEDULER_CONFIG_PATH}")
    print("- 若配置文件缺失，脚本会回退到内置默认任务表")


def print_status():
    ensure_dirs()
    print("===== scheduler status =====")
    print(f"now={now_str()}")
    print(f"project={PROJECT_ROOT}")
    print(f"lock_pid={_read_scheduler_lock()}")
    print(f"lock_file={SCHEDULER_LOCK_FILE}")
    print("heartbeat tail:")
    if HEARTBEAT_LOG.exists():
        lines = HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines()[-10:]
        for line in lines:
            print(line)
    else:
        print("no heartbeat log")
    print("today logs:")
    for p in sorted(LOG_DIR.glob(f"{date_str()}_*.log"))[-20:]:
        print(f"- {p.name} size={p.stat().st_size}")


def parse_args():
    import json
    parser = argparse.ArgumentParser(description="测试二买系统 APScheduler 自动调度器")
    
    # 从配置文件读取任务列表
    job_names = []
    if SCHEDULER_CONFIG_PATH.exists():
        try:
            config = json.loads(SCHEDULER_CONFIG_PATH.read_text(encoding="utf-8"))
            job_names = [job.get("name", "") for job in config.get("jobs", []) if job.get("name")]
        except Exception:
            pass
    
    parser.add_argument("--run-once", choices=job_names if job_names else None, help="立即执行某个任务一次")
    parser.add_argument("--list", action="store_true", help="列出任务与命令")
    parser.add_argument("--replace", action="store_true", help="已有调度器运行时先停止旧调度器")
    parser.add_argument("--status", action="store_true", help="打印调度器状态")
    return parser.parse_args()


def is_trading_day(date_str: str = None) -> bool:
    """判断指定日期是否为交易日"""
    try:
        from core.trading_calendar import is_trading_day as check_trading_day
        return check_trading_day(date_str)
    except ImportError:
        # 如果导入失败，使用简化版本
        import datetime as dt
        from datetime import datetime
        
        if date_str:
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                date_obj = datetime.now().date()
        else:
            date_obj = datetime.now().date()
        
        # 首先检查是否是周末
        if date_obj.weekday() >= 5:  # 5=周六, 6=周日
            return False
        
        # 检查缓存文件
        trading_day_cache = PROJECT_ROOT / "data" / "trading_days.txt"
        if trading_day_cache.exists():
            try:
                trading_days = trading_day_cache.read_text(encoding="utf-8").splitlines()
                date_str = date_obj.strftime("%Y-%m-%d")
                return date_str in trading_days
            except:
                pass
        
        # 如果没有交易日历数据，假设是交易日（除了周末）
        return True

def load_trading_days() -> list:
    """加载交易日历"""
    try:
        from core.trading_calendar import load_trading_days_from_cache
        return load_trading_days_from_cache()
    except ImportError:
        trading_day_cache = PROJECT_ROOT / "data" / "trading_days.txt"
        if trading_day_cache.exists():
            try:
                return trading_day_cache.read_text(encoding="utf-8").splitlines()
            except:
                pass
        return []

def main():
    args = parse_args()
    ensure_dirs()
    
    # 日志轮转
    rotate_logs()
    
    # 检查是否是交易日（除了 --list, --status, --run-once 参数）
    if not args.list and not args.status and not args.run_once:
        if not is_trading_day():
            print(f"[{now_str()}] 今天不是交易日，调度器不启动")
            return
    
    if args.list:
        print_jobs()
        return
    if args.status:
        print_status()
        return
    if args.run_once:
        run_job(args.run_once)
        return

    acquire_scheduler_singleton(replace=args.replace)
    print("=" * 80)
    print("测试二买系统自动调度器已启动")
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
    maybe_catch_up_on_start()
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
