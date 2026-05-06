from __future__ import annotations

import csv
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
REPORT_DIR = DATA_DIR / "reports"
LOG_DIR = PROJECT_ROOT / "logs"
SCHEDULER_LOG_DIR = LOG_DIR / "scheduler"
SCHEDULER_HEARTBEAT = LOG_DIR / "scheduler_heartbeat.log"
WATCHLIST_PATH = DATA_DIR / "watchlist.parquet"
POSITIONS_PATH = DATA_DIR / "positions.parquet"
TRADE_PLAN_PATH = DATA_DIR / "trade_plan.parquet"
PAPER_CANDIDATES_PATH = DATA_DIR / "paper_trade_candidates.parquet"
CONFIG_PATH = PROJECT_ROOT / "config" / "scheduler_jobs.json"

HOST = "127.0.0.1"
PORT = 8787


def _read_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return default


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _read_csv_rows(path: Path, limit: int = 50) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        return rows[-limit:]
    except Exception:
        return []

def _get_watchlist_status_history() -> List[Dict[str, str]]:
    """获取观察池状态变化历史"""
    history = []
    
    # 检查最近的观察池文件变化
    watchlist_path = DATA_DIR / "watchlist.parquet"
    if watchlist_path.exists():
        try:
            import pandas as pd
            import os
            from datetime import datetime
            
            # 获取当前观察池
            current_df = pd.read_parquet(watchlist_path)
            current_codes = set(current_df['code'].astype(str).tolist()) if 'code' in current_df.columns else set()
            
            # 检查历史观察池文件
            watchlist_history_dir = DATA_DIR / "watchlist_history"
            if not watchlist_history_dir.exists():
                watchlist_history_dir.mkdir(exist_ok=True)
            
            # 保存当前观察池作为历史记录
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            history_file = watchlist_history_dir / f"watchlist_{timestamp}.parquet"
            current_df.to_parquet(history_file)
            
            # 获取历史文件列表
            history_files = sorted(watchlist_history_dir.glob("watchlist_*.parquet"), key=os.path.getmtime, reverse=True)
            
            # 比较最近几次的变化
            for i in range(min(3, len(history_files) - 1)):
                try:
                    prev_file = history_files[i + 1]
                    prev_df = pd.read_parquet(prev_file)
                    prev_codes = set(prev_df['code'].astype(str).tolist()) if 'code' in prev_df.columns else set()
                    
                    # 计算变化
                    added = current_codes - prev_codes
                    removed = prev_codes - current_codes
                    
                    if added:
                        history.append({
                            "time": timestamp,
                            "type": "added",
                            "codes": list(added)[:5],  # 只显示前5个
                            "count": len(added),
                            "period": f"对比 {prev_file.stem}"
                        })
                    
                    if removed:
                        history.append({
                            "time": timestamp,
                            "type": "removed",
                            "codes": list(removed)[:5],
                            "count": len(removed),
                            "period": f"对比 {prev_file.stem}"
                        })
                        
                except:
                    continue
                    
        except Exception as e:
            print(f"观察池历史跟踪错误: {e}")
    
    return history


def _get_recent_rejections() -> List[Dict[str, str]]:
    """获取最近的剔除记录"""
    rejections = []
    
    # 检查扫描日志中的剔除记录
    scan_log_dir = PROJECT_ROOT / "logs"
    if scan_log_dir.exists():
        log_files = sorted(scan_log_dir.glob("scan_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        
        for log_file in log_files[:3]:  # 检查最近3个扫描日志
            try:
                content = log_file.read_text(encoding="utf-8")
                lines = content.splitlines()
                
                for line in lines[-200:]:  # 检查最后200行
                    if "reject" in line.lower() and "|" in line:
                        # 提取剔除信息
                        import re
                        time_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                        time_str = time_match.group(1) if time_match else log_file.stem.split("_")[1]
                        
                        # 提取股票代码和原因
                        code_match = re.search(r'reject (\d{6})', line)
                        reason_match = re.search(r'reject \d{6} (\w+)', line)
                        score_match = re.search(r'score=(\d+)', line)
                        
                        if code_match and reason_match:
                            rejections.append({
                                "time": time_str,
                                "code": code_match.group(1),
                                "reason": reason_match.group(1),
                                "score": score_match.group(1) if score_match else "N/A",
                                "source": log_file.name
                            })
                            
                            if len(rejections) >= 20:  # 最多显示20条
                                return rejections
            except:
                continue
    
    return rejections


def _get_recent_wechat_notifications() -> List[Dict[str, str]]:
    """获取最近的企业微信通知"""
    notifications = []
    
    # 检查调度器日志中的企业微信通知
    scheduler_log_dir = PROJECT_ROOT / "logs" / "scheduler"
    if scheduler_log_dir.exists():
        log_files = sorted(scheduler_log_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        
        for log_file in log_files[:5]:  # 检查最近5个日志文件
            try:
                content = log_file.read_text(encoding="utf-8")
                lines = content.splitlines()
                
                for line in lines[-100:]:  # 检查最后100行
                    if "企业微信通知" in line or "测试二买交易助手" in line or "A股二买交易助手" in line:
                        # 提取时间戳和消息
                        import re
                        time_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line)
                        time_str = time_match.group(1) if time_match else log_file.stem.split("_")[0]
                        
                        # 改进的消息提取：支持多种格式
                        message = line.strip()
                        # 尝试提取消息内容
                        message_match = re.search(r'【([^】]+)】', line)
                        if message_match:
                            message = message_match.group(1)
                        elif "📤 企业微信通知" in line:
                            # 格式：📤 企业微信通知 | A股二买交易助手 | TRADE_SCAN | pages=1
                            parts = line.split("|")
                            if len(parts) >= 3:
                                message = f"{parts[1].strip()} - {parts[2].strip()}"
                                if len(parts) > 3:
                                    message += f" ({parts[3].strip()})"
                        
                        notifications.append({
                            "time": time_str,
                            "message": message,
                            "source": log_file.name,
                            "full_line": line.strip()[:200]  # 保留完整行用于调试
                        })
                        
                        if len(notifications) >= 15:  # 最多显示15条
                            return notifications
            except:
                continue
    
    return notifications


def _read_parquet_preview(path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return []
        return df.head(limit).to_dict(orient="records")
    except Exception:
        return []


def _latest_report_date() -> str:
    """获取最新的有效日报日期"""
    from datetime import datetime, timedelta
    
    if not REPORT_DIR.exists():
        # 如果没有报告目录，返回昨天
        yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
        return yesterday
    
    # 获取所有日报文件，按日期排序（最新的在前）
    candidates = []
    for report_file in REPORT_DIR.glob("daily_report_v290_*.json"):
        try:
            name = report_file.stem
            date_str = name.replace("daily_report_v290_", "")
            # 验证日期格式
            datetime.strptime(date_str, "%Y-%m-%d")
            
            # 检查日报内容是否有效（不是所有股票都被拒绝）
            report_content = _read_json(report_file, {})
            observe_data = (report_content.get("payload") or {}).get("observe", {})
            
            # 如果观察池有数据或者不是所有股票都被拒绝，认为是有效日报
            total = observe_data.get("total", 0)
            rejected_count = observe_data.get("status_counter", {}).get("REJECTED", 0)
            
            # 有效条件：有数据且不是全部被拒绝
            is_valid = total > 0 and rejected_count < total
            
            candidates.append((date_str, report_file, is_valid))
        except:
            continue
    
    if not candidates:
        # 没有有效的日报，返回昨天
        yesterday = (datetime.now() - timedelta(days=1)).date().isoformat()
        return yesterday
    
    # 优先选择有效的日报，然后按日期降序排序
    valid_candidates = [(date, file) for date, file, valid in candidates if valid]
    if valid_candidates:
        valid_candidates.sort(key=lambda x: x[0], reverse=True)
        return valid_candidates[0][0]
    
    # 如果没有有效日报，返回最新的日报（即使可能无效）
    all_candidates = [(date, file) for date, file, _ in candidates]
    all_candidates.sort(key=lambda x: x[0], reverse=True)
    return all_candidates[0][0]


def calculate_system_health() -> Dict[str, Any]:
    """计算系统健康状态"""
    from datetime import datetime
    
    health_status = {
        "status": "unknown",
        "last_heartbeat": None,
        "scheduler_running": False,
        "recent_logs": False
    }
    
    # 检查调度器心跳
    if SCHEDULER_HEARTBEAT.exists():
        try:
            lines = SCHEDULER_HEARTBEAT.read_text(encoding="utf-8").splitlines()
            if lines:
                last_line = lines[-1]
                health_status["last_heartbeat"] = last_line
                # 检查是否在最近30分钟内有心跳
                for line in reversed(lines[-10:]):
                    if "[heartbeat]" in line or "heartbeat pid=" in line:
                        # 提取时间戳
                        import re
                        match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line)
                        if match:
                            heartbeat_time = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
                            time_diff = (datetime.now() - heartbeat_time).total_seconds()
                            if time_diff < 1800:  # 30分钟
                                health_status["scheduler_running"] = True
                                health_status["status"] = "healthy"
                                break
        except:
            pass
    
    # 检查最近日志
    scan_logs = list((PROJECT_ROOT / "logs").glob("scan_*.log"))
    if scan_logs:
        latest_log = max(scan_logs, key=lambda p: p.stat().st_mtime)
        log_age = (datetime.now() - datetime.fromtimestamp(latest_log.stat().st_mtime)).total_seconds()
        if log_age < 86400:  # 24小时内有日志
            health_status["recent_logs"] = True
            if health_status["status"] == "unknown":
                health_status["status"] = "healthy"
    
    return health_status

def build_dashboard_state() -> Dict[str, Any]:
    trade_date = _latest_report_date()
    report_json = _read_json(REPORT_DIR / f"daily_report_v290_{trade_date}.json", {})
    observe_json = (report_json.get("payload") or {}).get("observe", {})
    tail_json = (report_json.get("payload") or {}).get("tail_confirm", {})
    buy_json = (report_json.get("payload") or {}).get("buy_bridge", {})
    
    # 计算系统健康状态
    health = calculate_system_health()

    watchlist_rows = _read_parquet_preview(WATCHLIST_PATH, 12)
    positions_rows = _read_parquet_preview(POSITIONS_PATH, 12)
    trade_plan_rows = _read_parquet_preview(TRADE_PLAN_PATH, 12)
    paper_candidates_rows = _read_parquet_preview(PAPER_CANDIDATES_PATH, 12)
    scheduler_jobs = _read_json(CONFIG_PATH, {}).get("jobs", [])
    scheduler_tail = _read_text(SCHEDULER_HEARTBEAT).splitlines()[-12:]

    return {
        "trade_date": trade_date,
        "report": report_json,
        "observe": observe_json,
        "tail": tail_json,
        "buy": buy_json,
        "health": health,
        "watchlist_rows": watchlist_rows,
        "positions_rows": positions_rows,
        "trade_plan_rows": trade_plan_rows,
        "paper_candidates_rows": paper_candidates_rows,
        "scheduler_jobs": scheduler_jobs,
        "scheduler_tail": scheduler_tail,
        "scan_logs": sorted([p.name for p in (PROJECT_ROOT / "logs").glob("scan_*.log")], reverse=True)[:10],
        "reject_logs": sorted([p.name for p in (PROJECT_ROOT / "logs").glob("rejects_*.csv")], reverse=True)[:10],
        "wechat_notifications": _get_recent_wechat_notifications(),
        "rejections": _get_recent_rejections(),
        "watchlist_history": _get_watchlist_status_history(),
    }


def esc(v: Any) -> str:
    text = "" if v is None else str(v)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_table(rows: List[Dict[str, Any]], columns: List[str], page: int = 1, page_size: int = 20) -> str:
    if not rows:
        return '<div class="empty">暂无数据 / No data</div>'
    
    # 分页逻辑
    total = len(rows)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paged_rows = rows[start_idx:end_idx]
    
    head = "".join(f"<th>{esc(c)}</th>" for c in columns)
    body = []
    for row in paged_rows:
        body.append("<tr>" + "".join(f"<td>{esc(row.get(c, ''))}</td>" for c in columns) + "</tr>")
    
    # 分页导航
    total_pages = (total + page_size - 1) // page_size
    pagination = ""
    if total_pages > 1:
        pagination = f'<div class="pagination">'
        if page > 1:
            pagination += f'<a href="?page={page-1}#watchlist">上一页</a> '
        pagination += f'<span>第 {page}/{total_pages} 页 (共 {total} 条)</span>'
        if page < total_pages:
            pagination += f' <a href="?page={page+1}#watchlist">下一页</a>'
        pagination += '</div>'
    
    return f"""
    {pagination}
    <div class="table-wrap">
        <table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>
    </div>
    {pagination}
    """


def render_html(page: int = 1) -> bytes:
    state = build_dashboard_state()
    report = state["report"].get("payload") or {}
    scheduler_tail_text = "\n".join(state["scheduler_tail"]) or "暂无心跳日志"
    scheduler_jobs_html = ''.join(
        f'<div class="item"><strong>{esc(job.get("name", ""))}</strong><div class="small">cron: {esc((job.get("schedule") or {}).get("expr", ""))} · timeout: {esc(job.get("timeoutSeconds", "-"))}s</div></div>'
        for job in state["scheduler_jobs"] if isinstance(job, dict)
    )
    html = f"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<meta http-equiv="refresh" content="30" />
<title>测试二买系统可视化面板</title>
<style>
:root {{ --bg:#f7f1e8; --panel:rgba(255,255,255,.85); --text:#1e2a33; --muted:#66727f; --line:rgba(30,42,51,.1); --accent:#ff7b54; --accent2:#2f8a83; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:Inter, "Noto Sans SC", sans-serif; background:linear-gradient(180deg,#fff9f0 0%,#f7f1e8 22%,#f3e8db 100%); color:var(--text); }}
a {{ color:inherit; }}
.wrapper {{ max-width:1320px; margin:0 auto; padding:24px 18px 90px; }}
.topbar {{ display:flex; justify-content:space-between; gap:16px; align-items:center; margin-bottom:18px; }}
.title h1 {{ margin:0; font-size:30px; }}
.title p {{ margin:6px 0 0; color:var(--muted); }}
.badge {{ padding:8px 12px; border-radius:999px; background:rgba(47,138,131,.12); color:var(--accent2); font-weight:700; }}
.grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin-bottom:16px; }}
.card, .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:20px; box-shadow:0 16px 40px rgba(61,44,28,.10); }}
.card {{ padding:16px; }}
.k {{ color:var(--muted); font-size:12px; margin-bottom:8px; text-transform:uppercase; letter-spacing:.08em; }}
.v {{ font-size:22px; font-weight:800; }}
.layout {{ display:grid; grid-template-columns:1.1fr .9fr; gap:16px; }}
.panel {{ padding:18px; margin-bottom:16px; overflow:hidden; }}
.panel h2 {{ margin:0 0 12px; font-size:20px; }}
.quick {{ display:flex; flex-wrap:wrap; gap:10px; margin-bottom:16px; }}
.quick a {{ text-decoration:none; padding:8px 12px; border-radius:999px; background:rgba(30,42,51,.06); }}
.small {{ color:var(--muted); font-size:12px; }}
pre {{ margin:0; white-space:pre-wrap; font-family:inherit; line-height:1.7; color:#32414d; }}
.table-wrap {{ overflow:auto; border-radius:14px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th, td {{ padding:10px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
th {{ color:var(--muted); font-size:12px; letter-spacing:.04em; text-transform:uppercase; }}
.list {{ display:grid; gap:8px; }}
.item {{ padding:10px 12px; border-radius:14px; background:rgba(30,42,51,.04); }}
.empty {{ color:var(--muted); padding:8px 0; }}
.footer {{ color:var(--muted); font-size:12px; margin-top:10px; }}
.pagination {{ display:flex; justify-content:space-between; align-items:center; margin:12px 0; padding:8px 12px; background:rgba(30,42,51,.04); border-radius:14px; }}
.pagination a {{ text-decoration:none; padding:6px 12px; border-radius:999px; background:rgba(47,138,131,.12); color:var(--accent2); }}
.pagination span {{ color:var(--muted); font-size:13px; }}
@media (max-width: 1100px) {{ .grid, .layout {{ grid-template-columns:1fr; }} .topbar {{ flex-direction:column; align-items:start; }} }}
</style>
</head>
<body>
<div class="wrapper">
  <div class="topbar">
    <div class="title">
      <h1>测试二买系统可视化面板</h1>
      <p>观察池、持仓、调度、日报、日志一页看完 / One page for watchlist, positions, scheduler and reports.</p>
    </div>
    <div class="badge">Latest report: {esc(state['trade_date'])}</div>
  </div>

  <div class="grid">
    <div class="card"><div class="k">观察池 / Watchlist</div><div class="v">{esc(report.get('observe', {}).get('total', len(state['watchlist_rows'])))}</div></div>
    <div class="card"><div class="k">尾盘买入 / Buy</div><div class="v">{esc(report.get('tail_confirm', {}).get('buy_count', 0))}</div></div>
    <div class="card"><div class="k">持仓 / Positions</div><div class="v">{esc(len(state['positions_rows']))}</div></div>
    <div class="card"><div class="k">系统健康 / Health</div><div class="v">{esc(state['health'].get('status', 'unknown'))}</div></div>
  </div>

  <div class="quick">
    <a href="#report">日报</a>
    <a href="#watchlist">观察池</a>
    <a href="#positions">持仓</a>
    <a href="#plans">交易计划</a>
    <a href="#scheduler">调度</a>
    <a href="#logs">日志</a>
    <a href="#notifications">企业微信通知</a>
  </div>

  <div class="layout">
    <div>
      <section class="panel" id="report">
        <h2>日报摘要 / Daily report</h2>
        <div class="list">
          <div class="item">交易日期：{esc(state['trade_date'])}</div>
          <div class="item">观察池候选：{esc(report.get('observe', {}).get('total', '-'))}</div>
          <div class="item">尾盘重点候选：{esc(report.get('observe', {}).get('tail_focus_count', '-'))}</div>
          <div class="item">低优先级候选：{esc(report.get('observe', {}).get('low_priority_count', '-'))}</div>
          <div class="item">尾盘买入：{esc(report.get('tail_confirm', {}).get('buy_count', 0))}</div>
          <div class="item">纸面交易候选：{esc(report.get('paper_candidates_rows', len(state['paper_candidates_rows'])))}</div>
          <div class="item">开盘复核计划：{esc(report.get('open_recheck_rows', len(state['trade_plan_rows'])))}</div>
          <div class="item">持仓数量：{esc(report.get('positions_rows', len(state['positions_rows'])))}</div>
          <!-- 添加状态统计 -->
          {''.join(f'<div class="item" style="color:{"var(--accent)" if "REJECTED" in status else "inherit"}">{status}：{count}</div>' for status, count in (report.get('observe', {}).get('status_counter', {}).items()))}
        </div>
        <div class="footer">日报日期：{esc(state['trade_date'])} | 如果当天没有有效日报，会显示最近的日报。</div>
      </section>

      <section class="panel" id="watchlist">
        <h2>观察池 / Watchlist preview</h2>
        {render_table(state['watchlist_rows'], ['code', 'name', 'status', 'action', 'total_score', 'risk_pct'], page=page)}
      </section>

      <section class="panel" id="positions">
        <h2>持仓 / Positions preview</h2>
        <div class="table-wrap">{render_table(state['positions_rows'], ['code', 'name', 'buy_price', 'shares', 'current_price', 'pnl_pct'])}</div>
      </section>

      <section class="panel" id="plans">
        <h2>交易计划 / Trade plans</h2>
        <div class="table-wrap">{render_table(state['trade_plan_rows'], ['code', 'name', 'action', 'entry_price', 'stop_loss', 'take_profit_1'])}</div>
      </section>
    </div>

    <div>
      <section class="panel" id="scheduler">
        <h2>调度配置 / Scheduler jobs</h2>
        <div class="list">
          {scheduler_jobs_html}
        </div>
        <div class="footer">配置文件：config/scheduler_jobs.json</div>
      </section>

      <section class="panel">
        <h2>调度心跳 / Scheduler heartbeat</h2>
        <pre>{esc(scheduler_tail_text)}</pre>
      </section>

      <section class="panel" id="logs">
        <h2>日志 / Logs</h2>
        <div class="list">
          <div class="item"><strong>Scan logs</strong><div class="small">{esc(', '.join(state['scan_logs']) or '暂无')}</div></div>
          <div class="item"><strong>Reject logs</strong><div class="small">{esc(', '.join(state['reject_logs']) or '暂无')}</div></div>
        </div>
      </section>

      <section class="panel" id="notifications">
        <h2>企业微信通知 / WeChat Notifications</h2>
        <div class="list">
          {''.join(f'<div class="item"><strong>{esc(notif["time"])}</strong><div class="small">{esc(notif["message"])}</div><div class="small" style="color:var(--muted);">{esc(notif["source"])}</div></div>' for notif in state['wechat_notifications'])}
          {'' if state['wechat_notifications'] else '<div class="empty">暂无最近的企业微信通知</div>'}
        </div>
        <div class="footer">显示最近的企业微信通知（最多15条）</div>
      </section>

      <section class="panel" id="rejections">
        <h2>观察池剔除记录 / Watchlist Rejections</h2>
        <div class="list">
          {''.join(f'<div class="item"><strong>{esc(rej["code"])} - {esc(rej["reason"])}</strong><div class="small">时间: {esc(rej["time"])} | 分数: {esc(rej["score"])}</div><div class="small" style="color:var(--muted);">{esc(rej["source"])}</div></div>' for rej in state['rejections'])}
          {'' if state['rejections'] else '<div class="empty">暂无最近的剔除记录</div>'}
        </div>
        <div class="footer">显示最近的观察池剔除记录（最多20条）</div>
      </section>

      <section class="panel" id="watchlist_history">
        <h2>观察池状态跟踪 / Watchlist Status Tracking</h2>
        <div class="list">
          {''.join(f'<div class="item" style="color:{"var(--accent2)" if hist["type"] == "added" else "var(--accent)"}"><strong>{esc("新增" if hist["type"] == "added" else "移除")} {esc(hist["count"])} 只股票</strong><div class="small">时间: {esc(hist["time"])} | 示例: {esc(", ".join(hist["codes"]))}</div><div class="small" style="color:var(--muted);">{esc(hist["period"])}</div></div>' for hist in state['watchlist_history'])}
          {'' if state['watchlist_history'] else '<div class="empty">暂无观察池状态变化记录</div>'}
        </div>
        <div class="footer">跟踪观察池中股票的添加和移除情况</div>
      </section>
    </div>
  </div>
</div>
</body>
</html>
"""
    return html.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return
        
        # 解析查询参数
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        query_params = parse_qs(parsed.query)
        page = int(query_params.get('page', ['1'])[0])
        
        content = render_html(page=page)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"dashboard running at http://{HOST}:{PORT}/")
    server.serve_forever()
