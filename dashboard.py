from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
REPORT_DIR = DATA_DIR / "reports"
LOG_DIR = PROJECT_ROOT / "logs"
SCHEDULER_LOG_DIR = LOG_DIR / "scheduler"
SCHEDULER_HEARTBEAT = LOG_DIR / "scheduler_heartbeat.log"
CONFIG_PATH = PROJECT_ROOT / "config" / "scheduler_jobs.json"

WATCHLIST_PATH = DATA_DIR / "watchlist.parquet"
TAIL_FOCUS_PATH = DATA_DIR / "watchlist_tail_focus.parquet"
TAIL_RESULTS_PATH = DATA_DIR / "tail_confirm_results_v265.parquet"
POSITIONS_PATH = DATA_DIR / "positions.parquet"
TRADE_PLAN_PATH = DATA_DIR / "trade_plan.parquet"
OPEN_RECHECK_PATH = DATA_DIR / "trade_plan_open_recheck.parquet"
PAPER_CANDIDATES_PATH = DATA_DIR / "paper_trade_candidates.parquet"
WATCHLIST_HISTORY_DIR = DATA_DIR / "watchlist_history"

HOST = "127.0.0.1"
PORT = 8787
AUTO_REFRESH_SECONDS = 30


def esc(v: Any) -> str:
    s = "" if v is None else str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _read_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return default


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _read_parquet(path: Path):
    if not path.exists():
        return None
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        if df is None or df.empty:
            return None
        return df.where(df.notna(), None)
    except Exception:
        return None


def parquet_rows(path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    df = _read_parquet(path)
    if df is None:
        return []
    return df.head(limit).to_dict(orient="records")


def parquet_count(path: Path) -> int:
    df = _read_parquet(path)
    return 0 if df is None else int(len(df))


def latest_report_date() -> str:
    if not REPORT_DIR.exists():
        return datetime.now().date().isoformat()
    dates = []
    for p in REPORT_DIR.glob("daily_report_v290_*.json"):
        try:
            d = p.stem.replace("daily_report_v290_", "")
            datetime.strptime(d, "%Y-%m-%d")
            dates.append(d)
        except Exception:
            pass
    return sorted(dates, reverse=True)[0] if dates else datetime.now().date().isoformat()


def load_report_payload(date: str) -> Dict[str, Any]:
    obj = _read_json(REPORT_DIR / f"daily_report_v290_{date}.json", {})
    if not isinstance(obj, dict):
        return {}
    payload = obj.get("payload")
    return payload if isinstance(payload, dict) else obj


def extract_time(line: str, fallback: str = "") -> str:
    for pat in [r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"]:
        m = re.search(pat, line)
        if m:
            return m.group(1)
    return fallback


def classify_notice(line: str) -> str:
    if "ERROR" in line or "失败" in line or "异常" in line:
        return "ERROR"
    if "WARN" in line or "警告" in line or "跳过" in line:
        return "WARN"
    if "日报" in line or "daily_report" in line:
        return "DAILY_REPORT"
    if "尾盘" in line or "tail_confirm" in line:
        return "TAIL_CONFIRM"
    if "持仓" in line or "position" in line.lower():
        return "POSITION"
    if "扫描" in line or "TRADE_SCAN" in line:
        return "TRADE_SCAN"
    return "INFO"


def clean_notice(line: str) -> Dict[str, str]:
    raw = line.strip()
    title = "企业微信通知"
    category = classify_notice(raw)
    summary = re.sub(r"^\[.*?\]\s*", "", raw)
    if "|" in raw:
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        if len(parts) >= 2:
            title = re.sub(r"^\[.*?\]\s*", "", parts[0]).replace("📤", "").strip() or title
            if len(parts) >= 3:
                category = parts[2]
                summary = " · ".join(parts[1:])
            else:
                summary = " · ".join(parts)
    m = re.search(r"【([^】]+)】", raw)
    if m:
        title = m.group(1)
        summary = raw.replace(m.group(0), "").strip(" -|")
    return {"title": title[:60], "category": category[:40], "summary": summary[:260], "raw": raw[:500]}


def recent_wechat_notifications(limit: int = 30) -> Dict[str, Any]:
    files: List[Path] = []
    if SCHEDULER_LOG_DIR.exists():
        files.extend(sorted(SCHEDULER_LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:30])
    if LOG_DIR.exists():
        files.extend(sorted(LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:20])
    keywords = ["企业微信通知", "A股二买交易助手", "测试二买交易助手", "wecom", "webhook", "📤"]
    items: List[Dict[str, str]] = []
    for f in files:
        try:
            lines = f.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        fallback = f.stem.split("_")[0]
        for line in reversed(lines[-600:]):
            if not any(k.lower() in line.lower() for k in keywords):
                continue
            info = clean_notice(line)
            items.append({"time": extract_time(line, fallback), "title": info["title"], "category": info["category"], "summary": info["summary"], "source": f.name, "raw": info["raw"]})
            if len(items) >= limit:
                break
        if len(items) >= limit:
            break
    deduped = []
    seen = set()
    for x in items:
        key = (x.get("time"), x.get("summary"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(x)
    return {"items": deduped[:limit], "count": len(deduped[:limit]), "category_counter": dict(Counter(x.get("category") or "INFO" for x in deduped)), "latest_time": deduped[0]["time"] if deduped else None}


def read_codes(path: Path) -> set[str]:
    df = _read_parquet(path)
    if df is None:
        return set()
    for col in ["symbol", "code"]:
        if col in df.columns:
            return set(df[col].astype(str).tolist())
    return set()


def watchlist_history(limit: int = 10) -> Dict[str, Any]:
    # 只读：不创建目录，不写快照。
    if not WATCHLIST_HISTORY_DIR.exists():
        return {"items": [], "history_files": 0, "message": "暂无历史快照。dashboard 只读，不会自动创建 watchlist_history。"}
    files = sorted(WATCHLIST_HISTORY_DIR.glob("watchlist_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return {"items": [], "history_files": 0, "message": "暂无历史快照。dashboard 只读，不会自动创建 watchlist_history。"}
    items = []
    current, latest = read_codes(WATCHLIST_PATH), read_codes(files[0])
    for typ, codes in [("added", current - latest), ("removed", latest - current)]:
        if codes:
            items.append({"time": datetime.fromtimestamp(files[0].stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"), "type": typ, "count": len(codes), "codes": sorted(codes)[:8], "period": f"当前观察池 vs {files[0].name}"})
    for i in range(min(len(files) - 1, 5)):
        a, b = files[i], files[i + 1]
        ca, cb = read_codes(a), read_codes(b)
        for typ, codes in [("added", ca - cb), ("removed", cb - ca)]:
            if codes:
                items.append({"time": datetime.fromtimestamp(a.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"), "type": typ, "count": len(codes), "codes": sorted(codes)[:8], "period": f"{a.name} vs {b.name}"})
    return {"items": items[:limit], "history_files": len(files), "message": "" if items else "历史快照存在，但最近几次观察池没有明显新增/移除。"}


def recent_rejections(limit: int = 20) -> List[Dict[str, str]]:
    files = []
    if SCHEDULER_LOG_DIR.exists():
        files.extend(sorted(SCHEDULER_LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:30])
    if LOG_DIR.exists():
        files.extend(sorted(LOG_DIR.glob("scan_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:10])
    out = []
    for f in files:
        try:
            lines = f.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for line in reversed(lines[-600:]):
            if not ("reject" in line.lower() or "rejected" in line.lower() or "拒绝" in line or "剔除" in line):
                continue
            code = re.search(r"([036]\d{5})(?:\.(?:SH|SZ))?", line)
            score = re.search(r"(?:score|total_score|daily)[=:]\s*([0-9.]+)", line)
            out.append({"time": extract_time(line, f.stem.split("_")[0]), "code": code.group(1) if code else "-", "reason": line.strip()[:220], "score": score.group(1) if score else "-", "source": f.name})
            if len(out) >= limit:
                return out
    return out


def health() -> Dict[str, Any]:
    h = {"status": "warning", "last_heartbeat": None, "scheduler_running": False, "recent_logs": False}
    if SCHEDULER_HEARTBEAT.exists():
        lines = SCHEDULER_HEARTBEAT.read_text(encoding="utf-8", errors="ignore").splitlines()
        if lines:
            h["last_heartbeat"] = lines[-1]
            for line in reversed(lines[-30:]):
                if "heartbeat pid=" in line or "job_heartbeat" in line:
                    m = re.search(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", line)
                    if m:
                        t = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                        if (datetime.now() - t).total_seconds() < 3600:
                            h["scheduler_running"] = True
                            h["status"] = "healthy"
                            break
    logs = []
    if SCHEDULER_LOG_DIR.exists():
        logs.extend(SCHEDULER_LOG_DIR.glob("*.log"))
    if LOG_DIR.exists():
        logs.extend(LOG_DIR.glob("scan_*.log"))
    if logs:
        latest = max(logs, key=lambda p: p.stat().st_mtime)
        if (datetime.now() - datetime.fromtimestamp(latest.stat().st_mtime)).total_seconds() < 86400:
            h["recent_logs"] = True
            if h["status"] == "warning":
                h["status"] = "healthy"
    return h


def build_dashboard_state() -> Dict[str, Any]:
    trade_date = latest_report_date()
    payload = load_report_payload(trade_date)
    jobs = _read_json(CONFIG_PATH, {}).get("jobs", [])
    return {
        "trade_date": trade_date,
        "report": payload,
        "observe": payload.get("observe", {}) if isinstance(payload, dict) else {},
        "tail": payload.get("tail_confirm", {}) if isinstance(payload, dict) else {},
        "buy": payload.get("buy_bridge", {}) if isinstance(payload, dict) else {},
        "health": health(),
        "counts": {
            "watchlist": parquet_count(WATCHLIST_PATH),
            "tail_focus": parquet_count(TAIL_FOCUS_PATH),
            "tail_results": parquet_count(TAIL_RESULTS_PATH),
            "positions": parquet_count(POSITIONS_PATH),
            "paper_candidates": parquet_count(PAPER_CANDIDATES_PATH),
            "open_recheck": parquet_count(OPEN_RECHECK_PATH),
        },
        "watchlist_rows": parquet_rows(WATCHLIST_PATH, 20),
        "tail_focus_rows": parquet_rows(TAIL_FOCUS_PATH, 20),
        "tail_results_rows": parquet_rows(TAIL_RESULTS_PATH, 20),
        "positions_rows": parquet_rows(POSITIONS_PATH, 20),
        "trade_plan_rows": parquet_rows(TRADE_PLAN_PATH, 20),
        "open_recheck_rows": parquet_rows(OPEN_RECHECK_PATH, 20),
        "paper_candidates_rows": parquet_rows(PAPER_CANDIDATES_PATH, 20),
        "scheduler_jobs": jobs,
        "scheduler_tail": _read_text(SCHEDULER_HEARTBEAT).splitlines()[-12:],
        "scheduler_logs": sorted([p.name for p in SCHEDULER_LOG_DIR.glob("*.log")], reverse=True)[:10] if SCHEDULER_LOG_DIR.exists() else [],
        "scan_logs": sorted([p.name for p in LOG_DIR.glob("scan_*.log")], reverse=True)[:10] if LOG_DIR.exists() else [],
        "reject_logs": sorted([p.name for p in LOG_DIR.glob("rejects_*.csv")], reverse=True)[:10] if LOG_DIR.exists() else [],
        "wechat_notifications": recent_wechat_notifications(),
        "rejections": recent_rejections(),
        "watchlist_history": watchlist_history(),
        "last_refresh": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def pick_columns(rows: List[Dict[str, Any]], preferred: List[str]) -> List[str]:
    if not rows:
        return preferred
    keys = set()
    for r in rows[:10]:
        keys.update(r.keys())
    cols = [c for c in preferred if c in keys]
    return cols if cols else list(rows[0].keys())[:8]


def render_table(rows: List[Dict[str, Any]], columns: List[str], page: int = 1, page_size: int = 20, anchor: str = "") -> str:
    if not rows:
        return '<div class="empty">暂无数据 / No data</div>'
    total = len(rows)
    page = max(1, page)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    chunk = rows[(page - 1) * page_size: page * page_size]
    columns = pick_columns(rows, columns)
    head = "".join(f"<th>{esc(c)}</th>" for c in columns)
    body = "".join("<tr>" + "".join(f"<td>{esc(r.get(c, ''))}</td>" for c in columns) + "</tr>" for r in chunk)
    pager = ""
    if total_pages > 1:
        prev = f"/?page={page-1}#{anchor}" if page > 1 else "#"
        nxt = f"/?page={page+1}#{anchor}" if page < total_pages else "#"
        pager = f'<div class="pagination"><a class="{"" if page > 1 else "disabled"}" href="{prev}">上一页</a><span>第 {page}/{total_pages} 页，共 {total} 条</span><a class="{"" if page < total_pages else "disabled"}" href="{nxt}">下一页</a></div>'
    return f'{pager}<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>{pager}'


def render_notices(notice: Dict[str, Any]) -> str:
    items = notice.get("items") or []
    if not items:
        return '<div class="empty">暂无最近企业微信通知。这里只读解析日志，不发送消息。</div>'
    cards = []
    for n in items[:15]:
        cards.append(f'<div class="notice-card"><div class="notice-top"><span class="pill">{esc(n.get("category"))}</span><span class="small">{esc(n.get("time"))}</span></div><strong>{esc(n.get("title"))}</strong><div class="notice-summary">{esc(n.get("summary"))}</div><div class="small">来源：{esc(n.get("source"))}</div></div>')
    return "".join(cards)


def render_jobs(jobs: List[Dict[str, Any]]) -> str:
    if not jobs:
        return '<div class="empty">暂无调度配置</div>'
    rows = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        expr = str((job.get("schedule") or {}).get("expr") or "")
        parts = expr.split()
        t = expr
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            t = f"{int(parts[1]):02d}:{int(parts[0]):02d}"
        cmd = " / ".join(" ".join(c) for c in (job.get("commands") or []) if isinstance(c, list))
        rows.append((t, f'<div class="job-row"><div class="job-time">{esc(t)}</div><div><strong>{esc(job.get("name"))}</strong><div class="small">{esc(cmd)}</div></div></div>'))
    return "".join(x[1] for x in sorted(rows, key=lambda x: x[0]))


def render_html(page: int = 1) -> bytes:
    s = build_dashboard_state()
    obs = s["observe"] if isinstance(s["observe"], dict) else {}
    tail = s["tail"] if isinstance(s["tail"], dict) else {}
    counts = s["counts"]
    notice = s["wechat_notifications"]
    hist = s["watchlist_history"]
    heartbeat = "\n".join(s["scheduler_tail"]) or "暂无心跳日志"
    hist_cards = "".join(
        f'<div class="item {esc("history-add" if h.get("type") == "added" else "history-remove")}"><strong>{esc("新增" if h.get("type") == "added" else "移除")} {esc(h.get("count"))} 只</strong><div class="small">时间：{esc(h.get("time"))} | 示例：{esc(", ".join(h.get("codes") or []))}</div><div class="small">{esc(h.get("period"))}</div></div>'
        for h in hist.get("items", [])
    )
    if not hist.get("items"):
        hist_cards += f'<div class="empty">{esc(hist.get("message"))}</div>'
    reject_cards = "".join(
        f'<div class="item"><strong>{esc(r.get("code"))}</strong><div>{esc(r.get("reason"))}</div><div class="small">时间：{esc(r.get("time"))} | 分数：{esc(r.get("score"))} | 来源：{esc(r.get("source"))}</div></div>'
        for r in s["rejections"]
    ) or '<div class="empty">暂无最近拒绝/剔除记录</div>'

    css = """
:root{--panel:rgba(255,255,255,.88);--text:#1e2a33;--muted:#66727f;--line:rgba(30,42,51,.1);--accent:#ff7b54;--accent2:#2f8a83;--red:#c2410c;--green:#047857}*{box-sizing:border-box}body{margin:0;font-family:Inter,"Noto Sans SC",-apple-system,BlinkMacSystemFont,sans-serif;background:linear-gradient(180deg,#fff9f0 0%,#f7f1e8 26%,#f3e8db 100%);color:var(--text)}a{color:inherit}.wrapper{max-width:1420px;margin:0 auto;padding:24px 18px 90px}.topbar{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:18px}.title h1{margin:0;font-size:30px}.title p{margin:6px 0 0;color:var(--muted)}.badges{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:8px}.badge{padding:8px 12px;border-radius:999px;background:rgba(47,138,131,.12);color:var(--accent2);font-weight:700;text-decoration:none}.grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:14px;margin-bottom:16px}.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:20px;box-shadow:0 16px 40px rgba(61,44,28,.10)}.card{padding:16px}.k{color:var(--muted);font-size:12px;margin-bottom:8px;text-transform:uppercase;letter-spacing:.08em}.v{font-size:22px;font-weight:800}.layout{display:grid;grid-template-columns:1.08fr .92fr;gap:16px}.panel{padding:18px;margin-bottom:16px;overflow:hidden}.panel h2{margin:0 0 12px;font-size:20px}.quick{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}.quick a{text-decoration:none;padding:8px 12px;border-radius:999px;background:rgba(30,42,51,.06)}.small{color:var(--muted);font-size:12px}pre{margin:0;white-space:pre-wrap;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;line-height:1.65;color:#32414d}.table-wrap{overflow:auto;border-radius:14px}table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:10px 8px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{color:var(--muted);font-size:12px;letter-spacing:.04em;text-transform:uppercase}.list{display:grid;gap:8px}.item{padding:10px 12px;border-radius:14px;background:rgba(30,42,51,.04)}.empty{color:var(--muted);padding:8px 0}.footer{color:var(--muted);font-size:12px;margin-top:10px}.pagination{display:flex;justify-content:space-between;align-items:center;margin:12px 0;padding:8px 12px;background:rgba(30,42,51,.04);border-radius:14px}.pagination a{text-decoration:none;padding:6px 12px;border-radius:999px;background:rgba(47,138,131,.12);color:var(--accent2)}.pagination a.disabled{pointer-events:none;opacity:.35}.notice-card{padding:12px;border-radius:16px;background:rgba(30,42,51,.045);border:1px solid var(--line);margin-bottom:10px}.notice-top{display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:8px}.notice-summary{margin:6px 0;color:#32414d;line-height:1.55}.pill{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(47,138,131,.12);color:var(--accent2);font-size:12px;font-weight:700}.job-row{display:grid;grid-template-columns:54px 1fr;gap:10px;padding:10px 0;border-bottom:1px solid var(--line)}.job-time{font-weight:800;color:var(--accent2)}.history-add{color:var(--green)}.history-remove{color:var(--red)}@media(max-width:1180px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.layout{grid-template-columns:1fr}.topbar{flex-direction:column;align-items:start}.badges{justify-content:flex-start}}
"""
    html = f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><meta http-equiv="refresh" content="{AUTO_REFRESH_SECONDS}"/><meta http-equiv="Cache-Control" content="no-store"/><title>A股二买交易助手可视化面板</title><style>{css}</style></head><body><div class="wrapper">
<div class="topbar"><div class="title"><h1>A股二买交易助手可视化面板</h1><p>只读展示。页面每 {AUTO_REFRESH_SECONDS} 秒自动重新读取 data/reports、parquet 和日志；点击刷新也可立即看最新数据。</p></div><div class="badges"><div class="badge">日报日期：{esc(s['trade_date'])}</div><div class="badge">页面刷新：{esc(s['last_refresh'])}</div><a class="badge" href="/">立即刷新</a></div></div>
<div class="grid"><div class="card"><div class="k">观察池</div><div class="v">{esc(obs.get('total', counts['watchlist']))}</div></div><div class="card"><div class="k">尾盘重点</div><div class="v">{esc(obs.get('tail_focus_count', counts['tail_focus']))}</div></div><div class="card"><div class="k">买入信号</div><div class="v">{esc(tail.get('buy_count',0))}</div></div><div class="card"><div class="k">观察 / 拒绝</div><div class="v">{esc(tail.get('watch_count',0))} / {esc(tail.get('rejected_count',0))}</div></div><div class="card"><div class="k">纸面候选</div><div class="v">{esc(counts['paper_candidates'])}</div></div><div class="card"><div class="k">系统健康</div><div class="v">{esc(s['health'].get('status','unknown'))}</div></div></div>
<div class="quick"><a href="#report">日报</a><a href="#tail">尾盘确认</a><a href="#watchlist">观察池</a><a href="#paper">纸面交易</a><a href="#plans">开盘复核</a><a href="#scheduler">调度</a><a href="#notifications">企业微信通知</a><a href="#history">观察池历史</a><a href="#logs">日志</a></div>
<div class="layout"><div>
<section class="panel" id="report"><h2>日报摘要</h2><div class="list"><div class="item">观察池候选：{esc(obs.get('total', counts['watchlist']))}</div><div class="item">尾盘重点候选：{esc(obs.get('tail_focus_count', counts['tail_focus']))}</div><div class="item">低优先级候选：{esc(obs.get('low_priority_count','-'))}</div><div class="item">尾盘买入：{esc(tail.get('buy_count',0))}</div><div class="item">尾盘观察：{esc(tail.get('watch_count',0))}</div><div class="item">尾盘拒绝：{esc(tail.get('rejected_count',0))}</div><div class="item">纸面交易候选：{esc(counts['paper_candidates'])}</div><div class="item">开盘复核计划：{esc(counts['open_recheck'])}</div><div class="item">持仓数量：{esc(counts['positions'])}</div></div><div class="footer">页面只读，不写入任何数据。</div></section>
<section class="panel" id="tail"><h2>尾盘确认结果</h2>{render_table(s['tail_results_rows'], ['symbol','code','stock_name','name','signal_status','observe_quality','risk_pct','daily_2buy_score','current_price','trigger_price','explain_reasons'], page=page, anchor='tail')}</section>
<section class="panel" id="watchlist"><h2>观察池</h2>{render_table(s['watchlist_rows'], ['code','symbol','name','status','action','total_score','daily_2buy_score','risk_pct','trigger_price','current_price'], page=page, anchor='watchlist')}</section>
<section class="panel" id="tail_focus"><h2>尾盘重点候选</h2>{render_table(s['tail_focus_rows'], ['symbol','code','stock_name','name','observe_quality','observe_priority','risk_pct','daily_2buy_score','current_price','trigger_price','risk_reasons'], page=page, anchor='tail_focus')}</section>
<section class="panel" id="paper"><h2>纸面交易候选</h2>{render_table(s['paper_candidates_rows'], ['trade_date','symbol','stock_name','signal_status','paper_status','buy_price','stop_loss','risk_pct','daily_2buy_score'], page=page, anchor='paper')}</section>
<section class="panel" id="plans"><h2>明日开盘复核计划</h2>{render_table(s['open_recheck_rows'], ['trade_date','plan_date','symbol','stock_name','plan_type','planned_buy_price','trigger_price','stop_loss','risk_pct','status'], page=page, anchor='plans')}</section>
<section class="panel" id="positions"><h2>持仓</h2>{render_table(s['positions_rows'], ['code','symbol','name','stock_name','status','buy_price','shares','current_price','pnl_pct','stop_loss'], page=page, anchor='positions')}</section>
</div><div>
<section class="panel" id="notifications"><h2>企业微信通知</h2><div class="list"><div class="item">最近通知数：{esc(notice.get('count',0))}</div><div class="item">最新时间：{esc(notice.get('latest_time') or '-')}</div><div class="item">类型分布：{esc(notice.get('category_counter') or {})}</div></div><div class="footer">这里只读解析日志里的通知记录，不发送企业微信、不读取 webhook。</div><br/>{render_notices(notice)}</section>
<section class="panel" id="scheduler"><h2>调度任务</h2>{render_jobs(s['scheduler_jobs'])}<div class="footer">配置文件：config/scheduler_jobs.json</div></section>
<section class="panel"><h2>调度心跳</h2><pre>{esc(heartbeat)}</pre></section>
<section class="panel" id="history"><h2>观察池新增/移除历史</h2><div class="list"><div class="item">历史快照文件数：{esc(hist.get('history_files',0))}</div>{hist_cards}</div><div class="footer">只读展示已有快照；dashboard 不再自动创建 watchlist_history。</div></section>
<section class="panel" id="rejections"><h2>最近拒绝/剔除</h2><div class="list">{reject_cards}</div></section>
<section class="panel" id="logs"><h2>日志文件</h2><div class="list"><div class="item"><strong>Scheduler logs</strong><div class="small">{esc(', '.join(s['scheduler_logs']) or '暂无')}</div></div><div class="item"><strong>Scan logs</strong><div class="small">{esc(', '.join(s['scan_logs']) or '暂无')}</div></div><div class="item"><strong>Reject CSV</strong><div class="small">{esc(', '.join(s['reject_logs']) or '暂无')}</div></div></div></section>
</div></div></div></body></html>"""
    return html.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            content = json.dumps(build_dashboard_state(), ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return
        if parsed.path not in ("/", "/index.html"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return
        try:
            page = int(parse_qs(parsed.query).get("page", ["1"])[0])
        except Exception:
            page = 1
        content = render_html(page=page)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"dashboard running at http://{HOST}:{PORT}/")
    print("说明：页面只读；每次刷新都会重新读取 data/reports、parquet 和日志。")
    server.serve_forever()
