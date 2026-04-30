"""
v2.9.0 日报聚合。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime as dt
import json

from .parquet_safe_writer_v263 import read_parquet_safe_v263
from .system_health_v290 import build_system_health_v290


def _read_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def count_rows(path: Path) -> int:
    try:
        return len(read_parquet_safe_v263(path))
    except Exception:
        return 0


def build_daily_report_v290(
    *,
    trade_date: Optional[str] = None,
    root: str | Path = ".",
) -> Dict[str, Any]:
    trade_date = trade_date or dt.date.today().isoformat()
    root = Path(root)

    observe_md = _read_text(root / "data" / "reports" / f"observe_gate_summary_{trade_date}.md")
    tail_md = _read_text(root / "data" / "reports" / f"tail_daily_section_v265_{trade_date}.md")
    buy_bridge_md = _read_text(root / "data" / "reports" / f"buy_bridge_summary_{trade_date}.md")

    observe_json = _read_json(root / "data" / "reports" / f"observe_gate_summary_{trade_date}.json")
    tail_json = _read_json(root / "data" / "reports" / f"tail_confirm_summary_v265_{trade_date}.json")
    buy_json = _read_json(root / "data" / "reports" / f"buy_bridge_summary_{trade_date}.json")

    positions_rows = count_rows(root / "data" / "positions.parquet")
    paper_candidates_rows = count_rows(root / "data" / "paper_trade_candidates.parquet")
    open_recheck_rows = count_rows(root / "data" / "trade_plan_open_recheck.parquet")

    health = build_system_health_v290(root)

    md = format_daily_report_md_v290(
        trade_date=trade_date,
        observe_json=observe_json,
        tail_json=tail_json,
        buy_json=buy_json,
        observe_md=observe_md,
        tail_md=tail_md,
        buy_bridge_md=buy_bridge_md,
        positions_rows=positions_rows,
        paper_candidates_rows=paper_candidates_rows,
        open_recheck_rows=open_recheck_rows,
        health=health,
    )

    payload = {
        "trade_date": trade_date,
        "observe": observe_json,
        "tail_confirm": tail_json,
        "buy_bridge": buy_json,
        "positions_rows": positions_rows,
        "paper_candidates_rows": paper_candidates_rows,
        "open_recheck_rows": open_recheck_rows,
        "health": health,
        "markdown": md,
    }

    report_dir = root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    md_path = report_dir / f"daily_report_v290_{trade_date}.md"
    json_path = report_dir / f"daily_report_v290_{trade_date}.json"
    health_path = report_dir / f"system_health_v290_{trade_date}.json"

    md_path.write_text(md, encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    health_path.write_text(json.dumps(health, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return {
        "trade_date": trade_date,
        "md_path": str(md_path),
        "json_path": str(json_path),
        "health_path": str(health_path),
        "payload": payload,
    }


def format_daily_report_md_v290(
    *,
    trade_date: str,
    observe_json: Dict[str, Any],
    tail_json: Dict[str, Any],
    buy_json: Dict[str, Any],
    observe_md: str,
    tail_md: str,
    buy_bridge_md: str,
    positions_rows: int,
    paper_candidates_rows: int,
    open_recheck_rows: int,
    health: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append(f"# A股二买交易助手日报 {trade_date}")
    lines.append("")
    lines.append("## 一、今日总览")
    lines.append("")
    lines.append(f"- 观察池候选：{observe_json.get('total', '-')}")
    lines.append(f"- 尾盘重点候选：{observe_json.get('tail_focus_count', '-')}")
    lines.append(f"- 低优先级候选：{observe_json.get('low_priority_count', '-')}")
    lines.append(f"- 尾盘买入信号：{tail_json.get('buy_count', 0)}")
    lines.append(f"- 尾盘观察：{tail_json.get('watch_count', 0)}")
    lines.append(f"- 尾盘拒绝：{tail_json.get('rejected_count', 0)}")
    lines.append(f"- 纸面交易候选累计：{paper_candidates_rows}")
    lines.append(f"- 开盘复核计划累计：{open_recheck_rows}")
    lines.append(f"- 持仓记录：{positions_rows}")
    lines.append("")

    lines.append("## 二、尾盘确认")
    lines.append("")
    if tail_md:
        # 去掉重复标题，保留主体
        body = tail_md.replace("## 尾盘确认", "").strip()
        lines.append(body)
    else:
        lines.append("- 今日没有尾盘确认结果。")
    lines.append("")

    lines.append("## 三、BUY / 明日计划")
    lines.append("")
    if buy_bridge_md:
        lines.append(buy_bridge_md)
    else:
        lines.append("- 今日无 BUY 桥接报告。")
    lines.append("")

    lines.append("## 四、系统健康")
    lines.append("")
    lines.append(f"- 状态：{health.get('status')}")
    missing = health.get("missing_required") or []
    if missing:
        lines.append(f"- 缺失文件：{', '.join(missing)}")
    else:
        lines.append("- 关键文件：正常")
    lines.append("")

    lines.append("## 五、结论")
    lines.append("")
    if tail_json.get("buy_count", 0):
        lines.append("- 今日出现买入信号，已进入纸面交易/明日计划流程。")
    else:
        lines.append("- 今日无买入信号，系统保持观察。")
    lines.append("- 不自动下单，所有结果用于辅助决策。")

    return "\n".join(lines)
