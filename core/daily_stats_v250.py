"""
v2.5.0 日报统计模块。

目标：
- 统计最终信号分布
- 统计未买/降级原因
- 统计板块/周线/援军候选质量
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter, defaultdict


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def summarize_final_signals(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    status_counter = Counter()
    flag_counter = Counter()
    theme_counter = Counter()

    strong_yj = 0
    strong_weekly = 0
    strong_sector = 0

    for c in candidates or []:
        status = c.get("signal_status") or c.get("open_status") or "UNKNOWN"
        status_counter[status] += 1

        theme = c.get("theme_name")
        if theme:
            theme_counter[theme] += 1

        if _f(c.get("yuanjun_score")) >= 80:
            strong_yj += 1
        if _f(c.get("weekly_score")) >= 70:
            strong_weekly += 1
        if _f(c.get("sector_score")) >= 70:
            strong_sector += 1

        for k in ["blocking_flags", "downgrade_flags", "risk_flags", "sector_flags", "weekly_flags", "yuanjun_flags"]:
            v = c.get(k)
            if isinstance(v, list):
                for x in v:
                    flag_counter[str(x)] += 1
            elif isinstance(v, str) and v:
                flag_counter[v] += 1

    return {
        "total": len(candidates or []),
        "status_counter": dict(status_counter),
        "top_flags": dict(flag_counter.most_common(15)),
        "top_themes": dict(theme_counter.most_common(10)),
        "strong_yuanjun_count": strong_yj,
        "strong_weekly_count": strong_weekly,
        "strong_sector_count": strong_sector,
    }


def build_v250_daily_section(candidates: List[Dict[str, Any]]) -> str:
    s = summarize_final_signals(candidates)
    lines: List[str] = []

    lines.append("【v2.5.0 最终信号统计】")
    lines.append("")
    lines.append(f"候选总数：{s['total']}")
    lines.append(f"强援军候选：{s['strong_yuanjun_count']}")
    lines.append(f"周线强候选：{s['strong_weekly_count']}")
    lines.append(f"板块强候选：{s['strong_sector_count']}")
    lines.append("")

    lines.append("信号分布：")
    if s["status_counter"]:
        for k, v in s["status_counter"].items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- 无")

    lines.append("")
    lines.append("热门题材 Top10：")
    if s["top_themes"]:
        for k, v in s["top_themes"].items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- 无")

    lines.append("")
    lines.append("未买/降级/拒绝原因 Top15：")
    if s["top_flags"]:
        for k, v in s["top_flags"].items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- 无")

    return "\n".join(lines)
