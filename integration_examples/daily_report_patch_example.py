"""
daily_report 接入示例。

目标：
- 增加评分统计
- 增加“为什么没买”的原因统计
"""

from collections import Counter


def build_v240_score_section(candidates):
    lines = []
    lines.append("【v2.4.0 板块/周线/援军评分统计】")
    lines.append("")

    if not candidates:
        lines.append("今日无评分候选。")
        return "\n".join(lines)

    total = len(candidates)
    strong_yj = sum(1 for x in candidates if float(x.get("yuanjun_score") or 0) >= 80)
    strong_sector = sum(1 for x in candidates if float(x.get("sector_score") or 0) >= 75)
    strong_weekly = sum(1 for x in candidates if float(x.get("weekly_score") or 0) >= 70)

    lines.append(f"评分候选数：{total}")
    lines.append(f"强援军候选：{strong_yj}")
    lines.append(f"强板块候选：{strong_sector}")
    lines.append(f"周线较强候选：{strong_weekly}")
    lines.append("")

    flag_counter = Counter()
    for c in candidates:
        for f in c.get("risk_flags") or []:
            flag_counter[f] += 1
        for f in c.get("sector_flags") or []:
            flag_counter[f] += 1
        for f in c.get("weekly_flags") or []:
            flag_counter[f] += 1
        for f in c.get("yuanjun_flags") or []:
            flag_counter[f] += 1

    if flag_counter:
        lines.append("未买/降级原因 Top10：")
        for k, v in flag_counter.most_common(10):
            lines.append(f"- {k}: {v}")

    return "\n".join(lines)
