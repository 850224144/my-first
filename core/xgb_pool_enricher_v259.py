"""
v2.5.9 XGB 个股池增强器。

基于 v2.5.8 的核心改动：
- XGB 匹配成功后，清理旧的 sector_data_missing / yuanjun_data_missing 等残留标签
- 避免日报/诊断还显示“板块数据缺失”
"""

from __future__ import annotations

from typing import Any, Dict, List
from collections import Counter

from .xgb_pool_enricher_v258 import (
    build_xgb_symbol_index,
    score_xgb_entry,
)

try:
    from .data_normalizer import normalize_symbol
except Exception:
    def normalize_symbol(x): return str(x)


STALE_MISSING_KEYWORDS = [
    "sector_data_missing",
    "yuanjun_data_missing",
    "sector_hot_no_match",
    "sector_hot_missing",
    "sector_hot_empty",
    "xgb_pool_no_match",
    "板块数据缺失",
    "援军数据缺失",
    "本地 sector_hot 未匹配",
]


def _clean_flags(flags: Any) -> List[str]:
    if flags is None:
        return []
    if isinstance(flags, str):
        items = [flags]
    elif isinstance(flags, list):
        items = [str(x) for x in flags if str(x)]
    else:
        items = [str(flags)]

    cleaned = []
    for item in items:
        if any(k in item for k in STALE_MISSING_KEYWORDS):
            continue
        cleaned.append(item)
    return list(dict.fromkeys(cleaned))


def clean_stale_missing_flags(candidate: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(candidate)
    for key in [
        "risk_flags",
        "blocking_flags",
        "downgrade_flags",
        "sector_flags",
        "yuanjun_flags",
        "signal_reasons",
        "sector_reasons",
        "yuanjun_reasons",
    ]:
        if key in out:
            out[key] = _clean_flags(out.get(key))
    return out


def enrich_candidate_with_xgb_pools_v259(candidate: Dict[str, Any], pools: Dict[str, Any]) -> Dict[str, Any]:
    """
    匹配成功：清理旧缺失标签并补评分。
    匹配失败：保留缺失/未匹配提示，但不硬拒绝。
    """
    out = dict(candidate)
    index = build_xgb_symbol_index(pools)
    try:
        sym = normalize_symbol(out.get("symbol") or out.get("code"))
    except Exception:
        sym = str(out.get("symbol") or out.get("code"))

    entry = index.get(sym)
    if not entry:
        flags = list(out.get("risk_flags") or [])
        if not any("xgb_pool_no_match" in str(x) for x in flags):
            flags.append("未匹配选股宝个股池(xgb_pool_no_match)")
        out["risk_flags"] = list(dict.fromkeys(str(x) for x in flags if str(x)))
        out["xgb_pool_matched"] = False
        return out

    out = clean_stale_missing_flags(out)
    scores = score_xgb_entry(entry)
    out.update(scores)

    out["theme_name"] = out.get("theme_name") or entry.get("theme_name")
    out["xgb_pools"] = entry.get("pools")
    out["xgb_pool_matched"] = True
    out["stock_name"] = out.get("stock_name") or entry.get("name")

    flags = list(out.get("risk_flags") or [])
    flags.extend(scores.get("sector_flags") or [])
    flags.extend(scores.get("yuanjun_flags") or [])
    flags.append("已匹配选股宝个股池(xgb_pool_matched)")
    out["risk_flags"] = list(dict.fromkeys(str(x) for x in flags if str(x)))

    reasons = list(out.get("sector_reasons") or [])
    reasons.extend(scores.get("sector_reasons") or [])
    if entry.get("theme_name"):
        reasons.append(f"题材匹配：{entry.get('theme_name')}")
    out["sector_reasons"] = list(dict.fromkeys(str(x) for x in reasons if str(x)))

    yj_reasons = list(out.get("yuanjun_reasons") or [])
    yj_reasons.extend(scores.get("yuanjun_reasons") or [])
    out["yuanjun_reasons"] = list(dict.fromkeys(str(x) for x in yj_reasons if str(x)))

    return out


def xgb_enrich_report_v259(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    matched = [c for c in candidates if c.get("xgb_pool_matched")]
    themes = Counter(c.get("theme_name") for c in matched if c.get("theme_name"))
    pools = Counter()
    for c in matched:
        for p in c.get("xgb_pools") or []:
            pools[p] += 1

    stale_flag_count = 0
    for c in candidates:
        flags = []
        for key in ["risk_flags", "blocking_flags", "downgrade_flags"]:
            v = c.get(key)
            if isinstance(v, list):
                flags.extend(v)
        if any(any(k in str(f) for k in STALE_MISSING_KEYWORDS) for f in flags):
            stale_flag_count += 1

    return {
        "total": len(candidates),
        "matched": len(matched),
        "matched_ratio": round(len(matched) / max(len(candidates), 1), 4),
        "top_themes": dict(themes.most_common(10)),
        "pool_hits": dict(pools),
        "stale_missing_flag_count": stale_flag_count,
    }
