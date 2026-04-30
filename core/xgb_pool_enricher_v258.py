"""
v2.5.8 XGB 个股池增强器。

基于 live_pools:
- limit_up
- continuous_limit_up
- strong_stock
- yesterday_limit_up
- limit_up_broken
- limit_down

给候选补：
- theme_name
- sector_score
- leader_score
- yuanjun_score
- leader_type
- sector_state
- yuanjun_state
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from collections import defaultdict, Counter

try:
    from .data_normalizer import normalize_symbol
except Exception:
    def normalize_symbol(x): return str(x)


POOL_WEIGHT = {
    "limit_up": 16,
    "continuous_limit_up": 22,
    "strong_stock": 18,
    "yesterday_limit_up": 10,
    "limit_up_broken": -4,
    "limit_down": -20,
}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def item_symbol(item: Dict[str, Any]) -> Optional[str]:
    # XGB 主要在 surge_reason.symbol
    for path in [
        ("surge_reason", "symbol"),
        ("symbol",),
        ("stock_symbol",),
        ("code",),
    ]:
        cur: Any = item
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and cur:
            try:
                return normalize_symbol(cur)
            except Exception:
                return str(cur)
    return None


def item_name(item: Dict[str, Any]) -> Optional[str]:
    return item.get("stock_chi_name") or item.get("name") or item.get("stock_name")


def related_plates(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    sr = item.get("surge_reason") or {}
    plates = sr.get("related_plates") if isinstance(sr, dict) else None
    if isinstance(plates, list):
        return [x for x in plates if isinstance(x, dict)]
    return []


def item_theme(item: Dict[str, Any]) -> Optional[str]:
    plates = related_plates(item)
    if plates:
        for p in plates:
            name = p.get("plate_name") or p.get("name") or p.get("title")
            if name:
                return str(name)
    sr = item.get("surge_reason") or {}
    if isinstance(sr, dict):
        return sr.get("symbol_type") or sr.get("stock_reason", "")[:20] or None
    return None


def build_xgb_symbol_index(pools: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for pool_name, data in (pools or {}).items():
        if pool_name.startswith("_"):
            continue
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            sym = item_symbol(item)
            if not sym:
                continue
            entry = index.setdefault(sym, {
                "symbol": sym,
                "name": item_name(item),
                "pools": [],
                "items": {},
                "themes": [],
                "max_limit_up_days": 0,
                "max_boards": 0,
                "break_limit_up_times": 0,
                "change_percent": 0.0,
            })
            entry["pools"].append(pool_name)
            entry["items"][pool_name] = item
            th = item_theme(item)
            if th:
                entry["themes"].append(th)
            entry["max_limit_up_days"] = max(entry["max_limit_up_days"], _i(item.get("limit_up_days")))
            entry["max_boards"] = max(entry["max_boards"], _i(item.get("m_days_n_boards_boards")))
            entry["break_limit_up_times"] += _i(item.get("break_limit_up_times"))
            entry["change_percent"] = max(entry["change_percent"], _f(item.get("change_percent")) * 100 if abs(_f(item.get("change_percent"))) < 1 else _f(item.get("change_percent")))

    # 去重
    for entry in index.values():
        entry["pools"] = list(dict.fromkeys(entry["pools"]))
        if entry["themes"]:
            entry["theme_name"] = Counter(entry["themes"]).most_common(1)[0][0]
        else:
            entry["theme_name"] = None
    return index


def score_xgb_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    pools = set(entry.get("pools") or [])
    sector_score = 50.0
    leader_score = 50.0
    yuanjun_score = 50.0
    reasons: List[str] = []
    flags: List[str] = []

    for p in pools:
        sector_score += POOL_WEIGHT.get(p, 0)
        reasons.append(f"入选选股宝 {p}")

    limit_days = _i(entry.get("max_limit_up_days"))
    boards = _i(entry.get("max_boards"))
    break_times = _i(entry.get("break_limit_up_times"))
    chg = _f(entry.get("change_percent"))

    if limit_days >= 2:
        leader_score += min(25, limit_days * 8)
        reasons.append(f"连板/涨停天数 {limit_days}")
    if boards >= 2:
        leader_score += min(20, boards * 7)
        reasons.append(f"{boards}板高度")
    if "continuous_limit_up" in pools:
        leader_score += 12
        reasons.append("入选连板池")
    if "strong_stock" in pools:
        leader_score += 8
        reasons.append("入选强势股池")

    if "limit_up_broken" in pools:
        yuanjun_score += 10
        reasons.append("出现炸板/分歧池，具备分歧观察价值")
    if "limit_up" in pools or "strong_stock" in pools:
        yuanjun_score += 12
    if break_times > 0:
        yuanjun_score += min(12, break_times)
        reasons.append(f"炸板/回封次数 {break_times}")
    if chg >= 9:
        yuanjun_score += 8
    if "limit_down" in pools:
        sector_score -= 20
        leader_score -= 15
        yuanjun_score -= 20
        flags.append("入选跌停/风险池(limit_down)")

    sector_score = max(0, min(100, sector_score))
    leader_score = max(0, min(100, leader_score))
    yuanjun_score = max(0, min(100, yuanjun_score))

    if leader_score >= 80:
        leader_type = "xgb_leader"
    elif leader_score >= 65:
        leader_type = "xgb_front_runner"
    elif leader_score >= 55:
        leader_type = "xgb_active_stock"
    else:
        leader_type = "unknown"

    if yuanjun_score >= 75:
        yj_state = "YJ_XGB_CONFIRMED"
    elif yuanjun_score >= 60:
        yj_state = "YJ_XGB_WATCH"
    else:
        yj_state = "YJ_XGB_NEUTRAL"

    return {
        "sector_score": sector_score,
        "leader_score": leader_score,
        "yuanjun_score": yuanjun_score,
        "leader_type": leader_type,
        "sector_state": "xgb_pool_matched",
        "yuanjun_state": yj_state,
        "sector_reasons": reasons,
        "yuanjun_reasons": reasons,
        "sector_flags": flags,
        "yuanjun_flags": flags,
        "divergence_count": 1 if "limit_up_broken" in pools else 0,
        "rescue_candle_score": max(50.0, min(90.0, yuanjun_score)),
    }


def enrich_candidate_with_xgb_pools_v258(candidate: Dict[str, Any], pools: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(candidate)
    index = build_xgb_symbol_index(pools)
    try:
        sym = normalize_symbol(out.get("symbol") or out.get("code"))
    except Exception:
        sym = str(out.get("symbol") or out.get("code"))

    entry = index.get(sym)
    if not entry:
        # 不匹配时保持已有分，不伪造
        flags = list(out.get("risk_flags") or [])
        flags.append("未匹配选股宝个股池(xgb_pool_no_match)")
        out["risk_flags"] = list(dict.fromkeys(flags))
        return out

    scores = score_xgb_entry(entry)
    out.update(scores)
    out["theme_name"] = out.get("theme_name") or entry.get("theme_name")
    out["xgb_pools"] = entry.get("pools")
    out["xgb_pool_matched"] = True
    out["stock_name"] = out.get("stock_name") or entry.get("name")

    flags = list(out.get("risk_flags") or [])
    flags.extend(scores.get("sector_flags") or [])
    flags.extend(scores.get("yuanjun_flags") or [])
    out["risk_flags"] = list(dict.fromkeys(str(x) for x in flags if str(x)))
    return out


def xgb_enrich_report(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    matched = [c for c in candidates if c.get("xgb_pool_matched")]
    themes = Counter(c.get("theme_name") for c in matched if c.get("theme_name"))
    pools = Counter()
    for c in matched:
        for p in c.get("xgb_pools") or []:
            pools[p] += 1
    return {
        "total": len(candidates),
        "matched": len(matched),
        "matched_ratio": round(len(matched) / max(len(candidates), 1), 4),
        "top_themes": dict(themes.most_common(10)),
        "pool_hits": dict(pools),
    }
