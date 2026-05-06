"""
板块 / 龙头评分模块。

v2.4.0 目标：
- 提供 sector_score、leader_score、sector_state、sector_flags
- 兼容旧调用 filter_universe_by_strong_sector
- 不把板块过滤一刀切做死
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Iterable, Tuple
from dataclasses import dataclass, asdict
import json
import re

try:
    from .data_normalizer import normalize_symbol, standardize_xgb_pool_item, clamp_score
except Exception:
    def normalize_symbol(x): return str(x)
    def standardize_xgb_pool_item(item, **kwargs): return dict(item)
    def clamp_score(value, low=0, high=100): return max(low, min(high, float(value)))


@dataclass
class SectorScore:
    symbol: str
    sector_score: float
    leader_score: float
    sector_state: str
    leader_type: str
    theme_name: Optional[str]
    sector_flags: List[str]
    sector_reasons: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def extract_theme_from_surge_reason(surge_reason: Any) -> List[str]:
    """
    轻量解析 surge_reason。
    第一版不做复杂 NLP，只做常见分隔符切分。
    """
    if surge_reason is None:
        return []
    if isinstance(surge_reason, dict):
        text = json.dumps(surge_reason, ensure_ascii=False)
    else:
        text = str(surge_reason)

    # 去掉明显解释文本，只保留候选关键词
    parts = re.split(r"[、,，;/；\|\n\r]+", text)
    themes: List[str] = []
    for p in parts:
        t = re.sub(r"[：:【】\[\]{}（）()]", " ", p).strip()
        if not t:
            continue
        # 避免太长的句子当主题
        if len(t) > 18:
            continue
        # 去除纯数字
        if re.fullmatch(r"\d+", t):
            continue
        themes.append(t)

    # 去重
    out: List[str] = []
    for t in themes:
        if t not in out:
            out.append(t)
    return out[:5]


def build_theme_stats(core_pools: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    根据选股宝核心股池构建主题热度统计。

    core_pools 可传：
    {
      "limit_up": [...],
      "continuous_limit_up": [...],
      "strong_stock": [...],
      "limit_up_broken": [...],
      "limit_down": [...]
    }

    也兼容 xgb_cache.get_core_pools 返回：
    {"limit_up": {"data": [...]}}
    """
    def pool(name: str) -> List[Dict[str, Any]]:
        v = core_pools.get(name, [])
        if isinstance(v, dict):
            return v.get("data") or []
        return v or []

    stats: Dict[str, Dict[str, Any]] = {}

    def add(theme: str, key: str, item: Dict[str, Any]):
        s = stats.setdefault(theme, {
            "theme": theme,
            "limit_up_count": 0,
            "continuous_limit_up_count": 0,
            "strong_stock_count": 0,
            "broken_count": 0,
            "limit_down_count": 0,
            "max_limit_up_days": 0,
            "symbols": set(),
        })
        s[key] += 1
        sym = item.get("symbol")
        if sym:
            try:
                s["symbols"].add(normalize_symbol(sym))
            except Exception:
                s["symbols"].add(str(sym))
        s["max_limit_up_days"] = max(s["max_limit_up_days"], _safe_int(item.get("limit_up_days"), 0))

    for name, key in [
        ("limit_up", "limit_up_count"),
        ("continuous_limit_up", "continuous_limit_up_count"),
        ("strong_stock", "strong_stock_count"),
        ("limit_up_broken", "broken_count"),
        ("limit_down", "limit_down_count"),
    ]:
        for item in pool(name):
            themes = extract_theme_from_surge_reason(item.get("surge_reason"))
            if not themes:
                continue
            for t in themes:
                add(t, key, item)

    for s in stats.values():
        score = (
            s["limit_up_count"] * 8
            + s["continuous_limit_up_count"] * 12
            + s["strong_stock_count"] * 5
            + s["max_limit_up_days"] * 5
            - s["broken_count"] * 6
            - s["limit_down_count"] * 10
        )
        s["theme_heat_score"] = clamp_score(score)
        s["symbols"] = list(s["symbols"])

    return stats


def _symbol_in_pool(symbol: str, items: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    std = normalize_symbol(symbol)
    for x in items or []:
        try:
            if normalize_symbol(x.get("symbol")) == std:
                return x
        except Exception:
            continue
    return None


def score_sector_for_stock(
    symbol: str,
    *,
    core_pools: Optional[Dict[str, Any]] = None,
    stock_daily_bars: Optional[Any] = None,
    trade_date: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    给单只股票评分。

    第一版数据优先来自选股宝股池：
    - limit_up
    - continuous_limit_up
    - strong_stock
    - limit_up_broken
    - limit_down
    """
    std = normalize_symbol(symbol)
    pools = core_pools or {}

    def pool(name: str) -> List[Dict[str, Any]]:
        v = pools.get(name, [])
        if isinstance(v, dict):
            return v.get("data") or []
        return v or []

    limit_up = pool("limit_up")
    cont = pool("continuous_limit_up")
    strong = pool("strong_stock")
    broken = pool("limit_up_broken")
    limit_down = pool("limit_down")

    flags: List[str] = []
    reasons: List[str] = []

    lu_item = _symbol_in_pool(std, limit_up)
    cont_item = _symbol_in_pool(std, cont)
    strong_item = _symbol_in_pool(std, strong)
    broken_item = _symbol_in_pool(std, broken)
    down_item = _symbol_in_pool(std, limit_down)

    theme_stats = build_theme_stats(pools)
    candidate_themes: List[str] = []

    for item in [lu_item, cont_item, strong_item, broken_item, down_item]:
        if item:
            candidate_themes.extend(extract_theme_from_surge_reason(item.get("surge_reason")))

    # 如果个股不在池内，第一版可能拿不到主题，给未知
    candidate_themes = list(dict.fromkeys([x for x in candidate_themes if x]))
    theme_name = None
    theme_heat_score = 50.0

    if candidate_themes:
        best = None
        for t in candidate_themes:
            s = theme_stats.get(t)
            if s and (best is None or s["theme_heat_score"] > best["theme_heat_score"]):
                best = s
        if best:
            theme_name = best["theme"]
            theme_heat_score = float(best["theme_heat_score"])
            reasons.append(f"所属题材 {theme_name} 热度分 {theme_heat_score:.1f}")
        else:
            theme_name = candidate_themes[0]
            flags.append("题材统计不足(sector_theme_stats_missing)")
    else:
        flags.append("题材归属不明确(sector_unknown)")

    sector_score = theme_heat_score
    leader_score = 45.0

    if lu_item:
        leader_score += 15
        reasons.append("入选涨停池")
    if cont_item:
        leader_score += 25
        reasons.append("入选连板池")
    if strong_item:
        leader_score += 15
        reasons.append("入选强势股池")
    if broken_item:
        leader_score -= 15
        flags.append("炸板风险(limit_up_broken)")
    if down_item:
        leader_score -= 30
        flags.append("跌停风险(limit_down)")

    # 连板天数加权
    source_item = cont_item or lu_item or strong_item or {}
    limit_up_days = _safe_int(source_item.get("limit_up_days"), 0)
    if limit_up_days >= 2:
        leader_score += min(20, limit_up_days * 5)
        reasons.append(f"连板高度 {limit_up_days}")

    # 首封时间越早越强，粗略加分：如果字段存在即可少量加分
    if source_item.get("first_limit_up_time"):
        leader_score += 5
        reasons.append("存在首次涨停时间，具备辨识度")

    leader_score = clamp_score(leader_score)

    if leader_score >= 85:
        leader_type = "total_leader"
    elif leader_score >= 70:
        leader_type = "turnover_leader"
    elif leader_score >= 60:
        leader_type = "front_runner"
    elif leader_score >= 50:
        leader_type = "normal_stock"
    else:
        leader_type = "follower"
        flags.append("非板块前排/后排跟风(leader_follower)")

    if sector_score >= 80:
        sector_state = "strong_mainline"
    elif sector_score >= 70:
        sector_state = "active_mainline"
    elif sector_score >= 55:
        sector_state = "active_sector"
    elif sector_score >= 45:
        sector_state = "neutral_sector"
    else:
        sector_state = "weak_sector"
        flags.append("板块强度不足(sector_weak)")

    return SectorScore(
        symbol=std,
        sector_score=round(clamp_score(sector_score), 2),
        leader_score=round(leader_score, 2),
        sector_state=sector_state,
        leader_type=leader_type,
        theme_name=theme_name,
        sector_flags=list(dict.fromkeys(flags)),
        sector_reasons=list(dict.fromkeys(reasons)),
    ).to_dict()


def filter_universe_by_strong_sector(
    candidates: List[Dict[str, Any]],
    *,
    core_pools: Optional[Dict[str, Any]] = None,
    trade_date: Optional[str] = None,
    mode: str = "observe",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    兼容旧调用：filter_universe_by_strong_sector

    observe 阶段宽松，tail_confirm 阶段严格。
    """
    cfg = config or {}
    hard_reject_score = float(cfg.get("hard_reject_score", 45))
    leader_reject_score = float(cfg.get("leader_reject_score", 50))

    passed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    scored: List[Dict[str, Any]] = []

    for c in candidates:
        symbol = c.get("symbol") or c.get("code")
        if not symbol:
            item = dict(c)
            item["sector_flags"] = ["缺少股票代码(symbol_missing)"]
            rejected.append(item)
            scored.append(item)
            continue

        ss = score_sector_for_stock(
            symbol,
            core_pools=core_pools,
            trade_date=trade_date,
            config=cfg,
        )
        item = dict(c)
        item.update(ss)
        scored.append(item)

        hard_reject = (
            ss["sector_score"] < hard_reject_score
            or ss["leader_score"] < leader_reject_score
            or ss["leader_type"] == "follower"
        )

        # observe 阶段先宽进
        if mode == "observe":
            hard_reject = ss["sector_score"] < 35 or ss["leader_score"] < 35

        if hard_reject:
            rejected.append(item)
        else:
            passed.append(item)

    return {"passed": passed, "rejected": rejected, "scored": scored}
