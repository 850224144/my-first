"""
板块 / 龙头评分模块。

v2.9.6 稳定性修复：
- 兼容 market_state / **kwargs
- 支持 list[dict] / dict(data=...) / pandas.DataFrame / polars.DataFrame
- 支持 pandas.Series / polars.Series / 列字典
- 避免 DataFrame / Series 参与布尔判断
- 不改变原板块/龙头评分策略
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Iterable
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


def _scalar(value: Any) -> Any:
    """
    把 pandas/polars Series、numpy scalar、单元素 list 变成普通标量。
    多元素序列保留第一个，主要用于 symbol/surge_reason/limit_up_days 这类单字段兜底。
    """
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    # numpy scalar
    try:
        if hasattr(value, "item") and callable(value.item):
            return value.item()
    except Exception:
        pass

    # polars Series
    try:
        if hasattr(value, "to_list") and callable(value.to_list):
            arr = value.to_list()
            return arr[0] if arr else None
    except Exception:
        pass

    # pandas Series / numpy array
    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            arr = value.tolist()
            if isinstance(arr, list):
                return arr[0] if arr else None
            return arr
    except Exception:
        pass

    if isinstance(value, (list, tuple)):
        return value[0] if value else None

    return value


def _value_to_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        return [value]

    try:
        if hasattr(value, "to_list") and callable(value.to_list):
            arr = value.to_list()
            return list(arr) if isinstance(arr, (list, tuple)) else [arr]
    except Exception:
        pass

    try:
        if hasattr(value, "tolist") and callable(value.tolist):
            arr = value.tolist()
            return list(arr) if isinstance(arr, (list, tuple)) else [arr]
    except Exception:
        pass

    if isinstance(value, (list, tuple)):
        return list(value)

    return [value]


def _dict_to_records(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    支持：
    - {"data": [...]}
    - {"code": [...], "name": [...]} 这种列字典
    - 单条 dict
    """
    if "data" in d:
        return _to_records(d.get("data"))

    values = {k: _value_to_list(v) for k, v in d.items()}
    max_len = max((len(v) for v in values.values()), default=0)

    # 多列数组，按列字典展开
    if max_len > 1:
        rows: List[Dict[str, Any]] = []
        for i in range(max_len):
            row = {}
            for k, arr in values.items():
                row[k] = arr[i] if i < len(arr) else None
            rows.append(row)
        return rows

    # 单条 dict，所有值标量化
    return [{k: _scalar(v) for k, v in d.items()}]


def _to_records(value: Any) -> List[Dict[str, Any]]:
    """
    把各种输入安全转为 list[dict]。
    重点：绝不对 DataFrame/Series 使用 if value / value or []。
    """
    if value is None:
        return []

    # polars DataFrame 优先。polars 也有 to_dict，但不是 records 语义。
    if hasattr(value, "to_dicts") and callable(value.to_dicts):
        try:
            records = value.to_dicts()
            if isinstance(records, list):
                return [dict(x) for x in records if isinstance(x, dict)]
        except Exception:
            pass

    # pandas DataFrame
    if hasattr(value, "to_dict") and callable(value.to_dict) and not isinstance(value, dict):
        try:
            records = value.to_dict("records")
            if isinstance(records, list):
                return [dict(x) for x in records if isinstance(x, dict)]
        except TypeError:
            try:
                d = value.to_dict()
                if isinstance(d, dict):
                    return _dict_to_records(d)
            except Exception:
                pass
        except Exception:
            pass

    if isinstance(value, dict):
        return _dict_to_records(value)

    if isinstance(value, (list, tuple)):
        out: List[Dict[str, Any]] = []
        for x in value:
            if isinstance(x, dict):
                out.extend(_dict_to_records(x))
            elif hasattr(x, "to_dicts") or hasattr(x, "to_dict"):
                out.extend(_to_records(x))
        return out

    return []


def _safe_float(value: Any, default: float = 0.0) -> float:
    value = _scalar(value)
    try:
        if value is None:
            return default
        return float(str(value).replace(",", ""))
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    value = _scalar(value)
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def _safe_get(item: Optional[Dict[str, Any]], key: str, default: Any = None) -> Any:
    if not isinstance(item, dict):
        return default
    return _scalar(item.get(key, default))


def _is_non_empty_item(item: Optional[Dict[str, Any]]) -> bool:
    return isinstance(item, dict) and len(item) > 0


def _first_item(*items: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    for item in items:
        if _is_non_empty_item(item):
            return item
    return {}


def extract_theme_from_surge_reason(surge_reason: Any) -> List[str]:
    """
    轻量解析 surge_reason。
    第一版不做复杂 NLP，只做常见分隔符切分。
    """
    surge_reason = _scalar(surge_reason)
    if surge_reason is None:
        return []
    if isinstance(surge_reason, dict):
        text = json.dumps(surge_reason, ensure_ascii=False)
    else:
        text = str(surge_reason)

    parts = re.split(r"[、,，;/；\|\n\r]+", text)
    themes: List[str] = []
    for p in parts:
        t = re.sub(r"[：:【】\[\]{}（）()]", " ", p).strip()
        if not t:
            continue
        if len(t) > 18:
            continue
        if re.fullmatch(r"\d+", t):
            continue
        themes.append(t)

    out: List[str] = []
    for t in themes:
        if t not in out:
            out.append(t)
    return out[:5]


def build_theme_stats(core_pools: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    根据选股宝核心股池构建主题热度统计。
    """
    if not isinstance(core_pools, dict):
        core_pools = {}

    def pool(name: str) -> List[Dict[str, Any]]:
        return _to_records(core_pools.get(name))

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
        sym = _safe_get(item, "symbol") or _safe_get(item, "code")
        if sym:
            try:
                s["symbols"].add(normalize_symbol(sym))
            except Exception:
                s["symbols"].add(str(sym))
        s["max_limit_up_days"] = max(s["max_limit_up_days"], _safe_int(_safe_get(item, "limit_up_days"), 0))

    for name, key in [
        ("limit_up", "limit_up_count"),
        ("continuous_limit_up", "continuous_limit_up_count"),
        ("strong_stock", "strong_stock_count"),
        ("limit_up_broken", "broken_count"),
        ("limit_down", "limit_down_count"),
    ]:
        for item in pool(name):
            themes = extract_theme_from_surge_reason(_safe_get(item, "surge_reason"))
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
    for x in _to_records(items):
        try:
            sym = _safe_get(x, "symbol") or _safe_get(x, "code")
            if normalize_symbol(sym) == std:
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
    market_state: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    给单只股票评分。
    market_state 仅兼容，不直接改变评分。
    """
    std = normalize_symbol(_scalar(symbol))
    pools = core_pools if isinstance(core_pools, dict) else {}

    def pool(name: str) -> List[Dict[str, Any]]:
        return _to_records(pools.get(name))

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
        if _is_non_empty_item(item):
            candidate_themes.extend(extract_theme_from_surge_reason(_safe_get(item, "surge_reason")))

    candidate_themes = list(dict.fromkeys([x for x in candidate_themes if x]))
    theme_name = None
    theme_heat_score = 50.0

    if candidate_themes:
        best = None
        for t in candidate_themes:
            st = theme_stats.get(t)
            if isinstance(st, dict) and (best is None or st["theme_heat_score"] > best["theme_heat_score"]):
                best = st
        if isinstance(best, dict):
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

    if _is_non_empty_item(lu_item):
        leader_score += 15
        reasons.append("入选涨停池")
    if _is_non_empty_item(cont_item):
        leader_score += 25
        reasons.append("入选连板池")
    if _is_non_empty_item(strong_item):
        leader_score += 15
        reasons.append("入选强势股池")
    if _is_non_empty_item(broken_item):
        leader_score -= 15
        flags.append("炸板风险(limit_up_broken)")
    if _is_non_empty_item(down_item):
        leader_score -= 30
        flags.append("跌停风险(limit_down)")

    source_item = _first_item(cont_item, lu_item, strong_item)
    limit_up_days = _safe_int(_safe_get(source_item, "limit_up_days"), 0)
    if limit_up_days >= 2:
        leader_score += min(20, limit_up_days * 5)
        reasons.append(f"连板高度 {limit_up_days}")

    if _safe_get(source_item, "first_limit_up_time"):
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
    market_state: Optional[str] = None,
    mode: str = "observe",
    config: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    兼容旧调用：filter_universe_by_strong_sector

    observe 阶段宽松，tail_confirm 阶段严格。
    candidates 可以是 list[dict] / pandas.DataFrame / polars.DataFrame / 列字典。
    """
    cfg = dict(config) if isinstance(config, dict) else {}
    if market_state is not None:
        cfg.setdefault("market_state", market_state)

    hard_reject_score = float(cfg.get("hard_reject_score", 45))
    leader_reject_score = float(cfg.get("leader_reject_score", 50))

    passed: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    scored: List[Dict[str, Any]] = []

    for c in _to_records(candidates):
        item = dict(c)
        symbol = _safe_get(item, "symbol") or _safe_get(item, "code")
        if not symbol:
            item["sector_flags"] = ["缺少股票代码(symbol_missing)"]
            rejected.append(item)
            scored.append(item)
            continue

        try:
            ss = score_sector_for_stock(
                symbol,
                core_pools=core_pools,
                trade_date=trade_date,
                market_state=market_state,
                config=cfg,
            )
        except Exception as exc:
            item["sector_score"] = item.get("sector_score", 50.0)
            item["leader_score"] = item.get("leader_score", 45.0)
            item["sector_state"] = item.get("sector_state", "sector_error")
            item["leader_type"] = item.get("leader_type", "unknown")
            item["theme_name"] = item.get("theme_name")
            old_flags = item.get("sector_flags")
            if not isinstance(old_flags, list):
                old_flags = []
            item["sector_flags"] = list(dict.fromkeys(old_flags + [f"板块评分异常(sector_score_error):{exc}"]))
            item["sector_reasons"] = item.get("sector_reasons") if isinstance(item.get("sector_reasons"), list) else []
            rejected.append(item)
            scored.append(item)
            continue

        item.update(ss)
        scored.append(item)

        hard_reject = (
            ss["sector_score"] < hard_reject_score
            or ss["leader_score"] < leader_reject_score
            or ss["leader_type"] == "follower"
        )

        if mode == "observe":
            hard_reject = ss["sector_score"] < 35 or ss["leader_score"] < 35

        if hard_reject:
            rejected.append(item)
        else:
            passed.append(item)

    return {"passed": passed, "rejected": rejected, "scored": scored}
