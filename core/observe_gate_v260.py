"""
v2.6.0 Observe 门控。

用途：
- 对 watchlist/final_signal 预览结果进行观察层分层
- 不写库
- 不买入
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .risk_quality_v260 import classify_observe_quality, summarize_observe_quality
from .stop_loss_diagnostics_v260 import diagnose_stop_loss_for_candidate


def apply_observe_gate_v260(
    items: List[Dict[str, Any]],
    *,
    daily_bars_map: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    daily_bars_map = daily_bars_map or {}
    out: List[Dict[str, Any]] = []

    for item in items:
        q = classify_observe_quality(item)
        sym = item.get("symbol") or item.get("code")
        bars = daily_bars_map.get(sym) or daily_bars_map.get(str(sym).split(".")[0]) if sym else None
        stop_diag = diagnose_stop_loss_for_candidate(item, daily_bars=bars)

        row = dict(item)
        row.update(q)
        row["stop_loss_diagnosis"] = stop_diag
        row["can_enter_tail_focus"] = q["observe_quality"] in {"tail_ready", "observe_keep"}
        row["should_deprioritize"] = q["observe_quality"] in {"noise_high_risk", "reject_bad_data"}
        out.append(row)

    return out


def summarize_observe_gate_v260(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    gated = apply_observe_gate_v260(items)
    s = summarize_observe_quality(gated)
    tail_focus = [x for x in gated if x.get("can_enter_tail_focus")]
    deprioritize = [x for x in gated if x.get("should_deprioritize")]
    compressible = [
        x for x in gated
        if (x.get("stop_loss_diagnosis") or {}).get("conclusion") == "possible_stop_compression"
    ]
    return {
        "observe_quality": s,
        "tail_focus_count": len(tail_focus),
        "deprioritize_count": len(deprioritize),
        "possible_stop_compression_count": len(compressible),
        "tail_focus": tail_focus[:30],
        "possible_stop_compression": compressible[:30],
    }
