"""
v2.6.0 observe gate 接入示例。

建议接入位置：
- observe_morning 后
- observe_afternoon 后
- watchlist_refresh 前

原则：
- 不删除原始 watchlist
- 给候选增加 observe_quality / observe_priority
- tail_confirm 只重点处理 tail_ready + observe_keep
"""

from core.observe_gate_v260 import apply_observe_gate_v260

def apply_observe_quality_to_watchlist(candidates):
    gated = apply_observe_gate_v260(candidates)
    tail_focus = [x for x in gated if x.get("can_enter_tail_focus")]
    low_priority = [x for x in gated if x.get("should_deprioritize")]
    return {
        "all": gated,
        "tail_focus": tail_focus,
        "low_priority": low_priority,
    }
