# -*- coding: utf-8 -*-
"""
core/market.py

大盘三档过滤：
- strong：正常选股
- neutral：只做最强板块，仓位减半
- risk_off：不开新仓

原则：数据失败默认不交易。
"""

from __future__ import annotations

from typing import Dict, Any, Optional

import polars as pl

from core.data import get_data_with_status, SH_INDEX_CODE, CYB_INDEX_CODE, STATUS_NO_DATA
from core.feature import compute_features

MARKET_STRONG = "strong"
MARKET_NEUTRAL = "neutral"
MARKET_RISK_OFF = "risk_off"


def _index_snapshot(code: str) -> Optional[Dict[str, Any]]:
    df, status = get_data_with_status(code, bars=180)
    if df is None or len(df) < 80 or status == STATUS_NO_DATA:
        return None
    feat = compute_features(df)
    if feat.is_empty() or len(feat) < 30:
        return None
    last = feat.tail(1)
    close = float(last.select("close").item())
    ma20 = float(last.select("ma20").item())
    ma60 = float(last.select("ma60").item())
    ma20_slope = float(last.select("ma20_slope20").item())
    last_date = last.select("date").item()
    return {
        "code": code,
        "status": status,
        "date": str(last_date),
        "close": close,
        "ma20": ma20,
        "ma60": ma60,
        "above_ma20": close >= ma20,
        "above_ma60": close >= ma60,
        "ma20_slope20": ma20_slope,
    }


def get_market_state(debug: bool = True) -> Dict[str, Any]:
    """返回大盘状态。数据失败 => risk_off。"""
    sh = _index_snapshot(SH_INDEX_CODE)
    cyb = _index_snapshot(CYB_INDEX_CODE)

    if sh is None or cyb is None:
        if debug:
            print("⚠️ 大盘数据获取失败，默认不开新仓")
        return {
            "state": MARKET_RISK_OFF,
            "state_cn": "弱势/数据失败",
            "allow_new_position": False,
            "sector_top_pct": 0.0,
            "max_push": 0,
            "position_factor": 0.0,
            "reason": "大盘数据失败",
            "sh": sh,
            "cyb": cyb,
        }

    sh_strong = sh["above_ma20"] and sh["above_ma60"] and sh["ma20_slope20"] >= -0.005
    cyb_strong = cyb["above_ma20"] and cyb["above_ma60"] and cyb["ma20_slope20"] >= -0.005
    sh_weak = (not sh["above_ma20"]) and (not sh["above_ma60"])
    cyb_weak = (not cyb["above_ma20"]) and (not cyb["above_ma60"])

    if sh_strong and cyb_strong:
        state = MARKET_STRONG
        state_cn = "强势"
        allow = True
        sector_top_pct = 0.20
        max_push = 10
        position_factor = 1.0
        reason = "上证与创业板均站上 ma20/ma60，趋势环境较好"
    elif sh_weak and cyb_weak:
        state = MARKET_RISK_OFF
        state_cn = "弱势"
        allow = False
        sector_top_pct = 0.0
        max_push = 0
        position_factor = 0.0
        reason = "上证与创业板均弱于 ma20/ma60，不开新仓"
    else:
        state = MARKET_NEUTRAL
        state_cn = "震荡"
        allow = True
        sector_top_pct = 0.12
        max_push = 5
        position_factor = 0.5
        reason = "指数分化，只做最强板块，仓位减半"

    result = {
        "state": state,
        "state_cn": state_cn,
        "allow_new_position": allow,
        "sector_top_pct": sector_top_pct,
        "max_push": max_push,
        "position_factor": position_factor,
        "reason": reason,
        "sh": sh,
        "cyb": cyb,
    }

    if debug:
        print("=" * 70)
        print(f"📈 大盘状态：{state_cn} | {reason}")
        print(f"上证：{sh['date']} close={sh['close']:.2f} ma20={sh['ma20']:.2f} ma60={sh['ma60']:.2f} data={sh['status']}")
        print(f"创业板：{cyb['date']} close={cyb['close']:.2f} ma20={cyb['ma20']:.2f} ma60={cyb['ma60']:.2f} data={cyb['status']}")
        print("=" * 70)

    return result


# 兼容旧接口
def market_filter(debug: bool = True) -> bool:
    return get_market_state(debug=debug)["allow_new_position"]
