# core/market.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple
import re

import requests


SINA_URL = "http://hq.sinajs.cn/list={symbol}"

SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn",
    "User-Agent": "Mozilla/5.0",
}


INDEXES = {
    "sh": {
        "symbol": "sh000001",
        "name": "上证指数",
        "history_aliases": ["sh000001", "SH000001", "000001.SH"],
    },
    "cyb": {
        "symbol": "sz399006",
        "name": "创业板",
        "history_aliases": ["sz399006", "SZ399006", "399006.SZ", "399006"],
    },
}


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        if x == "":
            return default
        return float(x)
    except Exception:
        return default


def _extract_sina_payload(text: str) -> str:
    """
    新浪返回形如：
    var hq_str_sh000001="上证指数,...";
    """
    if not text:
        return ""

    m = re.search(r'="(.*)"', text)
    if not m:
        return ""

    return m.group(1).strip()


def _find_date_time(fields: list[str]) -> Tuple[str, str]:
    date = ""
    time = ""

    for item in fields:
        item = str(item).strip()

        if re.match(r"^\d{4}-\d{2}-\d{2}$", item):
            date = item

        if re.match(r"^\d{2}:\d{2}:\d{2}$", item):
            time = item

    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    if not time:
        time = datetime.now().strftime("%H:%M:%S")

    return date, time


def _request_sina_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    严格使用新浪实时接口：
    http://hq.sinajs.cn/list=sh000001
    http://hq.sinajs.cn/list=sz399006
    """
    try:
        url = SINA_URL.format(symbol=symbol)
        resp = requests.get(url, headers=SINA_HEADERS, timeout=8)
        text = resp.text.strip()

        payload = _extract_sina_payload(text)
        if not payload:
            return None

        fields = payload.split(",")

        if len(fields) < 6:
            return None

        name = fields[0]
        open_price = _safe_float(fields[1])
        pre_close = _safe_float(fields[2])
        close = _safe_float(fields[3])
        high = _safe_float(fields[4])
        low = _safe_float(fields[5])

        date, time_str = _find_date_time(fields)

        if close is None or close <= 0:
            return None

        pct_chg = 0.0
        if pre_close and pre_close > 0:
            pct_chg = (close / pre_close - 1) * 100

        data_status = "intraday"
        try:
            hhmmss = int(time_str.replace(":", ""))
            if hhmmss >= 150000:
                data_status = "after_close"
        except Exception:
            pass

        return {
            "symbol": symbol,
            "name": name,
            "date": date,
            "time": time_str,
            "open": open_price,
            "pre_close": pre_close,
            "close": close,
            "high": high,
            "low": low,
            "pct_chg": pct_chg,
            "source": "sina_realtime",
            "data_status": data_status,
        }

    except Exception:
        return None


def _read_index_history_ma(aliases: list[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    尝试从本地 stock_daily 读取指数历史均线。
    没有指数历史也没关系，后面会用实时数据降级判断。
    """
    try:
        from core.data import get_db_connection

        con = get_db_connection()

        placeholders = ",".join(["?"] * len(aliases))

        rows = con.execute(
            f"""
            SELECT date, close
            FROM stock_daily
            WHERE code IN ({placeholders})
            ORDER BY date DESC
            LIMIT 60
            """,
            aliases,
        ).fetchall()

        con.close()

        if not rows:
            return None, None, None

        closes = [_safe_float(r[1], None) for r in rows]
        closes = [x for x in closes if x is not None]

        if len(closes) < 20:
            return None, None, str(rows[0][0])

        ma20 = sum(closes[:20]) / 20
        ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None

        return ma20, ma60, str(rows[0][0])

    except Exception:
        return None, None, None


def _build_index_state(key: str) -> Optional[Dict[str, Any]]:
    cfg = INDEXES[key]

    quote = _request_sina_quote(cfg["symbol"])
    if not quote:
        return None

    ma20, ma60, hist_date = _read_index_history_ma(cfg["history_aliases"])

    close = quote["close"]

    # 没有历史均线时，给 print 兜底，避免 risk_off。
    # 这时 classification 会主要参考实时涨跌幅。
    if ma20 is None:
        ma20 = close
    if ma60 is None:
        ma60 = close

    status = quote["data_status"]
    if hist_date:
        status = f"{status}+local_ma"
    else:
        status = f"{status}+no_ma"

    return {
        "date": quote["date"],
        "time": quote["time"],
        "close": close,
        "ma20": ma20,
        "ma60": ma60,
        "pct_chg": quote["pct_chg"],
        "source": quote["source"],
        "data_status": status,
    }


def _classify_market(sh: Optional[Dict[str, Any]], cyb: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not sh and not cyb:
        return {
            "state": "risk_off",
            "message": "大盘数据全部获取失败，默认不开新仓",
        }

    if not sh or not cyb:
        return {
            "state": "震荡",
            "message": "仅获取到部分指数数据，只做观察，仓位降低",
        }

    sh_close = _safe_float(sh.get("close"), 0) or 0
    sh_ma20 = _safe_float(sh.get("ma20"), sh_close) or sh_close
    sh_ma60 = _safe_float(sh.get("ma60"), sh_close) or sh_close
    sh_pct = _safe_float(sh.get("pct_chg"), 0) or 0

    cyb_close = _safe_float(cyb.get("close"), 0) or 0
    cyb_ma20 = _safe_float(cyb.get("ma20"), cyb_close) or cyb_close
    cyb_ma60 = _safe_float(cyb.get("ma60"), cyb_close) or cyb_close
    cyb_pct = _safe_float(cyb.get("pct_chg"), 0) or 0

    sh_strong = sh_close >= sh_ma20 and sh_close >= sh_ma60 * 0.995 and sh_pct >= -0.5
    cyb_strong = cyb_close >= cyb_ma20 and cyb_close >= cyb_ma60 * 0.995 and cyb_pct >= -0.5

    sh_weak = (sh_close < sh_ma20 * 0.98 and sh_close < sh_ma60 * 0.98) or sh_pct <= -1.5
    cyb_weak = (cyb_close < cyb_ma20 * 0.98 and cyb_close < cyb_ma60 * 0.98) or cyb_pct <= -1.5

    if sh_weak and cyb_weak:
        return {
            "state": "弱势",
            "message": "双指数弱势，不开新仓",
        }

    if sh_strong and cyb_strong:
        return {
            "state": "强势",
            "message": "双指数强势，可正常筛选，优先强势板块",
        }

    return {
        "state": "震荡",
        "message": "指数分化，只做最强板块，仓位减半",
    }


def get_market_state() -> Dict[str, Any]:
    """
    大盘三档：
    - 强势
    - 震荡
    - 弱势
    - risk_off：只有指数数据完全失败才返回

    数据源：
    - 新浪实时指数优先
    - 本地 stock_daily 指数历史均线可用则叠加
    """
    sh = _build_index_state("sh")
    cyb = _build_index_state("cyb")

    result = _classify_market(sh, cyb)

    result["sh"] = sh or {}
    result["cyb"] = cyb or {}

    return result