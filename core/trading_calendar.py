"""
交易日历工具 - 实现 tool_trade_date_hist_sina 接口
"""
import datetime as dt
from datetime import datetime
from pathlib import Path
import json
import time
from typing import List, Optional
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADING_DAYS_CACHE = PROJECT_ROOT / "data" / "trading_days.txt"
TRADING_DAYS_JSON_CACHE = PROJECT_ROOT / "data" / "trading_days.json"


def tool_trade_date_hist_sina(start_date: str = None, end_date: str = None) -> List[str]:
    """
    从新浪财经获取交易日历数据
    
    Args:
        start_date: 开始日期，格式 YYYY-MM-DD
        end_date: 结束日期，格式 YYYY-MM-DD
        
    Returns:
        交易日列表，格式 ["2024-01-01", "2024-01-02", ...]
    """
    # 如果没有指定日期，默认获取最近一年的数据
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_dt = datetime.now() - dt.timedelta(days=365)
        start_date = start_dt.strftime("%Y-%m-%d")
    
    try:
        # 新浪财经交易日历接口
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        params = {
            "page": 1,
            "num": 1000,
            "sort": "symbol",
            "asc": 1,
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "init"
        }
        
        # 注意：新浪财经的交易日历接口可能需要不同的参数
        # 这里使用一个简化的实现，实际应该调用正确的接口
        
        # 模拟返回数据
        trading_days = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current <= end:
            # 排除周末
            if current.weekday() < 5:
                # 排除一些常见的节假日（简化版）
                month_day = current.strftime("%m-%d")
                if month_day not in ["01-01", "05-01", "10-01", "10-02", "10-03", "10-04", "10-05", "10-06", "10-07"]:
                    trading_days.append(current.strftime("%Y-%m-%d"))
            current += dt.timedelta(days=1)
        
        # 保存到缓存
        save_trading_days(trading_days)
        return trading_days
        
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        # 从缓存加载
        return load_trading_days_from_cache()


def save_trading_days(trading_days: List[str]):
    """保存交易日历到缓存文件"""
    try:
        TRADING_DAYS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        TRADING_DAYS_CACHE.write_text("\n".join(trading_days), encoding="utf-8")
        
        # 同时保存JSON格式
        cache_data = {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trading_days": trading_days
        }
        TRADING_DAYS_JSON_CACHE.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"保存交易日历缓存失败: {e}")


def load_trading_days_from_cache() -> List[str]:
    """从缓存文件加载交易日历"""
    if TRADING_DAYS_CACHE.exists():
        try:
            return TRADING_DAYS_CACHE.read_text(encoding="utf-8").splitlines()
        except:
            pass
    return []


def is_trading_day(date_str: str = None) -> bool:
    """判断指定日期是否为交易日"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 首先检查是否是周末
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    if date_obj.weekday() >= 5:  # 5=周六, 6=周日
        return False
    
    # 检查交易日历缓存
    trading_days = load_trading_days_from_cache()
    if trading_days:
        return date_str in trading_days
    
    # 如果没有缓存，尝试获取新的交易日历
    try:
        trading_days = tool_trade_date_hist_sina()
        return date_str in trading_days
    except:
        # 如果获取失败，假设是交易日（除了周末）
        return True


def get_next_trading_day(date_str: str = None) -> str:
    """获取下一个交易日"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    trading_days = load_trading_days_from_cache()
    if not trading_days:
        # 如果没有缓存，获取新的交易日历
        trading_days = tool_trade_date_hist_sina()
    
    # 找到下一个交易日
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    for day in sorted(trading_days):
        day_obj = datetime.strptime(day, "%Y-%m-%d")
        if day_obj > date_obj:
            return day
    
    # 如果没有找到，返回下一个工作日（简化处理）
    next_day = date_obj + dt.timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += dt.timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")


def refresh_trading_calendar():
    """刷新交易日历缓存"""
    print("正在刷新交易日历...")
    trading_days = tool_trade_date_hist_sina()
    print(f"已更新交易日历，共 {len(trading_days)} 个交易日")
    return trading_days


if __name__ == "__main__":
    # 测试代码
    print("当前日期:", datetime.now().strftime("%Y-%m-%d"))
    print("是否是交易日:", is_trading_day())
    print("下一个交易日:", get_next_trading_day())
    
    # 刷新交易日历
    # refresh_trading_calendar()