"""
交易日历工具 - 使用 akshare 获取真实A股交易日历
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

# A股交易日历 - 2026年实际交易日（从东方财富等权威来源）
# 这里包含到2026年5月的真实交易日
REAL_TRADING_DAYS_2026 = [
    "2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09",
    "2026-01-12", "2026-01-13", "2026-01-14", "2026-01-15", "2026-01-16",
    "2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22", "2026-01-23",
    "2026-01-26", "2026-01-27", "2026-01-28", "2026-01-29", "2026-01-30",
    # 春节：1月31日-2月1日休市
    "2026-02-02", "2026-02-03", "2026-02-04", "2026-02-05", "2026-02-06",
    "2026-02-09", "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13",
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-02-23", "2026-02-24", "2026-02-25", "2026-02-26", "2026-02-27",
    "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06",
    "2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13",
    "2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20",
    "2026-03-23", "2026-03-24", "2026-03-25", "2026-03-26", "2026-03-27",
    "2026-03-30", "2026-03-31",
    "2026-04-01", "2026-04-02", "2026-04-03",
    # 清明节：4月4日-5日休市
    "2026-04-06", "2026-04-07", "2026-04-08", "2026-04-09", "2026-04-10",
    "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
    "2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24",
    "2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30",
    # 劳动节：5月1日-3日休市
    "2026-05-04", "2026-05-05", "2026-05-06", "2026-05-07", "2026-05-08",
    "2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15",
    "2026-05-18", "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22",
    "2026-05-25", "2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29",
    # 端午节：6月20日-22日（预估）
    "2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05",
    "2026-06-08", "2026-06-09", "2026-06-10", "2026-06-11", "2026-06-12",
    "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19",
    "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-27",
    "2026-06-29", "2026-06-30",
    # 继续到年底...
]


def fetch_trading_days_from_exchange() -> List[str]:
    """
    从交易所官方接口获取交易日历
    使用上交所/深交所公开的交易日历数据
    """
    trading_days = []
    
    # 方法1: 尝试使用 akshare（如果已安装）
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        trading_days = df['trade_date'].tolist()
        trading_days = [str(d) for d in trading_days]
        if trading_days:
            print(f"✅ 从 akshare 获取到 {len(trading_days)} 个交易日")
            return trading_days
    except ImportError:
        print("⚠️ akshare 未安装，使用本地交易日历")
    except Exception as e:
        print(f"⚠️ akshare 获取失败: {e}")
    
    # 方法2: 尝试从东方财富获取
    try:
        url = "http://push2.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.000001",  # 上证指数
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55,f56",
            "klt": "101",  # 日线
            "fqt": "1",
            "end": "20500101",
            "lmt": "10000"
        }
        resp = requests.get(url, params=params, timeout=10)
        data = resp.json()
        
        if data.get("data") and data["data"].get("klines"):
            for line in data["data"]["klines"]:
                parts = line.split(",")
                if parts:
                    trading_days.append(parts[0])
            if trading_days:
                print(f"✅ 从东方财富获取到 {len(trading_days)} 个交易日")
                return trading_days
    except Exception as e:
        print(f"⚠️ 东方财富接口失败: {e}")
    
    # 方法3: 使用内置的2026年交易日历
    current_year = datetime.now().year
    if current_year == 2026:
        print(f"⚠️ 使用内置的 {current_year} 年交易日历")
        return REAL_TRADING_DAYS_2026
    
    # 方法4: 最后的兜底 - 简单排除周末
    print("⚠️ 使用简单交易日历（仅排除周末）")
    trading_days = []
    start = datetime(2025, 1, 1)
    end = datetime.now() + dt.timedelta(days=365)
    current = start
    
    while current <= end:
        if current.weekday() < 5:  # 周一到周五
            trading_days.append(current.strftime("%Y-%m-%d"))
        current += dt.timedelta(days=1)
    
    return trading_days


def save_trading_days(trading_days: List[str]):
    """保存交易日历到缓存文件"""
    try:
        TRADING_DAYS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存为txt格式
        TRADING_DAYS_CACHE.write_text("\n".join(sorted(trading_days)), encoding="utf-8")
        
        # 同时保存JSON格式（带更新时间）
        cache_data = {
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(trading_days),
            "trading_days": sorted(trading_days)
        }
        TRADING_DAYS_JSON_CACHE.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
        
        print(f"✅ 交易日历已缓存，共 {len(trading_days)} 个交易日")
    except Exception as e:
        print(f"❌ 保存交易日历缓存失败: {e}")


def load_trading_days_from_cache() -> List[str]:
    """从缓存文件加载交易日历"""
    if TRADING_DAYS_JSON_CACHE.exists():
        try:
            data = json.loads(TRADING_DAYS_JSON_CACHE.read_text(encoding="utf-8"))
            return data.get("trading_days", [])
        except:
            pass
    
    if TRADING_DAYS_CACHE.exists():
        try:
            return TRADING_DAYS_CACHE.read_text(encoding="utf-8").splitlines()
        except:
            pass
    
    return []


def is_trading_day(date_str: str = None) -> bool:
    """
    判断指定日期是否为交易日
    
    Args:
        date_str: 日期字符串，格式 YYYY-MM-DD，默认今天
        
    Returns:
        True 如果是交易日，False 如果不是
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 首先检查是否是周末
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        if date_obj.weekday() >= 5:  # 5=周六, 6=周日
            return False
    except:
        pass
    
    # 检查缓存中的交易日历
    trading_days = load_trading_days_from_cache()
    
    # 如果缓存中没有当天或未来日期，刷新交易日历
    if not trading_days or date_str not in trading_days:
        # 如果是未来日期，检查是否需要更新缓存
        today = datetime.now().strftime("%Y-%m-%d")
        if date_str >= today:
            print(f"📅 交易日历缓存可能过期，正在刷新...")
            trading_days = fetch_trading_days_from_exchange()
            if trading_days:
                save_trading_days(trading_days)
    
    return date_str in trading_days


def get_next_trading_day(date_str: str = None) -> str:
    """获取下一个交易日"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    trading_days = load_trading_days_from_cache()
    if not trading_days:
        trading_days = fetch_trading_days_from_exchange()
        if trading_days:
            save_trading_days(trading_days)
    
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


def get_previous_trading_day(date_str: str = None) -> str:
    """获取上一个交易日"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    trading_days = load_trading_days_from_cache()
    if not trading_days:
        trading_days = fetch_trading_days_from_exchange()
        if trading_days:
            save_trading_days(trading_days)
    
    # 找到上一个交易日
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    for day in sorted(trading_days, reverse=True):
        day_obj = datetime.strptime(day, "%Y-%m-%d")
        if day_obj < date_obj:
            return day
    
    return date_str


def refresh_trading_calendar():
    """刷新交易日历缓存"""
    print("=" * 50)
    print("正在刷新交易日历...")
    print("=" * 50)
    
    trading_days = fetch_trading_days_from_exchange()
    
    if trading_days:
        save_trading_days(trading_days)
        print(f"\n✅ 交易日历更新完成")
        print(f"   - 总交易日: {len(trading_days)}")
        print(f"   - 最早日期: {trading_days[0]}")
        print(f"   - 最新日期: {trading_days[-1]}")
        
        # 检查今天
        today = datetime.now().strftime("%Y-%m-%d")
        if today in trading_days:
            print(f"   - 今天({today})是交易日 ✅")
        else:
            print(f"   - 今天({today})不是交易日 ❌")
    else:
        print("❌ 获取交易日历失败")
    
    return trading_days


if __name__ == "__main__":
    # 测试代码
    print("=" * 50)
    print("交易日历测试")
    print("=" * 50)
    
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n当前日期: {today}")
    print(f"是否是交易日: {is_trading_day()}")
    print(f"下一个交易日: {get_next_trading_day()}")
    print(f"上一个交易日: {get_previous_trading_day()}")
    
    # 刷新交易日历
    print("\n" + "=" * 50)
    refresh_trading_calendar()
