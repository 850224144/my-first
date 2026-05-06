"""
实时数据预获取和缓存机制

核心思想：
1. 在需要实时数据之前提前获取（如9:40获取，10:00使用）
2. 如果实时获取失败，使用最近一次的成功数据
3. 避免使用旧的日线缓存数据
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import threading
import json
from pathlib import Path

from .realtime_guard import refresh_realtime_quotes, get_realtime_quote_summary

# 预获取缓存文件
PREFETCH_CACHE_DIR = Path(__file__).parent.parent / "data" / "realtime_prefetch"
PREFETCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 预获取配置
PREFETCH_CONFIG = {
    "observe_morning": {"prefetch_minutes": 20, "valid_minutes": 30},
    "observe_afternoon": {"prefetch_minutes": 20, "valid_minutes": 30},
    "tail_confirm": {"prefetch_minutes": 10, "valid_minutes": 15},
    "watchlist_refresh": {"prefetch_minutes": 10, "valid_minutes": 20},
}


def get_prefetch_cache_key(mode: str, codes: List[str]) -> str:
    """生成预获取缓存键"""
    codes_sorted = sorted(codes)
    codes_hash = hash(tuple(codes_sorted)) & 0xFFFFFFFF
    return f"{mode}_{codes_hash:08x}"


def save_prefetch_data(mode: str, codes: List[str], data: Dict[str, Any]) -> None:
    """保存预获取数据"""
    cache_key = get_prefetch_cache_key(mode, codes)
    cache_file = PREFETCH_CACHE_DIR / f"{cache_key}.json"
    
    # 确保目录存在
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    cache_data = {
        "mode": mode,
        "codes": codes,
        "data": data,
        "timestamp": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(minutes=PREFETCH_CONFIG.get(mode, {}).get("valid_minutes", 30))).isoformat()
    }
    
    cache_file.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_prefetch_data(mode: str, codes: List[str]) -> Optional[Dict[str, Any]]:
    """加载预获取数据"""
    cache_key = get_prefetch_cache_key(mode, codes)
    cache_file = PREFETCH_CACHE_DIR / f"{cache_key}.json"
    
    if not cache_file.exists():
        return None
    
    try:
        cache_data = json.loads(cache_file.read_text(encoding="utf-8"))
        
        # 检查是否过期
        expires_at = datetime.fromisoformat(cache_data["expires_at"])
        if datetime.now() > expires_at:
            cache_file.unlink(missing_ok=True)
            return None
        
        # 检查模式是否匹配
        if cache_data["mode"] != mode:
            return None
        
        # 检查代码列表是否匹配（允许子集）
        cached_codes = set(cache_data["codes"])
        requested_codes = set(codes)
        if not requested_codes.issubset(cached_codes):
            return None
        
        return cache_data["data"]
    except:
        cache_file.unlink(missing_ok=True)
        return None


def prefetch_realtime_data(mode: str, codes: List[str]) -> Dict[str, Any]:
    """
    预获取实时数据
    返回格式与 refresh_and_validate_realtime 相同
    """
    # 先尝试从缓存加载
    cached = load_prefetch_data(mode, codes)
    if cached:
        cached["using_prefetch"] = True
        return cached
    
    # 获取实时数据
    result = refresh_realtime_quotes(
        codes=codes,
        batch_size=50,
        batch_interval=1,
        min_success_rate=0.7,
        max_retries=2
    )
    
    # 保存到缓存
    save_prefetch_data(mode, codes, result)
    
    return result


def get_realtime_data_with_fallback(mode: str, codes: List[str]) -> Dict[str, Any]:
    """
    获取实时数据，支持预获取和降级
    策略：
    1. 先尝试获取实时数据
    2. 如果失败，使用预获取缓存
    3. 如果都没有，返回失败
    """
    # 尝试获取实时数据
    try:
        realtime_result = refresh_realtime_quotes(
            codes=codes,
            batch_size=50,
            batch_interval=1,
            min_success_rate=0.7,
            max_retries=2
        )
        
        if realtime_result.get("success_rate", 0) >= 0.7:
            # 实时数据成功，更新预获取缓存
            save_prefetch_data(mode, codes, realtime_result)
            return realtime_result
    except:
        pass
    
    # 实时数据失败，尝试使用预获取缓存
    prefetch_data = load_prefetch_data(mode, codes)
    if prefetch_data:
        prefetch_data["using_prefetch"] = True
        prefetch_data["message"] = f"{prefetch_data.get('message', '')} (使用预获取数据)"
        return prefetch_data
    
    # 都失败，返回空结果
    return {
        "requested": len(codes),
        "success": 0,
        "failed": len(codes),
        "success_rate": 0.0,
        "fresh": 0,
        "stale": len(codes),
        "missing": 0,
        "fresh_rate": 0.0,
        "newest_quote_time": "",
        "ok": False,
        "using_prefetch": False,
        "message": "实时数据获取失败，且无预获取缓存",
    }


# 后台预获取线程
class PrefetchScheduler:
    def __init__(self):
        self.running = False
        self.thread = None
        
    def schedule_prefetch(self, mode: str, codes: List[str], minutes_before: int = 20):
        """调度预获取任务"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(
                target=self._prefetch_worker,
                args=(mode, codes, minutes_before),
                daemon=True
            )
            self.thread.start()
    
    def _prefetch_worker(self, mode: str, codes: List[str], minutes_before: int):
        """预获取工作线程"""
        # 等待到指定时间前
        wait_seconds = max(0, minutes_before * 60 - 300)  # 提前5分钟开始
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        
        # 执行预获取
        print(f"[预获取] 开始为 {mode} 预获取 {len(codes)} 只股票的实时数据")
        result = prefetch_realtime_data(mode, codes)
        
        if result.get("success_rate", 0) >= 0.7:
            print(f"[预获取] 成功预获取 {result.get('success', 0)}/{len(codes)} 只股票")
        else:
            print(f"[预获取] 预获取失败: {result.get('message', '')}")
        
        self.running = False


# 全局预获取调度器
_prefetch_scheduler = PrefetchScheduler()


def schedule_realtime_prefetch(mode: str, codes: List[str]):
    """调度实时数据预获取"""
    config = PREFETCH_CONFIG.get(mode, {})
    minutes_before = config.get("prefetch_minutes", 20)
    
    _prefetch_scheduler.schedule_prefetch(mode, codes, minutes_before)