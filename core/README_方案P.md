# 方案 P：数据新鲜度强校验 + 企业微信通知 + 收盘日线刷新 + 候选后续表现统计

## 覆盖内容

新增/覆盖：

- `core/realtime_guard.py`：盘中实时行情刷新与强校验
- `core/daily_refresher.py`：17:30 正式刷新已有日线缓存
- `core/forward_stats.py`：候选后续 1/3/5/10 日表现统计
- `core/alert.py`：推送中显示行情快照时间、实时行情成功率
- `run_scan.py`：接入实时行情强校验、收盘刷新、forward stats
- `run_scheduler.py`：企业微信 webhook 注入、after_close 改为正式刷新已有日线、状态检查

## 正确覆盖方式

不要拖拽替换整个 `core/` 目录，只复制指定文件：

```bash
cp -f core/realtime_guard.py ./core/realtime_guard.py
cp -f core/daily_refresher.py ./core/daily_refresher.py
cp -f core/forward_stats.py ./core/forward_stats.py
cp -f core/alert.py ./core/alert.py
cp -f run_scan.py ./run_scan.py
cp -f run_scheduler.py ./run_scheduler.py
```

## 语法检查

```bash
python -m py_compile core/realtime_guard.py
python -m py_compile core/daily_refresher.py
python -m py_compile core/forward_stats.py
python -m py_compile core/alert.py
python -m py_compile run_scan.py
python -m py_compile run_scheduler.py
```

## 测试

```bash
python run_scan.py --watchlist-refresh --workers 1
python run_scan.py --refresh-daily-existing --daily-limit 50 --daily-workers 1
python run_scheduler.py --run-once watchlist_refresh_1030
python run_scheduler.py --status
```

## 启动调度器

```bash
pkill -f "run_scheduler.py"
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```

## 企业微信通知

本补丁已经把你提供的企业微信 webhook 写入 `run_scheduler.py` 的默认环境变量。
如果不想把 webhook 放代码里，可改成启动前执行：

```bash
export WECHAT_WEBHOOK='你的企业微信机器人地址'
```

## 数据新鲜度

盘中 `observe`、`watchlist_refresh`、`tail_confirm` 前会刷新新浪实时行情。
如果实时行情成功率低于阈值，本轮会停止扫描，避免用旧数据出信号。

推送头部会显示：

```text
行情快照：2026-04-29 10:30:03 | 实时行情：56/56 成功率:100.00% fresh:56 stale:0
```

## 收盘刷新

17:30 的 `after_close` 已从历史扩容改成：

```bash
python run_scan.py --refresh-daily-existing --daily-limit 1200 --daily-workers 1
python run_scan.py --build-universe --workers 1
python run_scan.py --mode after_close --workers 1
```

22:30 仍保留历史扩容：

```bash
python run_scan.py --build-daily-cache --daily-limit 300 --daily-workers 1
```

## 候选表现统计

每次 observe / watchlist_refresh / tail_confirm / after_close 会记录候选到：

```text
data/signal_forward_stats.parquet
```

每日收盘或生成日报时会更新：

- 1日收益
- 3日收益
- 5日收益
- 10日收益
- 是否触发买点
- 是否触及止损
- 是否达到目标1/目标2
- 最大浮盈
- 最大回撤
