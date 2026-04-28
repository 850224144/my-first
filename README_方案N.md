# 方案 N 增量包：盘中实时K合成 + watchlist 高频跟踪 + 调度补跑

## 文件

```text
core/intraday.py
run_scan.py
run_scheduler.py
```

## 覆盖前备份

在项目根目录执行：

```bash
cp run_scan.py run_scan_bak_before_N.py
cp run_scheduler.py run_scheduler_bak_before_N.py
```

## 覆盖方式

解压后，将文件复制到项目根目录对应位置：

```bash
cp -f core/intraday.py ./core/intraday.py
cp -f run_scan.py ./run_scan.py
cp -f run_scheduler.py ./run_scheduler.py
```

## 语法检查

```bash
python -m py_compile core/intraday.py
python -m py_compile run_scan.py
python -m py_compile run_scheduler.py
```

## 手动测试

```bash
python run_scan.py --watchlist-refresh --workers 1
python run_scheduler.py --run-once watchlist_refresh_1030
python run_scheduler.py --list
```

## 后台重启调度器

建议用 caffeinate 防止 Mac 睡眠：

```bash
nohup caffeinate -dimsu python run_scheduler.py --replace >> logs/scheduler.out 2>&1 &
```

查看：

```bash
ps aux | grep run_scheduler.py | grep -v grep
tail -f logs/scheduler.out
```

## 这版解决的问题

### 1. 错过 09:45 怎么办？

- 如果调度器一直运行，只是晚了 30 分钟以内，会自动补跑。
- 如果 10:30 才启动调度器，启动时会检查今天是否已有 watchlist；没有就自动补跑 observe_morning。
- 如果 watchlist_refresh 发现今天 watchlist 为空，会回退为全市场 observe。

### 2. 盘中数据是否还是昨天的？

这版在 observe / tail_confirm / watchlist_refresh 前，会先刷新新浪实时行情，再把实时快照合成为今日临时K线，送进评分模型。

全市场约 900 只会按 400 只/批请求，批次间隔 10 秒。

### 3. 新时间表

```text
09:15  preflight
09:45  observe_morning 全市场观察
10:30  watchlist_refresh
11:20  watchlist_refresh
11:30  track_positions
13:20  observe_afternoon 全市场复扫
14:20  watchlist_refresh
14:50  tail_confirm
14:55  track_positions
17:30  after_close
20:00  track_positions
20:30  daily_report
22:30  night_cache_expand
```

## 注意

- 不自动下单。
- 盘中全市场刷新会多花 20-30 秒，这是正常的。
- 如果新浪实时接口失败，会继续使用本地 realtime_quote，并打印 warning。
- 如果电脑睡眠，仍可能错过任务，所以建议用 caffeinate。
