# 方案 Q：通知分级系统 + 方案 P 全量合并

本增量包包含方案 P 的全部能力，并新增企业微信通知分级：

1. 交易信号通知：observe / watchlist_refresh / tail_confirm / after_close
2. 持仓风控通知：run_positions.py --track / 新增持仓 / 关闭持仓
3. 系统异常通知：调度任务失败 / 超时 / 跳过
4. 日报通知：run_scan.py --daily-report 自动推送日报摘要

企业微信 webhook 默认写入：
https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2e322113-3ba9-4d90-8257-412971cbc55b

## 覆盖方式

用户要求不备份，直接覆盖。请在项目根目录执行：

```bash
unzip -o a_stock_planQ_notify_classification_patch.zip
```

或者手动复制：

```bash
cp -f core/notify.py ./core/notify.py
cp -f core/realtime_guard.py ./core/realtime_guard.py
cp -f core/daily_refresher.py ./core/daily_refresher.py
cp -f core/forward_stats.py ./core/forward_stats.py
cp -f core/alert.py ./core/alert.py
cp -f run_scan.py ./run_scan.py
cp -f run_scheduler.py ./run_scheduler.py
cp -f run_positions.py ./run_positions.py
```

## 检查

```bash
python -m py_compile core/notify.py
python -m py_compile core/realtime_guard.py
python -m py_compile core/daily_refresher.py
python -m py_compile core/forward_stats.py
python -m py_compile core/alert.py
python -m py_compile run_scan.py
python -m py_compile run_scheduler.py
python -m py_compile run_positions.py
```

## 测试通知

```bash
python run_scheduler.py --run-once watchlist_refresh_1030
python run_positions.py --track
python run_scan.py --daily-report
```

## 启动

```bash
pkill -f "run_scheduler.py"
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```

## 查看

```bash
ps aux | grep run_scheduler.py | grep -v grep
cat data/scheduler_state/scheduler.lock
tail -f logs/scheduler.out
tail -f logs/scheduler_heartbeat.log
```
