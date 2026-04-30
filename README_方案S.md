# 方案 S：纸面交易账本 + T+1 模拟交易 + 买入触发提醒

新增：
- core/paper_trader.py
- run_paper.py
- tools/apply_planS_patch.py

实现：
1. BUY_TRIGGERED / STRONG_BUY_TRIGGERED / NEAR_TRIGGER 三层买入触发。
2. 自动写入 data/paper_positions.parquet。
3. T+1 模式：买入当天不可卖，下一交易日才可卖。
4. 自动止损、目标1标记、目标2止盈、时间止损。
5. 自动写入 data/paper_trade_journal.parquet。
6. 统计胜率、平均盈利、平均亏损、盈亏比、平均持仓天数。
7. 日报中合并生成纸面交易报告。
8. 调度器新增纸面持仓跟踪任务。
9. 任务执行前写 job_heartbeat；原心跳保持每60分钟一次。

安装：
```bash
cd /Users/liujunying/Downloads/project/my-first
unzip -o /path/to/a_stock_planS_paper_trading_patch.zip -d .
python tools/apply_planS_patch.py
python -m py_compile core/paper_trader.py run_paper.py run_scan.py run_scheduler.py
```

启动/重启：
```bash
cd /Users/liujunying/Downloads/project/my-first
pkill -f "run_scheduler.py"
sleep 2
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```

检查：
```bash
ps aux | grep run_scheduler.py | grep -v grep
cat data/scheduler_state/scheduler.lock
tail -f logs/scheduler.out
tail -f logs/scheduler_heartbeat.log
```

测试：
```bash
python run_scheduler.py --run-once watchlist_refresh_1030
python run_paper.py --track
python run_paper.py --stats
```
