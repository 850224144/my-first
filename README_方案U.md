# 方案 U：全项目企业微信通知统一拆分分页 + 限流兜底

这版不是只改日报，而是把全项目所有企业微信通知统一收口到 `core/notify.py`：

- 扫描报告 / 交易信号：`TRADE_SCAN`
- 观察池刷新 / 尾盘确认 / 收盘复盘：仍走 `send_markdown`
- 买入触发 / 接近触发：`BUY_TRIGGER` / `NEAR_TRIGGER`
- 纸面交易 / 纸面买入 / 纸面退出：`PAPER_TRADE` / `PAPER_BUY` / `PAPER_EXIT`
- 持仓风控：`POSITION`
- 交易日报：`DAILY_REPORT`
- 系统异常：`SYSTEM_ERROR`

所有通知都会统一执行：

1. 简化 Markdown：去掉表格、代码块、复杂格式。
2. 长文自动拆分页，每页默认控制在 3000 字符以内。
3. 多页之间至少延迟 1.5 秒。
4. 跨进程至少间隔 1.2 秒，避免多个任务瞬间连发。
5. 如果企业微信 markdown 仍返回 40058 等错误，会自动降级为 text 兜底发送。
6. 每页都会打印企业微信返回值，方便排查。

## 安装

把压缩包放到项目根目录后执行：

```bash
cd /Users/liujunying/Downloads/project/my-first
unzip -o a_stock_planU_all_wechat_split_patch.zip -d .
```

## 检查

```bash
python -m py_compile core/notify.py
```

## 重启

```bash
pkill -f "run_scheduler.py"
sleep 2
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```
