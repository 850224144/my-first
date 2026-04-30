# 方案 W：企业微信按 UTF-8 字节安全拆分 + 英文提醒中文化

## 修复点

1. 企业微信 markdown 不再按字符数拆分，改为按 UTF-8 bytes 拆分。
2. 单页默认控制在 2000 bytes 以内，明显低于 4096 bytes 限制。
3. `requests.post` 不再用 `json=`，改为 `ensure_ascii=False` 的 UTF-8 JSON，避免中文被转成 `\uXXXX` 导致请求体膨胀。
4. 所有项目通知统一走 `core/notify.py` 的 `send_markdown()`：扫描报告、观察池刷新、尾盘确认、买入触发、纸面交易、持仓风控、日报、系统异常。
5. 英文提醒 key 自动补中文，例如：
   - `volume_not_confirm` -> `量能未确认(volume_not_confirm)`
   - `volatility_not_contracting` -> `波动未明显收敛(volatility_not_contracting)`
   - `kline_quality_weak` -> `K线质量偏弱(kline_quality_weak)`
6. 日志打印 `chars` 和 `bytes`，排查时以 `bytes=` 为准。
7. Markdown 失败时自动 text 兜底。

## 安装

把压缩包放到项目根目录后执行：

```bash
cd /Users/liujunying/Downloads/project/my-first
unzip -o a_stock_planW_wechat_byte_fix_patch.zip -d .
python -m py_compile core/notify.py
```

## 重启

```bash
cd /Users/liujunying/Downloads/project/my-first
pkill -f "run_scheduler.py"
sleep 2
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```

## 验证

```bash
python run_scheduler.py --run-once watchlist_refresh_1030
python run_scan.py --daily-report

grep -R "企业微信返回" logs/scheduler/$(date +%Y%m%d)_*.log
```

正常日志里应看到：

```text
page=1/3 chars=xxx bytes=18xx
{'errcode': 0, 'errmsg': 'ok', '_bytes': 18xx}
```
