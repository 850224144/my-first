# 方案 V：企业微信按 UTF-8 字节安全拆分 + 提醒中文化

本补丁覆盖 `core/notify.py`。项目里的扫描报告、交易信号、买入触发、纸面交易、持仓风控、交易日报、系统异常，只要走 `core.notify.send_markdown()`，都会统一生效。

## 核心修复

1. 企业微信 Markdown 限制按 UTF-8 字节计算，不再按 Python 字符数判断。
2. 单页默认压到 `3000 bytes` 以下，低于企业微信 4096 bytes 限制，避免中文 3 字节导致 40058。
3. 长文按字节自动拆页，不会按字符误判。
4. 多页推送默认间隔 1.6 秒，跨进程最低间隔 1.2 秒，禁止瞬间连发。
5. Markdown 失败时自动降级 text 兜底。
6. 统一简化 Markdown：不用表格、不用代码块，只保留加粗和简单分行。
7. 英文提醒 key 自动补中文，例如：
   - `volatility_not_contracting` → 波动未明显收敛(volatility_not_contracting)
   - `volume_not_confirm` → 量能未确认(volume_not_confirm)
   - `kline_quality_weak` → K线质量偏弱(kline_quality_weak)
8. 日志打印每页 `bytes`，不是只打印字符数。

## 安装

把压缩包放到项目根目录后执行：

```bash
cd /Users/liujunying/Downloads/project/my-first
unzip -o a_stock_planV_wechat_byte_safe_patch.zip -d .
python -m py_compile core/notify.py
```

如果你的本机 `python -m py_compile` 因环境卡住，可以直接用项目原来的 Python 环境执行；正常不会影响运行。

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
```

日志里应该看到：

```text
企业微信返回 page=1/3 bytes=xxxx
```
