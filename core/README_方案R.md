# 方案 R：项目通知命名 + 测试持仓清理 + 启动说明

## 本次变更

1. 企业微信所有消息统一增加项目前缀：
   - 【A股二买交易助手｜交易信号】
   - 【A股二买交易助手｜持仓风控】
   - 【A股二买交易助手｜交易日报】
   - 【A股二买交易助手｜系统异常】

2. 企业微信推送会打印返回值，例如：
   - ✅ 企业微信返回：{'errcode': 0, 'errmsg': 'ok'}

3. run_positions.py 新增清理测试持仓命令：
   - python run_positions.py --delete-position 002594
   - 只删除 open 持仓，不写入 trade_journal。

4. 新增持仓支持 source 标记：
   - --source manual/test/imported
   - 通过 note 写入，例如 source=test。

## 覆盖安装

在项目根目录执行：

```bash
unzip -o a_stock_planR_project_notify_patch.zip -d .
```

## 检查

```bash
python -m py_compile core/notify.py
python -m py_compile core/alert.py
python -m py_compile run_scan.py
python -m py_compile run_scheduler.py
python -m py_compile run_positions.py
```

## 启动/重启

```bash
cd /Users/liujunying/Downloads/project/my-first
pkill -f "run_scheduler.py"
sleep 2
rm -f data/scheduler_state/scheduler.lock
nohup caffeinate -dimsu python -u run_scheduler.py --replace </dev/null >> logs/scheduler.out 2>&1 &
```

## 验证通知

```bash
python run_scheduler.py --run-once watchlist_refresh_1030
python run_positions.py --track
python run_scan.py --daily-report
```

收到的企业微信消息应带有【A股二买交易助手｜...】前缀。
