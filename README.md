# my-first-main

这是一个 **A 股选股与交易辅助系统**，不是通用测试脚本仓库。

它主要做这些事：
- 扫描 A 股“二买 / 回踩确认”机会
- 按大盘、板块、周线、风险和价格触发做信号合成
- 生成纸面交易、持仓跟踪和日报
- 通过调度器在盘中、尾盘和收盘后自动跑任务
- 通过企业微信等方式推送结果

## 核心入口
- `run_scan.py`：扫描与信号生成
- `run_scheduler.py`：交易时段调度器
- `run_positions.py`：持仓跟踪
- `core/strategy.py`：二买评分
- `core/final_signal_engine.py`：最终信号合成

## 推荐流程
1. 先构建股票池
2. 盘中用 observe 扫描
3. 尾盘用 tail_confirm 找确认信号
4. 收盘后做 after_close 和日报
5. 持仓交给 run_positions.py 跟踪

## 依赖
```bash
pip install -r requirements.txt
```

## 说明
- 这个项目偏工程化选股/交易辅助，不是教学模板。
- 历史脚本很多，当前正式逻辑以 `run_scan.py`、`run_scheduler.py` 和 `core/` 下主模块为准。
- 新增了一个本地可视化页面 `dashboard.py`，可以直接看观察池、持仓、日报和调度状态。
