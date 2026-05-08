# A股类缠论二买选股系统

这是一个 **A股选股与交易辅助系统**，专注于扫描"健康上涨后的缩量回调二买"机会。

## 🎯 四层完整架构

系统实现了从数据到复盘的完整闭环：

1. **数据层** - 多源行情数据，干净且全面
2. **策略层** - 缠论二买评分，明确规则可模拟
3. **决策信号层** - 触发买入/卖出/止损/止盈的纸面交易
4. **复盘进化层** ⭐ - 记录所有决策，定期复盘，持续改进

详见：[系统架构完整说明](SYSTEM_ARCHITECTURE_COMPLETE.md) | [快速开始复盘](QUICK_START_REVIEW.md)

## 核心功能

- **信号扫描**: 盘中观察、尾盘确认、收盘复盘三种模式
- **多维过滤**: 大盘三档 + 板块强度 + 周线趋势 + 风险控制
- **纸面交易**: 自动创建/跟踪/止损/止盈纸面持仓
- **持仓管理**: 实时跟踪实盘持仓，自动计算止损/止盈位
- **自动调度**: 盘中多时段任务自动执行
- **消息推送**: 企业微信/钉钉实时推送信号
- **回测系统**: 完整的回测引擎，支持多策略对比和可视化 ⭐ NEW
- **监控告警**: 系统资源和数据质量监控，多渠道告警 ⭐ NEW
- **交易复盘**: 记录所有决策，定期复盘分析，持续优化策略 ⭐ NEW

## 快速开始

### 1. 环境准备

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 初始化数据库

```bash
python run_scan.py --init-db
```

### 3. 构建股票池（首次建议小范围测试）

```bash
python run_scan.py --build-universe --limit 300 --workers 4
```

### 4. 运行扫描

```bash
# 盘中观察
python run_scan.py --mode observe --workers 4

# 尾盘确认（14:45-14:55）
python run_scan.py --mode tail_confirm --workers 4

# 收盘复盘（15:10后）
python run_scan.py --mode after_close --workers 4
```

### 5. 启动调度器

```bash
# 前台运行（调试）
python run_scheduler.py

# 后台运行（生产）
nohup caffeinate -dimsu python run_scheduler.py --replace >> logs/scheduler.out 2>&1 &
```

### 6. 可视化面板

```bash
python dashboard.py
# 访问 http://localhost:8088
```

### 7. 回测系统 ⭐ NEW

```bash
# 快速回测（50只股票）
python scripts/run_backtest.py --start-date 2024-01-01 --limit 50

# 完整回测
python scripts/run_backtest.py --start-date 2023-01-01

# 查看详细文档
cat docs/OPTIMIZATION_GUIDE.md
```

### 8. 监控系统 ⭐ NEW

```bash
# 单次检查
python scripts/run_monitor.py --check-all

# 守护进程（每5分钟检查一次）
python scripts/run_monitor.py --mode daemon --interval 300

# 查看详细文档
cat docs/OPTIMIZATION_GUIDE.md
```

### 9. 交易复盘系统 ⭐ NEW

```bash
# 复盘最近30天
python scripts/run_review.py --days 30

# 生成报告文件
python scripts/run_review.py --days 30 --output data/reports/review.md

# 更新错过机会跟踪
python scripts/run_review.py --update-missed --days 5

# 查看统计数据
python scripts/run_review.py --days 30 --stats

# 查看详细文档
cat docs/TRADE_JOURNAL_GUIDE.md
```

## 项目结构

```
project/
├── run_scan.py              # 主入口：扫描与信号生成
├── run_scheduler.py         # 调度器：交易时段自动任务
├── run_positions.py         # 持仓跟踪
├── dashboard.py             # 本地可视化面板
│
├── config/
│   ├── strategy_v2_5_0.yml  # 策略参数配置
│   └── scheduler_jobs.json  # 调度任务配置
│
├── core/                    # 核心模块
│   ├── data.py              # 行情数据获取与缓存
│   ├── universe.py          # 股票池构建与过滤
│   ├── feature.py           # Polars特征计算
│   ├── strategy.py          # 二买评分与交易计划
│   ├── final_signal_engine.py # 最终信号合成
│   ├── model.py             # XGBoost排序模型
│   ├── market.py            # 大盘三档过滤
│   ├── sector.py            # 板块强度过滤
│   ├── weekly.py            # 周线趋势过滤
│   ├── paper_trader.py      # 纸面交易管理
│   ├── position_tracker.py  # 实盘持仓跟踪
│   ├── notify.py            # 企业微信/钉钉推送
│   ├── logger.py            # 统一日志模块
│   └── backtest.py          # 回测模块
│
├── data/                    # 数据存储
│   ├── stock_data.duckdb    # 本地行情数据库
│   ├── universe.parquet     # 股票池缓存
│   ├── watchlist.parquet    # 观察名单
│   ├── positions.parquet    # 持仓数据
│   └── reports/             # 日报存储
│
├── logs/                    # 日志文件
│   ├── scan_*.log           # 运行日志
│   └── rejects_*.csv        # 过滤原因明细
│
└── archive/                 # 历史脚本归档
```

## 核心架构

### 数据流

```
数据源 → DuckDB缓存 → 特征计算 → 策略评分 → 信号合成 → 推送/纸面交易
    ↓
腾讯(主) → efinance(备) → 新浪(兜底)
```

### 策略逻辑

```
健康上涨（涨幅25%-80%，周期40-90日）
    ↓
缩量回调（幅度8%-25%，周期5-25日）
    ↓
止跌企稳（周期3-7日）
    ↓
尾盘/收盘温和突破确认
```

### 过滤层级

1. **基础过滤**: 上市天数≥250日，价格≥3元，20日均成交额≥8000万
2. **大盘过滤**: 强势正常仓位，震荡减半仓位，弱势不开仓
3. **板块过滤**: 优先强势板块，弱势板块排除
4. **周线过滤**: 周线上升趋势加分，下降趋势减分
5. **风险过滤**: 单笔最大风险≤8%，止损位距离过大排除

## 调度任务时间表

| 时间 | 任务 | 说明 |
|------|------|------|
| 09:15 | preflight | 数据源检查 |
| 09:45 | observe_morning | 全市场观察扫描 |
| 10:30 | watchlist_refresh | 观察名单刷新 |
| 11:20 | watchlist_refresh | 观察名单刷新 |
| 11:30 | track_positions | 持仓跟踪 |
| 13:20 | observe_afternoon | 全市场复扫 |
| 14:20 | watchlist_refresh | 观察名单刷新 |
| 14:40 | observe_gate_v270 | 观察门控扫描 |
| 14:50 | tail_confirm | 尾盘确认信号 |
| 14:52 | buy_bridge_v280 | 买入桥接 |
| 14:55 | track_positions | 持仓跟踪 |
| 17:30 | after_close | 收盘复盘 |
| 20:00 | track_positions | 持仓跟踪 |
| 20:30 | daily_report | 日报生成 |
| 22:30 | night_cache_expand | 夜间缓存扩展 |

## 配置说明

### 策略配置 (config/strategy_v2_5_0.yml)

```yaml
final_signal:
  daily_watch_score: 70      # 观察线
  daily_buy_score: 80        # 买入线
  daily_strong_score: 85     # 强势线
  max_risk_pct: 8            # 单笔最大风险%

  hard_reject_flags:         # 硬性拒绝条件
    - market_risk_off        # 大盘风险关闭
    - quote_not_fresh        # 行情不新鲜
    - risk_pct_too_high      # 风险比例过高
    - ...
```

### 推送配置

```bash
# 环境变量方式
export WECHAT_WEBHOOK="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"

# 或命令行参数
python run_scan.py --mode tail_confirm --webhook "xxx"
```

## 信号含义

| 信号 | 含义 | 操作建议 |
|------|------|----------|
| observe | 结构接近完成，但未确认 | 仅观察，不买入 |
| tail_confirm | 尾盘温和突破确认 | 接近买点，可考虑次日操作 |
| after_close | 收盘确认信号 | 用于次日计划，高开>3%放弃 |

## 使用纪律

1. 中午扫描只能看观察信号，不能当确认买点
2. 尾盘确认才接近实盘买点
3. 收盘确认用于第二天计划
4. 涨停不追
5. 高开超过3%不追
6. 数据失败不交易
7. 不要因为一天没信号就放宽策略

## 常见问题

### 股票池为空

```bash
cat logs/scan_*.log | head -50
cat logs/rejects_*.csv | head -50
```

常见原因：
- 股票列表接口失败
- 行情接口失败
- 过滤条件太严格

### 没有候选股

这是正常现象。策略较严格，不是每天都有信号。

```bash
cat logs/rejects_*.csv | grep second_buy_not_match | head
```

### 大盘状态为risk_off

数据源失败时系统默认不开仓，这是安全设计。

## 依赖说明

| 包 | 用途 |
|---|------|
| polars | 高性能数据处理（主力） |
| duckdb | 本地行情数据库 |
| requests | HTTP请求 |
| efinance | 东方财富数据源 |
| xgboost | 排序模型 |
| scikit-learn | 模型工具 |
| TA-Lib | 技术指标（可选） |

## 选股生命周期闭环

本系统实现了完整的选股生命周期管理，从选中到剔除全程跟踪：

```
observe(观察) → watch(关注) → triggered(触发) → holding(持仓) → exited(退出)
    ↓              ↓              ↓              ↓           ↓
  expired       cancelled      stopped       completed    时间退出
```

### 状态说明

| 状态 | 说明 | 触发条件 |
|------|------|----------|
| observe | 观察中 | 首次扫描到符合条件的股票 |
| watch | 关注中 | 尾盘确认信号，进入重点关注 |
| triggered | 已触发 | 买入信号触发，等待入场 |
| holding | 持仓中 | 已入场（纸面或实盘） |
| exited | 已退出 | 正常退出（时间/目标） |
| stopped | 已止损 | 触发止损退出 |
| completed | 已止盈 | 达到目标价退出 |
| expired | 已过期 | 超过N天未触发，自动过期 |
| cancelled | 已取消 | 手动取消或其他原因 |

### 生命周期管理

```bash
# 查看生命周期统计
python -c "from core.lifecycle_manager import get_lifecycle_manager; print(get_lifecycle_manager().get_statistics(30))"

# 生成生命周期报告
python -c "from core.lifecycle_manager import get_lifecycle_manager; print(get_lifecycle_manager().generate_report(30))"

# 过期旧观察记录
python -c "from core.lifecycle_manager import get_lifecycle_manager; print(f'已过期 {get_lifecycle_manager().expire_old_records(5)} 条')"
```

### 每日维护

系统会在每天 23:00 自动执行维护任务：
- 过期超过 5 天的观察记录
- 清理 30 天前的日志
- 优化数据库
- 生成统计报告

手动执行维护：

```bash
python scripts/daily_maintenance.py
```

## 性能优化

### 数据库优化

```bash
# 手动优化数据库
python -c "from core.performance_monitor import optimize_duckdb; optimize_duckdb()"

# 查看数据库统计
python -c "from core.performance_monitor import get_db_stats; import json; print(json.dumps(get_db_stats(), indent=2))"
```

### 缓存策略

- **DuckDB**: 存储历史K线数据，自动创建索引
- **Parquet**: 存储股票池、观察名单、持仓等中间数据
- **实时行情**: 优先使用新浪接口，按批次请求避免限流

### 清理技术债

```bash
# 清理备份文件、缓存、旧版本文件
python scripts/cleanup.py
```

## 增强版面板

启动增强版可视化面板（包含生命周期跟踪）：

```bash
python dashboard_v2.py
# 访问 http://localhost:8088
```

增强版面板功能：
- 完整的生命周期跟踪视图
- 性能监控面板
- 数据库状态统计
- 更清晰的交互体验

## 文档

- [操作手册 V2](a_stock_v2_operation_manual.md) - 最新使用指南
- [操作手册 V1](a_stock_v1_operation_manual.md) - 原始版本
- [方案N增量包](README_方案N.md) - 盘中实时K合成
- [优化功能指南](docs/OPTIMIZATION_GUIDE.md) - 回测系统与监控告警使用指南 ⭐ NEW
- [交易日志指南](docs/TRADE_JOURNAL_GUIDE.md) - 交易复盘系统使用指南 ⭐ NEW
- [系统架构完整说明](SYSTEM_ARCHITECTURE_COMPLETE.md) - 四层架构详解 ⭐ NEW
- [快速开始复盘](QUICK_START_REVIEW.md) - 5分钟快速上手复盘系统 ⭐ NEW
- [任务完成总结](TASK_COMPLETE_SUMMARY.md) - 四层架构实现总结 ⭐ NEW
- [实施总结](docs/IMPLEMENTATION_SUMMARY.md) - 系统优化实施总结

## 故障排查

### 交易日判断错误

如果调度器显示"今天不是交易日"但实际是交易日：

```bash
# 刷新交易日历
python core/trading_calendar.py
```

交易日历使用 akshare 从新浪财经获取官方数据。

### 股票池为空

```bash
# 检查日志
cat logs/scan_*.log | head -50

# 检查过滤原因
cat logs/rejects_*.csv | head -50

# 重建股票池
rm -f data/universe.parquet
python run_scan.py --build-universe --workers 4
```

### 数据库性能慢

```bash
# 优化数据库索引
python -c "from core.performance_monitor import optimize_duckdb; optimize_duckdb()"
```

## 免责声明

本项目仅供学习研究，不构成任何投资建议。股市有风险，投资需谨慎。

## License

MIT
