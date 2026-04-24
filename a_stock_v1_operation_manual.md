# A股类缠论二买 V1 操作手册

## 1. 系统定位

本系统用于扫描 A 股“健康上涨后的缩量回调二买”机会。当前版本不是完整缠论笔、线段、中枢系统，而是工程落地版：

```text
健康上涨 → 缩量回调 → 止跌企稳 → 尾盘/收盘温和突破确认
```

系统坚持四个原则：

1. 不做超跌反弹。
2. 不做妖股接力。
3. 不追涨停板。
4. 数据失败默认不交易。

## 2. 项目结构

```text
project/
├── run_scan.py              # 主入口
├── core/
│   ├── data.py              # 行情数据：腾讯主源，efinance / 新浪备用，DuckDB缓存
│   ├── universe.py          # 股票池构建与过滤
│   ├── market.py            # 大盘三档过滤：强势 / 震荡 / 弱势
│   ├── sector.py            # 板块强度过滤
│   ├── feature.py           # Polars 特征计算
│   ├── strategy.py          # 二买评分与交易计划
│   ├── model.py             # 模型排序，不决定买卖
│   ├── alert.py             # 企业微信 / 钉钉推送
│   ├── backtest.py          # 回测模块
│   └── logger.py            # 日志模块
├── data/
│   ├── stock_data.duckdb    # 本地行情数据库
│   └── universe.parquet     # 股票池缓存
└── logs/
    ├── scan_YYYYMMDD.log    # 运行日志
    └── rejects_YYYYMMDD.csv # 股票过滤/跳过原因明细
```

## 3. 安装依赖

建议在虚拟环境中执行：

```bash
pip install polars duckdb requests efinance xgboost scikit-learn joblib numpy
```

如果暂时不用模型排序，`xgboost` 和 `scikit-learn` 可以后装，但建议一次性装好。

## 4. 第一次使用

### 4.1 初始化数据库

```bash
python run_scan.py --init-db
```

正常输出：

```text
✅ 数据库初始化完成
```

### 4.2 小范围构建股票池

第一次不要直接全市场跑，先限制 300 只测试：

```bash
rm -f data/universe.parquet
python run_scan.py --build-universe --limit 300 --workers 6
```

如果能输出股票池前 20 行，说明基础链路正常。

### 4.3 小范围观察扫描

```bash
python run_scan.py --mode observe --limit 300 --workers 6
```

观察重点：

1. 大盘状态是否正常。
2. 强势行业是否能计算出来。
3. 是否正常打印推送内容。
4. `logs/` 下是否生成日志文件。

## 5. 日常使用命令

### 5.1 中午或盘中观察

```bash
python run_scan.py --mode observe --workers 6
```

用途：找出接近二买结构的候选股。这个模式不建议直接买，只用于观察名单。

### 5.2 尾盘确认

```bash
python run_scan.py --mode tail_confirm --workers 6
```

建议执行时间：14:45 - 14:55。

用途：寻找“尾盘温和突破确认”信号，这是实盘最接近买点的模式。

### 5.3 收盘后复盘

```bash
python run_scan.py --mode after_close --workers 6
```

建议执行时间：15:10 以后。

用途：确认当天日线结构是否成立，并生成第二天的交易计划。

### 5.4 自动判断模式

```bash
python run_scan.py --workers 6
```

系统会根据当前中国市场时间自动判断：

```text
14:45 - 14:55 → tail_confirm
15:10 以后 或 9:30 前 → after_close
其他时间 → observe
```

测试阶段建议明确写 `--mode`，避免自己不清楚跑的是哪种信号。

## 6. 股票池构建

### 6.1 使用缓存

默认会读取：

```text
data/universe.parquet
```

如果这个文件存在，系统会优先用缓存，速度更快。

### 6.2 强制重建股票池

```bash
rm -f data/universe.parquet
python run_scan.py --build-universe --workers 8
```

### 6.3 扫描时强制不用缓存

```bash
python run_scan.py --mode observe --no-cache-universe --workers 8
```

## 7. 日志功能

### 7.1 日志文件位置

每次运行会生成：

```text
logs/scan_YYYYMMDD.log
logs/rejects_YYYYMMDD.csv
```

`scan_YYYYMMDD.log` 记录运行过程、接口异常、线程异常、股票池缓存等信息。

`rejects_YYYYMMDD.csv` 记录股票被过滤或跳过的原因，适合排查类似：

```text
证券代码 xxxxxx 可能有误
```

### 7.2 reject CSV 字段说明

```text
time    时间
code    股票代码
name    股票名称
stage   发生阶段
reason  过滤/失败原因
detail  详细说明
```

常见 reason：

```text
no_kline_data              无行情数据，可能是代码不存在、接口失败、停牌或数据源不支持
listing_days_not_enough    有效交易天数不足250日
feature_not_enough         特征计算后数据不足
low_price                  最新价格低于3元
low_amount20               20日均成交额不足8000万
second_buy_not_match       二买结构不成立
invalid_plan               交易计划无效，例如止损距离过大
future_exception           多线程任务异常
```

### 7.3 调高日志级别

默认控制台日志级别为 `INFO`，文件日志总是记录更详细内容。

查看更多细节：

```bash
python run_scan.py --mode observe --limit 300 --workers 6 --log-level DEBUG
```

只看严重问题：

```bash
python run_scan.py --mode observe --workers 6 --log-level WARNING
```

指定日志目录：

```bash
python run_scan.py --mode observe --log-dir my_logs
```

指定日志文件：

```bash
python run_scan.py --mode observe --log-file logs/my_scan.log
```

## 8. 推送功能

### 8.1 企业微信推送

```bash
python run_scan.py --mode tail_confirm --workers 8 --webhook "你的企业微信机器人webhook"
```

也可以设置环境变量：

```bash
export WECHAT_WEBHOOK="你的企业微信机器人webhook"
python run_scan.py --mode tail_confirm --workers 8
```

### 8.2 钉钉推送

```bash
python run_scan.py --mode tail_confirm --platform dingtalk --webhook "你的钉钉机器人webhook"
```

没有 webhook 时，系统只会在终端打印推送内容，不会真正发送。

## 9. 信号含义

### 9.1 observe

观察信号。说明结构接近完成，但还不能当作确认买点。

### 9.2 tail_confirm

尾盘确认信号。适合 14:45 - 14:55 扫描。要求尾盘温和突破平台高点，且不是涨停、不是暴量冲高。

### 9.3 after_close

收盘确认信号。用于复盘和第二天计划。如果第二天高开超过 3%，原则上放弃。

## 10. 当前 V1 策略参数

```text
前置上涨幅度：25% - 80%
趋势周期：40 - 90 日
回调幅度：8% - 25%
回调周期：5 - 25 日
企稳周期：3 - 7 日
确认涨幅：1.5% - 6.5%
确认量比：1.1 - 1.8 倍20日均量
低价过滤：收盘价 < 3元剔除
成交额过滤：20日均成交额 < 8000万剔除
上市/有效交易天数：不足250日剔除
```

## 11. 常见问题排查

### 11.1 股票池为空

先看：

```bash
ls logs
cat logs/scan_YYYYMMDD.log
```

再看 reject 明细：

```bash
head -50 logs/rejects_YYYYMMDD.csv
```

常见原因：

1. efinance 获取股票列表失败。
2. 东方财富股票列表失败。
3. 走了代码号段兜底，产生大量不存在代码。
4. 行情接口失败。
5. 过滤条件太严格。

### 11.2 出现很多“证券代码可能有误”

这通常不是策略问题，而是候选池里包含不存在或数据源不支持的代码。

优先检查：

```bash
cat logs/rejects_YYYYMMDD.csv | grep no_kline_data | head
```

如果大量代码都是同一类号段，说明当前股票列表接口失败，系统走了代码号段兜底。

### 11.3 扫描没有候选股

这是正常现象。当前策略比较严格，只做健康趋势后的二买，不是每天都有信号。

排查方式：

```bash
cat logs/rejects_YYYYMMDD.csv | grep second_buy_not_match | head
```

如果大多数都是 `second_buy_not_match`，说明股票池正常，只是策略没有匹配。

### 11.4 大盘状态为弱势/数据失败

系统原则是数据失败默认不交易。大盘数据失败时不会产生买入确认信号。

### 11.5 使用了旧股票池缓存

如果你怀疑缓存不对，执行：

```bash
rm -f data/universe.parquet
python run_scan.py --build-universe --workers 8
```

## 12. 推荐测试流程

每天或每次改代码后，按这个顺序测试：

```bash
python run_scan.py --init-db
rm -f data/universe.parquet
python run_scan.py --build-universe --limit 300 --workers 6
python run_scan.py --mode observe --limit 300 --workers 6 --log-level DEBUG
python run_scan.py --mode after_close --limit 300 --workers 6
```

稳定后再全市场：

```bash
rm -f data/universe.parquet
python run_scan.py --build-universe --workers 8
python run_scan.py --mode observe --workers 8
```

## 13. 使用纪律

1. 中午扫描只能看观察信号，不能当确认买点。
2. 尾盘确认才接近实盘买点。
3. 收盘确认用于第二天计划。
4. 涨停不追。
5. 高开超过 3% 不追。
6. 数据失败不交易。
7. 不要因为一天没信号就放宽策略。

