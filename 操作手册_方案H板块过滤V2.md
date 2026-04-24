# 方案 H：板块过滤 V2 操作说明

## 1. 本次改动

本补丁只升级板块过滤逻辑，不修改二买评分和历史 K 主链路。

新增/更新：

```text
core/sector.py      板块过滤 V2
run_scan.py         接入 --strict-sector；默认板块失败跳过过滤
core/weekly.py      保留周线过滤
core/universe.py    保留覆盖率函数修复
```

## 2. 板块过滤 V2 逻辑

旧逻辑：

```text
先拉所有行业成分 → 再算板块强度
```

新逻辑：

```text
先获取行业行情 → 计算强势板块 → 只拉强势板块成分 → 过滤股票池
```

这样请求更少，也更适合当前系统。

## 3. 数据源优先级

```text
1. 同花顺行业一览表：ak.stock_board_industry_summary_ths()
2. 东方财富行业板块：ak.stock_board_industry_name_em()
3. 本地缓存：data/sector_hot.parquet / data/sector_members.parquet
```

## 4. 板块热度评分

```text
涨跌幅排名：50 分
成交额排名：30 分
换手率/活跃度排名：20 分
```

字段缺失会自动降级，不会因为缺少成交额或换手率直接失败。

## 5. 调试模式与严格模式

默认是调试模式：

```text
板块数据失败 → 跳过板块过滤 → 继续周线过滤和二买扫描
```

严格模式：

```bash
python run_scan.py --mode tail_confirm --strict-sector
```

严格模式下：

```text
板块数据失败 / 强势板块无成分股 / 板块命中为空 → 停止扫描
```

## 6. 推荐测试命令

```bash
python run_scan.py --coverage
python run_scan.py --build-universe --workers 1
python run_scan.py --mode observe --workers 1
```

正式尾盘可以用：

```bash
python run_scan.py --mode tail_confirm --workers 1 --strict-sector
```

## 7. 正常输出

你应该看到类似：

```text
板块过滤报告：
  行业行情源：ths / eastmoney / cache
  强势板块数量：X
  保留比例：15%
  强势板块Top：
...
📊 板块过滤后剩余：Y / Z
周线过滤报告：...
```

如果板块失败但未加 `--strict-sector`，会看到：

```text
⚠️ 板块强度计算失败或强势板块为空，调试模式跳过板块过滤继续扫描
```

这是预期行为。
