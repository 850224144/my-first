# 方案 T：企业微信长文拆分与限流推送

本增量包只替换 `core/notify.py`，解决企业微信 markdown 单条内容超过 4096 字符导致的 `40058 非法请求参数` 问题。

## 改动

1. 企业微信 markdown 单条安全长度默认控制在 3300 字符以内。
2. 长内容自动拆成多页，多次接口请求发送。
3. 多页之间默认延迟 1.2 秒，避免瞬间连发。
4. 自动简化复杂 Markdown：
   - 去掉代码块
   - 表格转简单分行
   - 标题转加粗
   - 引用转普通文本
5. 保留项目名前缀：`【A股二买交易助手｜...】`。
6. 每页推送都会打印企业微信返回值，方便排查。

## 可配置环境变量

```bash
export WECHAT_MARKDOWN_SAFE_LIMIT=3300
export WECHAT_NOTIFY_DELAY_SECONDS=1.2
```

不建议把 `WECHAT_MARKDOWN_SAFE_LIMIT` 设置太接近 4096。
