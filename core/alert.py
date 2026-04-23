import requests
import json


def send_wechat(content, webhook_url):
    if not webhook_url:
        return False
    try:
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        headers = {"Content-Type": "application/json"}
        r = requests.post(webhook_url, data=json.dumps(data), headers=headers, timeout=5)
        result = r.json()
        if result.get("errcode") == 0:
            return True
        else:
            print(f"⚠️  企业微信推送失败: {result}")
            return False
    except Exception as e:
        print(f"⚠️  企业微信推送异常: {e}")
        return False


def send_dingtalk(content, webhook_url):
    if not webhook_url:
        return False
    try:
        data = {
            "msgtype": "text",
            "text": {"content": content}
        }
        headers = {"Content-Type": "application/json"}
        r = requests.post(webhook_url, data=json.dumps(data), headers=headers, timeout=5)
        return r.status_code == 200
    except:
        return False


def push_results(results, webhook_url=None, platform="wechat"):
    if not results:
        content = "【今日量化选股】\n今日无符合条件的股票"
    else:
        content = f"【今日量化选股】（共{len(results)}只）\n"
        for i, res in enumerate(results[:10]):
            content += (f"{i + 1}. {res['code']}\n"
                        f"   买价：{res['buy']} | 止损：{res['stop']}\n"
                        f"   仓位：{res['position']} | 概率：{res['prob']}\n\n")

    print("\n" + "=" * 70)
    print("📤 推送内容")
    print("=" * 70)
    print(content)
    print("=" * 70)

    if webhook_url:
        if platform == "wechat":
            success = send_wechat(content, webhook_url)
            if success:
                print("✅ 企业微信推送成功")
        elif platform == "dingtalk":
            success = send_dingtalk(content, webhook_url)
            if success:
                print("✅ 钉钉推送成功")