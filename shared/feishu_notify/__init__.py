"""
Feishu Notify — 飞书消息推送公用组件
供 smart-money-scanner、binance-monitor 及其他项目共用

用法:
    from feishu_notify import FeishuPusher, send_text

    pusher = FeishuPusher()
    await pusher.start()
    await pusher.push("Hello from Smart Money Scanner!")
    await pusher.close()

    # 或直接用便捷函数（适合简单场景）
    await send_text("一次性推送消息")

使用 DeepSeek V4 Pro 编写
"""
import sys
import os

# 确保父目录在 path 中以便其他项目 import
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from .pusher import FeishuPusher, AsyncFeishuPusher
from .config import FEISHU_WEBHOOK, FEISHU_ENABLED

__all__ = ["FeishuPusher", "AsyncFeishuPusher", "send_text", "FEISHU_WEBHOOK", "FEISHU_ENABLED"]

# 便捷函数：一次性同步推送
def send_text(text: str, webhook_url: str = None) -> bool:
    """同步推送文本到飞书（阻塞，适合脚本/定时任务）"""
    import httpx
    url = webhook_url or FEISHU_WEBHOOK
    if not url:
        return False
    try:
        resp = httpx.post(
            url,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=15
        )
        if resp.status_code == 200:
            body = resp.json()
            return body.get("code") == 0 or body.get("StatusCode") == 0
        return False
    except Exception:
        return False
