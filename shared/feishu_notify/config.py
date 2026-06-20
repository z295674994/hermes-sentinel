"""
飞书推送配置
支持环境变量覆盖
"""
import os

# 飞书机器人 Webhook URL
FEISHU_WEBHOOK = os.getenv(
    "FEISHU_WEBHOOK",
    "https://open.feishu.cn/open-apis/bot/v2/hook/9d68d8c3-f694-49b1-9340-dd0a82b79f78"
)

# 是否启用飞书推送
FEISHU_ENABLED = os.getenv("FEISHU_ENABLED", "true").lower() == "true"

# 批次设置
BATCH_INTERVAL = float(os.getenv("FEISHU_BATCH_INTERVAL", "2.0"))  # 攒批间隔（秒）
MAX_BATCH_SIZE = int(os.getenv("FEISHU_MAX_BATCH", "20"))           # 单批最大条数
MAX_MSG_LENGTH = int(os.getenv("FEISHU_MAX_MSG_LENGTH", "15000"))   # 单条消息最大长度

# 限流设置
RATE_LIMIT_RPS = float(os.getenv("FEISHU_RATE_LIMIT", "5.0"))  # 每秒最大请求数
