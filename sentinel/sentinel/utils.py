"""
Hermes Sentinel 公共工具函数
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import logging

log = logging.getLogger(__name__)


def safe_call(callback, *args):
    """安全调用回调：自动检测 async/sync，吞掉异常"""
    try:
        if asyncio.iscoroutinefunction(callback):
            asyncio.create_task(callback(*args))
        else:
            callback(*args)
    except Exception as e:
        log.error("Callback error: %s", e)
