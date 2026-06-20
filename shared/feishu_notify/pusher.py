"""
飞书推送器 — 异步攒批 + 同步快捷推送
支持高并发场景的攒批发送和简单的同步一次性推送

使用 DeepSeek V4 Pro 编写
"""
import asyncio
import logging
import time
import httpx
from .config import (
    FEISHU_WEBHOOK, FEISHU_ENABLED,
    BATCH_INTERVAL, MAX_BATCH_SIZE, MAX_MSG_LENGTH, RATE_LIMIT_RPS
)

log = logging.getLogger("feishu_notify")


class AsyncFeishuPusher:
    """异步飞书推送器 — 攒批模式，适合高并发告警场景"""

    def __init__(self, webhook_url=None, batch_interval=None, max_batch=None):
        self.webhook_url = webhook_url or FEISHU_WEBHOOK
        self.batch_interval = batch_interval or BATCH_INTERVAL
        self.max_batch = max_batch or MAX_BATCH_SIZE
        self.enabled = FEISHU_ENABLED and bool(self.webhook_url)
        self._http = None
        self._queue = None
        self._task = None
        self._pending = []
        self._last_send = 0.0

    async def start(self):
        """启动攒批 worker"""
        if not self.enabled:
            log.warning("FeishuPusher disabled (no webhook or FEISHU_ENABLED=false)")
            return
        self._http = httpx.AsyncClient(timeout=15)
        self._queue = asyncio.Queue()
        self._task = asyncio.create_task(self._worker())
        log.info("FeishuPusher started (batch=%.1fs, max_batch=%d)",
                 self.batch_interval, self.max_batch)

    async def _worker(self):
        """攒批 + 限流发送线程"""
        while True:
            try:
                text = await self._queue.get()
                if text is None:
                    break
                self._pending.append(text)
                # 清空队列中所有待发消息
                while not self._queue.empty():
                    try:
                        more = self._queue.get_nowait()
                        if more is None:
                            break
                        self._pending.append(more)
                    except asyncio.QueueEmpty:
                        break
            except asyncio.CancelledError:
                break

            if not self._pending:
                continue

            # 分批
            batch = self._pending[:self.max_batch]
            self._pending = self._pending[self.max_batch:]

            combined = "\n".join(batch)
            if len(combined) > MAX_MSG_LENGTH:
                combined = combined[:MAX_MSG_LENGTH - 50] + "\n... (截断)"

            # 限流
            elapsed = time.time() - self._last_send
            if elapsed < 1.0 / RATE_LIMIT_RPS:
                await asyncio.sleep(1.0 / RATE_LIMIT_RPS - elapsed)

            ok = await self._send(combined)
            self._last_send = time.time()
            if ok:
                log.info("Feishu sent OK (%d alerts, %d chars)", len(batch), len(combined))
            else:
                log.warning("Feishu send FAILED (%d alerts)", len(batch))

            await asyncio.sleep(self.batch_interval)

    async def push(self, text: str) -> bool:
        """入队推送（攒批模式）"""
        if not self.enabled or not self._queue:
            return False
        await self._queue.put(text)
        return True

    async def push_now(self, text: str) -> bool:
        """立即推送（跳过攒批）"""
        if not self.enabled:
            return False
        return await self._send(text)

    async def _send(self, text: str) -> bool:
        """发送 HTTP POST 到飞书 webhook"""
        if not text.strip():
            return True
        try:
            resp = await self._http.post(
                self.webhook_url,
                json={"msg_type": "text", "content": {"text": text}},
            )
            if resp.status_code == 200:
                body = resp.json()
                code = body.get("code", body.get("StatusCode", -1))
                if code == 0:
                    return True
                log.warning("Feishu API error code=%s: %s", code, body.get("msg", ""))
                return False
            log.warning("Feishu HTTP %d: %s", resp.status_code, resp.text[:200])
            return False
        except Exception as e:
            log.warning("Feishu request error: %s", e)
            return False

    async def close(self):
        """优雅关闭"""
        if self._queue:
            await self._queue.put(None)
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http:
            await self._http.aclose()
        log.info("FeishuPusher closed")


# 兼容别名
FeishuPusher = AsyncFeishuPusher
