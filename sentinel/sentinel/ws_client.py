"""
WebSocket 客户端 — 单连接管理，多 stream 订阅
使用 DeepSeek V4 Pro 编写
"""
import asyncio, json, logging, time
from typing import Callable, Dict, List, Optional
import websockets

log = logging.getLogger(__name__)


class BinanceWSClient:
    """币安合约 WebSocket 客户端"""

    def __init__(self, config: dict):
        env = config.get("env", "testnet")
        ws_cfg = config.get("binance", {}).get("ws", {})
        market = ws_cfg.get("market", {})
        self.url = market.get(env, market.get("testnet", "wss://stream.binancefuture.com/ws"))
        ws = config.get("ws", {})
        self.ping_interval = ws.get("ping_interval", 180)
        self.reconnect_delay = ws.get("reconnect_delay", 3)
        self.backoff = ws.get("reconnect_backoff", 2.0)
        self.max_delay = ws.get("max_reconnect_delay", 300)
        self.stream_limit = ws.get("stream_limit", 200)
        self._ws = None
        self._running = False
        self._streams: List[str] = []
        self._callbacks: Dict[str, List[Callable]] = {}
        self._stats = {"connected": False, "uptime_start": None, "messages_received": 0, "messages_dropped": 0, "reconnects": 0}

    def subscribe(self, stream: str, callback: Callable):
        s = stream.lower()
        if s not in self._streams:
            self._streams.append(s)
        if s not in self._callbacks:
            self._callbacks[s] = []
        self._callbacks[s].append(callback)
        log.info("Subscribed %s (%d callbacks)", stream, len(self._callbacks[s]))

    async def connect(self):
        delay = self.reconnect_delay
        while self._running:
            try:
                log.info("Connecting to %s ...", self.url)
                self._ws = await websockets.connect(self.url, ping_interval=self.ping_interval, ping_timeout=10, close_timeout=5, max_size=2**23)
                payload = {"method": "SUBSCRIBE", "params": list(self._streams), "id": int(time.time()*1000)}
                await self._ws.send(json.dumps(payload))
                log.info("Subscribed %d streams", len(payload["params"]))
                self._stats["connected"] = True
                self._stats["uptime_start"] = self._stats["uptime_start"] or time.time()
                delay = self.reconnect_delay
                await self._message_loop()
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                self._stats["connected"] = False
                self._stats["reconnects"] += 1
                log.warning("WS disconnected: %s, retry in %.1fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * self.backoff, self.max_delay)

    async def _message_loop(self):
        async for raw in self._ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                self._stats["messages_dropped"] += 1
                continue
            self._stats["messages_received"] += 1
            await self._route(msg)

    async def _route(self, msg: dict):
        """路由：支持 SUBSCRIBE 裸事件 + URL stream 包装两种格式"""
        stream = msg.get("stream", "")
        data = msg.get("data", msg)
        if stream:
            callbacks = self._callbacks.get(stream.lower(), [])
        else:
            callbacks = [cb for cbs in self._callbacks.values() for cb in cbs]
        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(data)
                else:
                    cb(data)
            except Exception as e:
                log.error("Callback error: %s", e)

    async def start(self):
        self._running = True
        log.info("WS client: %d streams, %s", len(self._streams), self.url)
        await self.connect()

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    @property
    def stats(self) -> dict:
        u = int(time.time() - self._stats["uptime_start"]) if self._stats["uptime_start"] and self._stats["connected"] else 0
        return {**self._stats, "uptime_seconds": u}
