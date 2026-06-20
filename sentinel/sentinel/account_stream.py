"""
账户 WebSocket 流 — 实时持仓/订单/余额推送
使用 DeepSeek V4 Pro 编写
"""
import asyncio, json, logging, time
from typing import Callable, Dict, Optional
import aiohttp, websockets

log = logging.getLogger(__name__)
LISTEN_KEY_KEEPALIVE = 1800


class AccountStream:
    def __init__(self, config: dict, db):
        binance_cfg = config.get("binance", {})
        self.api_key = binance_cfg.get("api_key", "")
        self.api_secret = binance_cfg.get("api_secret", "")
        env = config.get("env", "testnet")
        rest_cfg = binance_cfg.get("rest", {})
        ws_cfg = binance_cfg.get("ws", {})
        account_ws = ws_cfg.get("account", {})
        self.rest_base = rest_cfg.get(env, rest_cfg.get("testnet", "https://testnet.binancefuture.com"))
        self.ws_base = account_ws.get(env, account_ws.get("testnet", "wss://stream.binancefuture.com/ws"))
        self.db = db
        self._listen_key: Optional[str] = None
        self._ws = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._reconnect_delay = 3
        self.positions: Dict[str, dict] = {}
        self.balances: Dict[str, float] = {}
        self.pending_orders: Dict[str, dict] = {}
        self.last_update: float = 0
        self._on_position: list = []
        self._on_order: list = []
        self._on_balance: list = []

    async def _create_listen_key(self) -> Optional[str]:
        url = f"{self.rest_base}/fapi/v1/listenKey"
        try:
            async with self._session.post(url, headers={"X-MBX-APIKEY": self.api_key}) as resp:
                data = await resp.json()
                key = data.get("listenKey")
                if key:
                    log.info("ListenKey: %s...%s", key[:6], key[-4:])
                    return key
        except Exception as e:
            log.error("ListenKey failed: %s", e)
        return None

    async def _keepalive_listen_key(self):
        if not self._listen_key: return
        try:
            async with self._session.put(f"{self.rest_base}/fapi/v1/listenKey",
                                          headers={"X-MBX-APIKEY": self.api_key}) as resp:
                if resp.status != 200:
                    log.warning("Keepalive: %d", resp.status)
        except Exception as e:
            log.error("Keepalive error: %s", e)

    async def _delete_listen_key(self):
        if not self._listen_key: return
        try:
            async with self._session.delete(f"{self.rest_base}/fapi/v1/listenKey",
                                             headers={"X-MBX-APIKEY": self.api_key}) as resp:
                log.info("ListenKey deleted: %d", resp.status)
        except Exception: pass
        self._listen_key = None

    async def _keepalive_loop(self):
        while self._running and self._listen_key:
            await asyncio.sleep(LISTEN_KEY_KEEPALIVE)
            await self._keepalive_listen_key()

    async def _connect_account_ws(self):
        url = f"{self.ws_base}/{self._listen_key}"
        log.info("Account WS: %s...", url[:50])
        self._ws = await websockets.connect(url, ping_interval=60, ping_timeout=10, close_timeout=5)

    async def _message_loop(self):
        async for raw in self._ws:
            try:
                event = json.loads(raw)
                await self._handle_event(event)
            except json.JSONDecodeError: continue

    async def _handle_event(self, event: dict):
        etype = event.get("e", "")
        self.last_update = time.time()
        if etype == "ACCOUNT_UPDATE":
            a = event.get("a", {})
            for b in a.get("B", []):
                self.balances[b.get("a", "")] = float(b.get("wb", 0))
            for p in a.get("P", []):
                s = p.get("s", "").lower()
                pos = {"symbol": s, "position_amount": float(p.get("pa", 0)),
                       "entry_price": float(p.get("ep", 0)), "unrealized_pnl": float(p.get("up", 0)),
                       "margin_type": p.get("mt", "cross"), "leverage": int(p.get("l", 1))}
                self.positions[s] = pos
                try:
                    self.db.execute("INSERT OR REPLACE INTO positions (symbol,timestamp,position_amount,entry_price,unrealized_pnl,margin_type,leverage) VALUES (?,?,?,?,?,?,?)",
                                    (s, int(time.time()), pos["position_amount"], pos["entry_price"], pos["unrealized_pnl"], pos["margin_type"], pos["leverage"]))
                except: pass
        elif etype == "ORDER_TRADE_UPDATE":
            o = event.get("o", {})
            order = {"symbol": o.get("s", "").lower(), "order_id": o.get("i", 0),
                     "side": o.get("S", ""), "status": o.get("X", ""),
                     "price": float(o.get("p", 0)), "executed_qty": float(o.get("z", 0)),
                     "avg_price": float(o.get("ap", 0)) if o.get("ap") else 0}
            self.pending_orders[str(order["order_id"])] = order
            if order["status"] == "FILLED":
                log.info("FILLED: %s %s %s %.4f@%.4f", order["side"], order["symbol"], order["type"] if "type" in order else "", order["executed_qty"], order["avg_price"])
        elif etype == "MARGIN_CALL":
            log.warning("MARGIN CALL!")

    def on_position_update(self, cb): self._on_position.append(cb)
    def on_order_update(self, cb): self._on_order.append(cb)
    def on_balance_update(self, cb): self._on_balance.append(cb)

    async def _run_forever(self):
        while self._running:
            try:
                if not self._listen_key:
                    self._listen_key = await self._create_listen_key()
                    if not self._listen_key:
                        await asyncio.sleep(30)
                        continue
                    asyncio.create_task(self._keepalive_loop())
                await self._connect_account_ws()
                await self._message_loop()
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning("Account WS disconnected: %s", e)
                self._listen_key = None
                await asyncio.sleep(self._reconnect_delay)

    async def start(self):
        if not self.api_key:
            log.warning("No BINANCE_API_KEY, account stream disabled")
            return
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        self._running = True
        log.info("AccountStream: %s", self.rest_base)
        asyncio.create_task(self._run_forever())

    async def stop(self):
        self._running = False
        await self._delete_listen_key()
        if self._session: await self._session.close()
