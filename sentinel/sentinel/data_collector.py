"""
数据采集器 — REST 轮询 K线/OI/资金费率/多空比 + 动态币种
使用 DeepSeek V4 Pro 编写
"""
import asyncio, logging, time
from typing import Dict, List, Optional
import aiohttp

log = logging.getLogger(__name__)

KLINE_ENDPOINT = "/fapi/v1/klines"
OI_ENDPOINT = "/fapi/v1/openInterest"
FUNDING_ENDPOINT = "/fapi/v1/premiumIndex"
LSR_ENDPOINT = "/futures/data/globalLongShortAccountRatio"
EXCHANGE_INFO = "/fapi/v1/exchangeInfo"
RETENTION_DAYS = 7
MAX_REQ_PER_SEC = 20


class DataCollector:
    def __init__(self, config: dict, db, router):
        self.config = config
        self.db = db
        self.router = router
        self.symbols_limit = config.get("scan", {}).get("symbols_limit", 527)
        env = config.get("env", "testnet")
        rest_cfg = config.get("binance", {}).get("rest", {})
        self.fapi_base = rest_cfg.get(env, rest_cfg.get("testnet", "https://testnet.binancefuture.com"))
        self._session: Optional[aiohttp.ClientSession] = None
        self._symbols: List[str] = []
        self._running = False
        self._kline_interval = 60
        self._oi_interval = 900
        self._symbol_refresh_interval = 21600

    def _get_delay(self) -> float:
        if not self._symbols:
            return 0.05
        return max(0.02, 1.0 / MAX_REQ_PER_SEC)

    async def _fetch_exchange_info(self) -> List[str]:
        url = f"{self.fapi_base}{EXCHANGE_INFO}"
        try:
            async with self._session.get(url) as resp:
                data = await resp.json()
                symbols = []
                for s in data.get("symbols", []):
                    if (s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                            and s.get("status") == "TRADING"):
                        symbols.append(s["symbol"].lower())
                return sorted(symbols)
        except Exception as e:
            log.error("ExchangeInfo fetch failed: %s", e)
            return []

    async def _refresh_symbols_loop(self):
        while self._running:
            await asyncio.sleep(self._symbol_refresh_interval)
            try:
                new_symbols = await self._fetch_exchange_info()
                if not new_symbols:
                    continue
                new_symbols = new_symbols[:self.symbols_limit]
                old_set = set(self._symbols)
                new_set = set(new_symbols)
                added = new_set - old_set
                removed = old_set - new_set
                if added or removed:
                    self._symbols = new_symbols
                    log.info("Symbols: +%d -%d = %d total", len(added), len(removed), len(self._symbols))
                else:
                    log.debug("Symbols unchanged: %d", len(self._symbols))
            except Exception as e:
                log.error("Symbol refresh error: %s", e)

    async def _kline_loop(self):
        while self._running:
            await asyncio.sleep(self._kline_interval)
            try:
                await self._fetch_all_klines()
            except Exception as e:
                log.error("Kline poll error: %s", e)

    async def _fetch_all_klines(self):
        start = time.time()
        total = 0
        delay = self._get_delay()
        for symbol in self._symbols:
            kline = await self._fetch_kline(symbol)
            if kline:
                self._save_kline(symbol, kline)
                total += 1
            await asyncio.sleep(delay)
        elapsed = time.time() - start
        if elapsed > 30:
            log.info("Kline: %d/%d in %.1fs", total, len(self._symbols), elapsed)

    async def _fetch_kline(self, symbol: str) -> Optional[dict]:
        try:
            async with self._session.get(
                f"{self.fapi_base}{KLINE_ENDPOINT}",
                params={"symbol": symbol.upper(), "interval": "1m", "limit": 2}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if len(data) >= 1:
                        raw = data[-1]
                        return {"open_time": raw[0], "open": float(raw[1]), "high": float(raw[2]),
                                "low": float(raw[3]), "close": float(raw[4]), "volume": float(raw[5]),
                                "close_time": raw[6], "quote_volume": float(raw[7]), "trades": raw[8]}
        except Exception:
            pass
        return None

    def _save_kline(self, symbol: str, kline: dict):
        try:
            self.db.execute(
                "INSERT OR IGNORE INTO kline_1m (symbol, open_time, open, high, low, close, volume, quote_volume, trades_count) VALUES (?,?,?,?,?,?,?,?,?)",
                (symbol, kline["open_time"], kline["open"], kline["high"], kline["low"],
                 kline["close"], kline["volume"], kline["quote_volume"], kline["trades"]))
        except Exception:
            pass

    async def _oi_loop(self):
        while self._running:
            await asyncio.sleep(self._oi_interval)
            try:
                await self._fetch_all_snapshots()
                self.db.commit()
            except Exception as e:
                log.error("Snapshot error: %s", e)

    async def _fetch_all_snapshots(self):
        start = time.time()
        oi_count = lsr_count = 0
        for symbol in self._symbols:
            oi = await self._fetch_oi(symbol)
            funding = await self._fetch_funding(symbol)
            lsr = await self._fetch_lsr(symbol)
            if oi: self._save_oi(symbol, oi); oi_count += 1
            if funding: self._save_funding(symbol, funding)
            if lsr: self._save_lsr(symbol, lsr); lsr_count += 1
            await asyncio.sleep(self._get_delay())
        await self._cleanup_old_data()
        elapsed = time.time() - start
        log.info("Snapshot: %d OI + %d LSR in %.1fs", oi_count, lsr_count, elapsed)

    async def _fetch_oi(self, symbol: str) -> Optional[float]:
        try:
            async with self._session.get(f"{self.fapi_base}{OI_ENDPOINT}",
                                          params={"symbol": symbol.upper()}) as resp:
                if resp.status == 200:
                    return float((await resp.json()).get("openInterest", 0))
        except Exception:
            pass
        return None

    async def _fetch_funding(self, symbol: str) -> Optional[dict]:
        try:
            async with self._session.get(f"{self.fapi_base}{FUNDING_ENDPOINT}",
                                          params={"symbol": symbol.upper()}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {"mark_price": float(data.get("markPrice", 0)),
                            "funding_rate": float(data.get("lastFundingRate", 0)),
                            "next_funding_time": data.get("nextFundingTime", 0)}
        except Exception:
            pass
        return None

    async def _fetch_lsr(self, symbol: str) -> Optional[dict]:
        try:
            async with self._session.get(f"{self.fapi_base}{LSR_ENDPOINT}",
                                          params={"symbol": symbol.upper(), "period": "5m", "limit": 1}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        item = data[0]
                        return {"long_ratio": float(item.get("longAccount", 0)),
                                "short_ratio": float(item.get("shortAccount", 0)),
                                "long_short_ratio": float(item.get("longShortRatio", 0))}
        except Exception:
            pass
        return None

    def _save_oi(self, symbol, oi):
        try: self.db.execute("INSERT INTO oi_history (symbol,timestamp,open_interest) VALUES (?,?,?)", (symbol, int(time.time()), oi))
        except: pass

    def _save_funding(self, symbol, data):
        try: self.db.execute("INSERT INTO funding_history (symbol,timestamp,mark_price,funding_rate,next_funding_time) VALUES (?,?,?,?,?)", (symbol, int(time.time()), data["mark_price"], data["funding_rate"], data["next_funding_time"]))
        except: pass

    def _save_lsr(self, symbol, data):
        try: self.db.execute("INSERT INTO lsr_history (symbol,timestamp,long_ratio,short_ratio,long_short_ratio) VALUES (?,?,?,?,?)", (symbol, int(time.time()), data["long_ratio"], data["short_ratio"], data["long_short_ratio"]))
        except: pass

    async def _cleanup_old_data(self):
        try:
            cutoff = int(time.time()) - RETENTION_DAYS * 86400
            for t in ["oi_history", "funding_history", "lsr_history"]:
                self.db.execute(f"DELETE FROM {t} WHERE timestamp < ?", (cutoff,))
            self.db.commit()
        except: pass

    def get_oi_change(self, symbol: str, hours: int = 24) -> float:
        try:
            cutoff = int(time.time()) - hours * 3600
            rows = self.db.execute("SELECT open_interest FROM oi_history WHERE symbol=? AND timestamp >= ? ORDER BY timestamp", (symbol.lower(), cutoff)).fetchall()
            if len(rows) >= 2 and rows[0][0] > 0:
                return (rows[-1][0] - rows[0][0]) / rows[0][0] * 100
        except: pass
        return 0

    def get_funding_rate(self, symbol: str) -> float:
        try:
            row = self.db.execute("SELECT funding_rate FROM funding_history WHERE symbol=? ORDER BY timestamp DESC LIMIT 1", (symbol.lower(),)).fetchone()
            return row[0] if row else 0
        except:
            return 0

    async def start(self, symbols: List[str]):
        self._symbols = [s.lower() for s in symbols[:self.symbols_limit]]
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                               connector=aiohttp.TCPConnector(limit=MAX_REQ_PER_SEC))
        self._running = True
        log.info("DataCollector: %d symbols, base=%s", len(self._symbols), self.fapi_base)
        asyncio.create_task(self._kline_loop())
        asyncio.create_task(self._oi_loop())
        asyncio.create_task(self._refresh_symbols_loop())

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()
