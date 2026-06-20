"""
启动阶段 — 一次性 REST 拉取历史 K 线
使用 DeepSeek V4 Pro 编写
"""
import asyncio, logging, time
from typing import List
import aiohttp

log = logging.getLogger(__name__)

KLINE_ENDPOINT = "/fapi/v1/klines"
EXCHANGE_INFO = "/fapi/v1/exchangeInfo"
BATCH_DELAY = 0.2


class BootstrapLoader:
    def __init__(self, config: dict):
        bc = config.get("bootstrap", {})
        self.kline_bars = bc.get("kline_bars", 100)
        self.rate_limit = bc.get("rate_limit", 120)
        self.timeframes = bc.get("timeframes", ["1m", "1h", "4h", "1d", "1w"])
        self.symbols_limit = config.get("scan", {}).get("symbols_limit", 527)
        env = config.get("env", "testnet")
        rest_cfg = config.get("binance", {}).get("rest", {})
        self.fapi_base = rest_cfg.get(env, rest_cfg.get("testnet", "https://testnet.binancefuture.com"))

    async def fetch_exchange_info(self) -> List[str]:
        url = f"{self.fapi_base}{EXCHANGE_INFO}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    symbols = []
                    for s in data.get("symbols", []):
                        if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                            symbols.append(s["symbol"].lower())
                    log.info("Found %d USDT-M perpetual symbols", len(symbols))
                    return symbols[:self.symbols_limit]
        except Exception as e:
            log.error("Failed to fetch exchange info: %s", e)
            return []

    async def fetch_klines(self, symbol: str, interval: str, limit: int = 100) -> list:
        url = f"{self.fapi_base}{KLINE_ENDPOINT}"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 429:
                        log.warning("Rate limited, waiting 5s...")
                        await asyncio.sleep(5)
                        return await self.fetch_klines(symbol, interval, limit)
                    data = await resp.json()
                    if isinstance(data, dict) and data.get("code"):
                        log.warning("API error for %s: %s", symbol, data.get("msg", ""))
                        return []
                    return data
        except Exception as e:
            log.error("Fetch kline error %s %s: %s", symbol, interval, e)
            return []

    @staticmethod
    def kline_to_dict(raw: list) -> dict:
        return {"open_time": raw[0], "open": float(raw[1]), "high": float(raw[2]),
                "low": float(raw[3]), "close": float(raw[4]), "volume": float(raw[5]),
                "close_time": raw[6], "quote_volume": float(raw[7]), "trades": raw[8]}

    async def load_all(self, db) -> dict:
        symbols = await self.fetch_exchange_info()
        if not symbols:
            log.warning("No symbols found, skipping bootstrap")
            return {"symbols": [], "count": 0, "klines": 0}

        total = 0
        errors = 0
        start = time.time()

        for i, symbol in enumerate(symbols):
            for interval in self.timeframes:
                klines = await self.fetch_klines(symbol, interval, self.kline_bars)
                if not klines:
                    errors += 1
                    continue

                table = "kline_1m" if interval == "1m" else "kline_large"
                for k in klines:
                    d = self.kline_to_dict(k)
                    try:
                        if table == "kline_1m":
                            db.execute("INSERT OR IGNORE INTO kline_1m (symbol, open_time, open, high, low, close, volume, quote_volume, trades_count) VALUES (?,?,?,?,?,?,?,?,?)",
                                       (symbol, d["open_time"], d["open"], d["high"], d["low"], d["close"], d["volume"], d["quote_volume"], d["trades"]))
                        else:
                            db.execute("INSERT OR IGNORE INTO kline_large (symbol, interval, open_time, open, high, low, close, volume, quote_volume) VALUES (?,?,?,?,?,?,?,?,?)",
                                       (symbol, interval, d["open_time"], d["open"], d["high"], d["low"], d["close"], d["volume"], d["quote_volume"]))
                        total += 1
                    except Exception as e:
                        log.error("DB insert error: %s", e)
                await asyncio.sleep(BATCH_DELAY)

            db.commit()
            if (i + 1) % 50 == 0:
                elapsed = time.time() - start
                log.info("Bootstrap: %d/%d symbols, %d klines, %.1f min", i + 1, len(symbols), total, elapsed / 60)

        elapsed = time.time() - start
        log.info("Bootstrap complete: %d symbols, %d klines, %d errors, %.1f min",
                 len(symbols), total, errors, elapsed / 60)
        return {"symbols": symbols, "count": len(symbols), "klines": total, "errors": errors, "elapsed_min": elapsed / 60}
