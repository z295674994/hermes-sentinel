"""
数据总线 — WS 帧分发到各引擎
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import json
import logging
import time

from sentinel.utils import safe_call
from collections import defaultdict
from typing import Dict, Optional

log = logging.getLogger(__name__)


class StreamRouter:
    """WS 数据分发总线
    
    职责:
    - 将 WS 原始帧解析为结构化数据
    - 按 stream 类型路由到对应引擎
    - 维护实时价格/交易量缓存
    - 提供引擎间共享数据接口
    """

    def __init__(self, config: dict):
        self.config = config
        self.symbols_count = config.get("scan", {}).get("symbols_limit", 527)

        # 实时价格缓存: {symbol: {"price": ..., "bid": ..., "ask": ..., "bid_qty": ..., "ask_qty": ...}}
        self._tickers: Dict[str, dict] = {}

        # aggTrade 环形缓冲区 (秒级引擎用)
        self._trade_buffers: Dict[str, list] = defaultdict(list)
        self._buffer_max = 200  # 每币最多保留 200 条成交

        # 1m K线最新缓存 (大级别引擎用)
        self._kline_1m: Dict[str, dict] = {}

        # OI 缓存 (从 markPrice 推算)
        self._oi: Dict[str, float] = {}

        # 回调注册
        self._on_ticker: list = []
        self._on_trade: list = []
        self._on_kline: list = []
        self._on_force_order: list = []
        self._stats = {
            "ticker_updates": 0,
            "trades_received": 0,
            "kline_updates": 0,
            "force_orders": 0,
            "last_update": None,
        }

    # ── 回调注册 ──────────────────────────

    def on_ticker(self, callback):
        """注册 bookTicker 回调: callback(symbol, ticker_data)"""
        self._on_ticker.append(callback)

    def on_trade(self, callback):
        """注册 aggTrade 回调: callback(symbol, trade_data)"""
        self._on_trade.append(callback)

    def on_kline(self, callback):
        """注册 kline 回调: callback(symbol, interval, kline_data)"""
        self._on_kline.append(callback)

    def on_force_order(self, callback):
        """注册 forceOrder 回调: callback(symbol, order_data)"""
        self._on_force_order.append(callback)

    # ── WS 消息处理 ──────────────────────

    async def handle_book_ticker(self, data: dict):
        """处理 bookTicker 消息"""
        symbol = data.get("s", "")
        ticker = {
            "symbol": symbol,
            "bid": float(data.get("b", 0)),
            "bid_qty": float(data.get("B", 0)),
            "ask": float(data.get("a", 0)),
            "ask_qty": float(data.get("A", 0)),
            "timestamp": data.get("E", int(time.time() * 1000)),
        }
        self._tickers[symbol] = ticker
        self._stats["ticker_updates"] += 1
        self._stats["last_update"] = time.time()

        for cb in self._on_ticker:
            safe_call(cb, symbol, ticker)

    async def handle_agg_trade(self, data: dict):
        """处理 aggTrade 消息"""
        symbol = data.get("s", "")
        trade = {
            "symbol": symbol,
            "price": float(data.get("p", 0)),
            "quantity": float(data.get("q", 0)),
            "is_buyer_maker": data.get("m", False),
            "timestamp": data.get("E", int(time.time() * 1000)),
        }
        # 环形缓冲区
        buf = self._trade_buffers[symbol]
        buf.append(trade)
        if len(buf) > self._buffer_max:
            buf.pop(0)

        self._stats["trades_received"] += 1

        for cb in self._on_trade:
            safe_call(cb, symbol, trade)

    async def handle_kline(self, data: dict):
        """处理 kline 消息"""
        k = data.get("k", {})
        symbol = k.get("s", data.get("s", ""))
        interval = k.get("i", "")
        kline = {
            "symbol": symbol,
            "interval": interval,
            "open_time": k.get("t", 0),
            "close_time": k.get("T", 0),
            "open": float(k.get("o", 0)),
            "high": float(k.get("h", 0)),
            "low": float(k.get("l", 0)),
            "close": float(k.get("c", 0)),
            "volume": float(k.get("v", 0)),
            "quote_volume": float(k.get("q", 0)),
            "trades": k.get("n", 0),
            "is_closed": k.get("x", False),
        }
        if interval == "1m":
            self._kline_1m[symbol] = kline
        self._stats["kline_updates"] += 1

        for cb in self._on_kline:
            safe_call(cb, symbol, interval, kline)

    async def handle_force_order(self, data: dict):
        """处理 forceOrder 消息"""
        o = data.get("o", {})
        symbol = o.get("s", data.get("s", ""))
        order_data = {
            "symbol": symbol,
            "side": o.get("S", ""),  # BUY/SELL
            "price": float(o.get("p", 0)),
            "quantity": float(o.get("q", 0)),
            "timestamp": o.get("T", data.get("E", 0)),
        }
        self._stats["force_orders"] += 1

        for cb in self._on_force_order:
            safe_call(cb, symbol, order_data)

    # ── 公共查询接口 ──────────────────────

    def get_ticker(self, symbol: str) -> Optional[dict]:
        return self._tickers.get(symbol)

    def get_trade_window(self, symbol: str, seconds: int = 30) -> list:
        """获取最近 N 秒的成交记录"""
        buf = self._trade_buffers.get(symbol, [])
        if not buf:
            return []
        cutoff = int(time.time() * 1000) - seconds * 1000
        return [t for t in buf if t["timestamp"] >= cutoff]

    def get_price(self, symbol: str) -> Optional[float]:
        t = self._tickers.get(symbol)
        if t:
            # 中间价
            return (t["bid"] + t["ask"]) / 2
        return None

    def get_volume_window(self, symbol: str, seconds: int = 5) -> float:
        """最近 N 秒总成交量"""
        trades = self.get_trade_window(symbol, seconds)
        return sum(t["quantity"] * t["price"] for t in trades)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    @property
    def active_symbols(self) -> list:
        return list(self._tickers.keys())
