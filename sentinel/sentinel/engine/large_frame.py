"""
大级别引擎 — 15分钟周期评分 + K线缓存 + OI/资金费率追踪
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import json
import logging
import sqlite3
import time
from collections import defaultdict
from typing import Dict, List, Optional

from sentinel.engine.indicators import IndicatorsEngine, MTFIndicators
from sentinel.patterns.frames import PatternDetector, AccumulationScorer

log = logging.getLogger(__name__)


class LargeFrameEngine:
    """大级别引擎：K线缓存管理 + 15分钟周期评分"""

    def __init__(self, config: dict, router, db: sqlite3.Connection):
        self.config = config
        self.router = router
        self.db = db
        self.interval = config.get("scan", {}).get("interval_seconds", 900)
        self.symbols_limit = config.get("scan", {}).get("symbols_limit", 527)

        self.indicators = IndicatorsEngine(db, router)
        self.detector = PatternDetector(config)
        self.scorer = AccumulationScorer(self.detector)

        # K线缓存: {symbol: {interval: [klines]}}
        self._kline_cache: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))

        # OI 追踪: {symbol: {"oi": float, "oi_7d_ago": float}}
        self._oi_tracker: Dict[str, dict] = defaultdict(dict)

        # 大单追踪缓冲区: {symbol: [whale_trades]}
        self._whale_buffer: Dict[str, list] = defaultdict(list)

        # 清算缓冲区
        self._liquidation_buffer: Dict[str, list] = defaultdict(list)

        # 最近评分结果
        self._last_scores: Dict[str, dict] = {}

        self._symbols: List[str] = []
        self._running = False
        self._cycles = 0

    # ── 数据接收 ──────────────────────────

    async def on_kline(self, symbol: str, interval: str, kline: dict):
        """接收 WS kline 推送"""
        symbol = symbol.lower()
        cache = self._kline_cache[symbol][interval]
        cache.append(kline)

        # 1m 缓存只保留 1440 根 (24h)
        if interval == "1m" and len(cache) > 1440:
            cache.pop(0)
        # 大级别保留 500 根
        elif interval != "1m" and len(cache) > 500:
            cache.pop(0)

        # 如果 kline 已关闭，写入 DB
        if kline.get("is_closed"):
            self._save_kline(symbol, interval, kline)

    async def on_trade(self, symbol: str, trade: dict):
        """接收 aggTrade → 检测大单"""
        symbol = symbol.lower()
        qty_usd = trade["price"] * trade["quantity"]
        if qty_usd >= 50000:  # > 5万U
            buf = self._whale_buffer[symbol]
            buf.append(trade)
            # 只保留最近 5 分钟
            cutoff = int(time.time() * 1000) - 300_000
            self._whale_buffer[symbol] = [t for t in buf if t["timestamp"] > cutoff]

    async def on_force_order(self, symbol: str, order: dict):
        """接收强平订单"""
        symbol = symbol.lower()
        buf = self._liquidation_buffer[symbol]
        buf.append(order)
        # 保留最近 15 分钟
        cutoff = int(time.time() * 1000) - 900_000
        self._liquidation_buffer[symbol] = [o for o in buf if o["timestamp"] > cutoff]

    # ── 数据存储 ──────────────────────────

    def _save_kline(self, symbol: str, interval: str, kline: dict):
        """保存 K 线到 DB"""
        try:
            if interval == "1m":
                self.db.execute(
                    "INSERT OR REPLACE INTO kline_1m (symbol, open_time, open, high, low, close, volume, quote_volume, trades_count) VALUES (?,?,?,?,?,?,?,?,?)",
                    (symbol, kline["open_time"], kline["open"], kline["high"],
                     kline["low"], kline["close"], kline["volume"], kline["quote_volume"], kline.get("trades", 0))
                )
            else:
                self.db.execute(
                    "INSERT OR REPLACE INTO kline_large (symbol, interval, open_time, open, high, low, close, volume, quote_volume) VALUES (?,?,?,?,?,?,?,?,?)",
                    (symbol, interval, kline["open_time"], kline["open"], kline["high"],
                     kline["low"], kline["close"], kline["volume"], kline["quote_volume"])
                )
        except Exception as e:
            log.error("Failed to save kline %s %s: %s", symbol, interval, e)

    def _save_snapshot(self, symbol: str, result: dict):
        """保存评分快照到 DB"""
        try:
            self.db.execute(
                "INSERT INTO score_snapshots (symbol, timestamp, large_score, small_score, combined_score, large_patterns, oi_change_pct, cvd_direction, funding_rate, price) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, int(time.time()), result.get("total_score", 0), 0, result.get("total_score", 0),
                 json.dumps([m["pattern_name"] for m in result.get("matches", [])]),
                 result.get("oi_change_pct", 0), result.get("cvd_direction", "flat"),
                 result.get("funding_rate", 0), result.get("price", 0))
            )
            self.db.commit()
        except Exception as e:
            log.error("Failed to save snapshot: %s", e)

    # ── 评分周期 ──────────────────────────

    async def _score_symbol(self, symbol: str) -> dict:
        """对单个币种进行评分"""
        try:
            mtf = MTFIndicators(self.indicators, symbol)
            data = mtf.refresh()

            # 注入大单和清算数据
            data["whale_trades"] = self._whale_buffer.get(symbol, [])
            data["liquidations"] = self._liquidation_buffer.get(symbol, [])
            data["oi_change_pct"] = self._get_oi_change(symbol)
            data["funding_rate"] = self._get_funding_rate(symbol)

            result = self.scorer.score(data)
            result["symbol"] = symbol
            result["price"] = data.get("price", 0)
            result["oi_change_pct"] = data.get("oi_change_pct", 0)
            result["cvd_direction"] = "up" if data.get("cvd", 0) > 0 else "down" if data.get("cvd", 0) < 0 else "flat"
            result["funding_rate"] = data.get("funding_rate", 0)
            result["support"] = data.get("support", [])
            result["resistance"] = data.get("resistance", [])
            result["timestamp"] = int(time.time())

            # 保存评分
            self._last_scores[symbol] = result
            self._save_snapshot(symbol, result)

            return result
        except Exception as e:
            log.error("Score error for %s: %s", symbol, e)
            return {"symbol": symbol, "total_score": 0, "direction": "neutral", "error": str(e)}

    def _get_oi_change(self, symbol: str) -> float:
        """从 oi_history 表获取 24h OI 变化"""
        try:
            cutoff = int(time.time()) - 24 * 3600
            rows = self.db.execute(
                "SELECT open_interest FROM oi_history WHERE symbol=? AND timestamp >= ? ORDER BY timestamp",
                (symbol, cutoff)
            ).fetchall()
            if len(rows) >= 2 and rows[0][0] > 0:
                return (rows[-1][0] - rows[0][0]) / rows[0][0] * 100
        except Exception:
            pass
        return 0

    def _get_funding_rate(self, symbol: str) -> float:
        """获取资金费率（从 funding_history 最近记录）"""
        try:
            row = self.db.execute(
                "SELECT funding_rate FROM funding_history WHERE symbol=? ORDER BY timestamp DESC LIMIT 1",
                (symbol,)
            ).fetchone()
            return row[0] if row else 0
        except Exception:
            return 0

    async def _scan_cycle(self):
        """单次评分周期"""
        self._cycles += 1
        start = time.time()
        log.info("=== Large Frame Cycle #%d ===", self._cycles)

        # 获取活跃符号列表
        symbols = self._get_active_symbols()
        if not symbols:
            log.warning("No active symbols")
            return

        # 分批异步评分
        batch_size = self.config.get("scan", {}).get("batch_size", 50)
        all_results = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = [self._score_symbol(sym) for sym in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in batch_results:
                if isinstance(res, dict):
                    all_results.append(res)
            await asyncio.sleep(0.1)  # yield

        elapsed = time.time() - start
        scored = sum(1 for r in all_results if r.get("total_score", 0) > 0)
        log.info("Cycle #%d complete: %d symbols, %d scored >0, %.1fs",
                 self._cycles, len(all_results), scored, elapsed)

        return all_results

    def _get_active_symbols(self) -> List[str]:
        """获取活跃符号列表"""
        # 优先从 router 获取
        active = self.router.active_symbols
        if active:
            return active[:self.symbols_limit]

        # 回退：从 DB 获取
        try:
            rows = self.db.execute(
                "SELECT DISTINCT symbol FROM kline_1m ORDER BY symbol LIMIT ?",
                (self.symbols_limit,)
            ).fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    # ── 公共接口 ──────────────────────────

    def get_score(self, symbol: str) -> Optional[dict]:
        """获取最近评分"""
        return self._last_scores.get(symbol.lower())

    def get_top_scores(self, n: int = 20, direction: str = None) -> List[dict]:
        """获取 Top N 评分"""
        scores = list(self._last_scores.values())
        if direction:
            scores = [s for s in scores if s.get("direction") == direction]
        return sorted(scores, key=lambda x: x.get("total_score", 0), reverse=True)[:n]

    # ── 生命周期 ──────────────────────────

    async def run(self):
        """启动大级别引擎主循环"""
        self._running = True
        log.info("Large Frame Engine started, interval=%ds", self.interval)

        # 等待 WS 数据积累
        await asyncio.sleep(30)

        while self._running:
            try:
                await self._scan_cycle()
                # 清理旧数据
                self._cleanup_old_klines()
            except Exception as e:
                log.error("Cycle error: %s", e)

            await asyncio.sleep(self.interval)

    async def stop(self):
        """停止引擎"""
        self._running = False
        log.info("Large Frame Engine stopped after %d cycles", self._cycles)

    def _cleanup_old_klines(self):
        """清理过期 1m K 线（保留 90 天）"""
        try:
            retention_days = self.config.get("storage", {}).get("kline_1m_retention_days", 90)
            cutoff = int(time.time() * 1000) - retention_days * 86400 * 1000
            self.db.execute("DELETE FROM kline_1m WHERE open_time < ?", (cutoff,))
            self.db.commit()
        except Exception as e:
            log.error("Cleanup error: %s", e)

    @property
    def stats(self) -> dict:
        return {"cycles": self._cycles, "symbols_tracked": len(self._last_scores)}
