"""
秒级引擎 — 实时事件驱动 + 环形缓冲区
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional

from sentinel.utils import safe_call

from sentinel.patterns.seconds import SecondPatternDetector

log = logging.getLogger(__name__)


class SecondLevelEngine:
    """秒级引擎：实时微结构监控"""

    def __init__(self, config: dict, router, db):
        self.config = config
        self.router = router
        self.db = db
        self.detector = SecondPatternDetector(config)

        # 冷却管理: {symbol: {"p0_last": ts, "p1_last": ts}}
        self._cooldowns: Dict[str, dict] = defaultdict(dict)

        # 回调
        self._on_alert: list = []

        self._running = False
        self._events_processed = 0

    # ── 数据接收 ──────────────────────────

    async def on_ticker(self, symbol: str, ticker: dict):
        """bookTicker 推送"""
        self.detector.feed_ticker(symbol, ticker)
        # 触发簿口失衡检测 (S4)
        await self._check(symbol)

    async def on_trade(self, symbol: str, trade: dict):
        """aggTrade 推送"""
        self.detector.feed_trade(symbol, trade)
        # 触发交易相关检测 (S1/S2/S3/S5/S6)
        await self._check(symbol)

    # ── 检测 ──────────────────────────────

    async def _check(self, symbol: str):
        """检查秒级模式"""
        self._events_processed += 1
        symbol = symbol.lower()

        # 冷却检查
        now = time.time()
        cooldowns = self._cooldowns[symbol]
        if cooldowns.get("p0", 0) > now:
            return  # 还在 P0 冷却中

        result = self.detector.score_all(symbol, self.router)
        if result["match_count"] == 0:
            return

        # 确定告警级别
        score = result["total_score"]
        if score >= 80:
            level = "P0"
            cooldown = self.config.get("feishu", {}).get("rate_limits", {}).get("p0_per_symbol", 300)
        elif score >= 65:
            level = "P1"
            cooldown = self.config.get("feishu", {}).get("rate_limits", {}).get("p1_per_symbol", 900)
        else:
            return  # 不触发

        # 更新冷却
        cooldowns[level.lower()] = now + cooldown

        alert = {
            "symbol": symbol,
            "timestamp": int(now),
            "engine": "second_level",
            "level": level,
            "small_score": score,
            "direction": result["direction"],
            "matches": result["matches"],
        }
        log.info("Second-level alert: %s %s score=%.1f patterns=%s",
                 symbol, level, score, [m["pattern_name"] for m in result["matches"]])

        for cb in self._on_alert:
            safe_call(cb, alert)

    # ── 回调注册 ──────────────────────────

    def on_alert(self, callback):
        """注册告警回调: callback(alert_dict)"""
        self._on_alert.append(callback)

    @property
    def stats(self) -> dict:
        return {"events_processed": self._events_processed}
