"""
共振引擎 — 大小级别结合 + 分区管理
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional

from sentinel.utils import safe_call

log = logging.getLogger(__name__)


class ResonanceEngine:
    """共振引擎：大级别评分 + 小级别信号 → 综合决策"""

    def __init__(self, config: dict, large_engine, second_engine, db):
        self.config = config
        self.large = large_engine
        self.second = second_engine
        self.db = db

        zone_config = config.get("zones", {})
        self.ambush_threshold = zone_config.get("ambush", {}).get("score_threshold", 65)
        self.ambush_periods = zone_config.get("ambush", {}).get("consecutive_periods", 3)
        self.large_weight = zone_config.get("realtime", {}).get("large_weight", 0.6)
        self.small_weight = zone_config.get("realtime", {}).get("small_weight", 0.4)
        self.thresholds = zone_config.get("realtime", {}).get("thresholds", {"p0": 85, "p1": 70, "p2": 60})

        # 埋伏区状态追踪
        self._ambush_state: Dict[str, dict] = {}  # {symbol: {state, consecutive, entered_at, ...}}
        
        # 实时区告警 (P2攒批)
        self._p2_batch: List[dict] = []
        self._p2_window = zone_config.get("realtime", {}).get("batch_window", 30)
        self._last_p2_flush = time.time()

        # 回调
        self._on_alert: list = []

        self._running = False

    # ── 大小级别结合 ──────────────────────

    def evaluate(self, symbol: str, small_alert: Optional[dict] = None) -> Optional[dict]:
        """综合评分：大级别(最近评分) + 小级别(可选告警)"""
        symbol = symbol.lower()
        large = self.large.get_score(symbol)
        
        large_score = large.get("total_score", 0) if large else 0
        large_direction = large.get("direction", "neutral") if large else "neutral"
        large_matches = large.get("matches", []) if large else []

        small_score = 0
        small_direction = "neutral"
        small_matches = []

        if small_alert:
            small_score = small_alert.get("small_score", 0)
            small_direction = small_alert.get("direction", "neutral")
            small_matches = small_alert.get("matches", [])

        # 综合评分
        combined = large_score * self.large_weight + small_score * self.small_weight
        combined = min(100, combined)

        # 方向一致性
        directions_align = (
            large_direction == small_direction and large_direction != "neutral"
        )

        # 告警级别
        if directions_align and combined >= self.thresholds["p0"]:
            level = "P0"
        elif combined >= self.thresholds["p1"]:
            level = "P1"
        elif combined >= self.thresholds["p2"]:
            level = "P2"
        else:
            return None

        return {
            "symbol": symbol,
            "timestamp": int(time.time()),
            "level": level,
            "zone": "realtime",
            "large_score": round(large_score, 1),
            "small_score": round(small_score, 1),
            "combined_score": round(combined, 1),
            "large_direction": large_direction,
            "small_direction": small_direction,
            "direction": large_direction if directions_align else small_direction,
            "directions_align": directions_align,
            "large_patterns": [m["pattern_name"] for m in large_matches],
            "small_patterns": [m["pattern_name"] for m in small_matches],
            "large_data": large,
            "small_data": small_alert,
        }

    # ── 埋伏区管理 ────────────────────────

    def update_ambush_zone(self, symbols: List[str]):
        """更新埋伏区状态（每轮大级别评分后调用）"""
        now = time.time()
        for symbol in symbols:
            symbol = symbol.lower()
            large = self.large.get_score(symbol)
            if not large:
                continue

            score = large.get("total_score", 0)
            direction = large.get("direction", "neutral")
            matches = large.get("matches", [])

            # 判断是否吸筹/多周期共振
            is_positive = (
                direction == "long" and
                any(m["pattern_id"] in [1, 4, 5, 7, 12] for m in matches)
            )

            if score >= self.ambush_threshold or is_positive:
                state = self._ambush_state.get(symbol, {})
                consecutive = state.get("consecutive", 0) + 1

                # 状态升级
                if consecutive >= self.ambush_periods * 4:  # 12个周期 = 3小时
                    ambush_state = "ready"
                elif consecutive >= self.ambush_periods * 2:  # 6个周期 = 1.5小时
                    ambush_state = "loading"
                else:
                    ambush_state = "building"

                self._ambush_state[symbol] = {
                    "symbol": symbol,
                    "state": ambush_state,
                    "consecutive": consecutive,
                    "entered_at": state.get("entered_at", int(now)),
                    "last_updated": int(now),
                    "large_score": score,
                    "oi_trend": large.get("oi_change_pct", 0),
                    "cvd_trend": large.get("cvd_direction", "flat"),
                }

                # 入库
                self._save_ambush(symbol, ambush_state, consecutive)
            else:
                # 退出埋伏区
                if symbol in self._ambush_state:
                    self._exit_ambush(symbol, "score dropped below threshold")
                    del self._ambush_state[symbol]

    def _save_ambush(self, symbol: str, state: str, consecutive: int):
        """保存埋伏区状态到 DB"""
        try:
            self.db.execute(
                "UPDATE ambush_zone SET exited_at=strftime('%s','now'), exit_reason='state updated' WHERE symbol=? AND exited_at IS NULL",
                (symbol,)
            )
            self.db.execute(
                "INSERT INTO ambush_zone (symbol, state, entered_at, last_updated, large_score, consecutive_signals) VALUES (?,?,strftime('%s','now'),strftime('%s','now'),?,?)",
                (symbol, state, self._ambush_state.get(symbol, {}).get("large_score", 0), consecutive)
            )
            self.db.commit()
        except Exception as e:
            log.error("Ambush save error: %s", e)

    def _exit_ambush(self, symbol: str, reason: str):
        """退出埋伏区"""
        try:
            self.db.execute(
                "UPDATE ambush_zone SET exited_at=strftime('%s','now'), exit_reason=? WHERE symbol=? AND exited_at IS NULL",
                (reason, symbol)
            )
            self.db.commit()
        except Exception as e:
            log.error("Ambush exit error: %s", e)

    def get_ambush_symbols(self) -> List[dict]:
        """获取埋伏区币种列表"""
        return sorted(
            self._ambush_state.values(),
            key=lambda x: {"ready": 3, "loading": 2, "building": 1}.get(x["state"], 0),
            reverse=True,
        )

    # ── 告警处理 ──────────────────────────

    def on_small_alert(self, alert: dict):
        """收到秒级告警，结合大级别评分"""
        result = self.evaluate(alert["symbol"], alert)
        if not result:
            return

        if result["level"] in ("P0", "P1"):
            # 即时推送
            for cb in self._on_alert:
                safe_call(cb, result)
        elif result["level"] == "P2":
            # 攒批
            self._p2_batch.append(result)

    async def _flush_p2_batch(self):
        """定期推送 P2 攒批"""
        while self._running:
            await asyncio.sleep(self._p2_window)
            if self._p2_batch:
                batch = self._p2_batch[:]
                self._p2_batch = []
                log.info("Flushing %d P2 alerts", len(batch))
                for cb in self._on_alert:
                    for alert in batch:
                        safe_call(cb, alert)

    # ── 回调 ──────────────────────────────

    def on_alert(self, callback):
        self._on_alert.append(callback)

    # ── 生命周期 ──────────────────────────

    async def run(self):
        self._running = True
        await self._flush_p2_batch()

    async def stop(self):
        self._running = False

    @property
    def stats(self) -> dict:
        return {
            "ambush_count": len(self._ambush_state),
            "p2_batch_size": len(self._p2_batch),
        }
