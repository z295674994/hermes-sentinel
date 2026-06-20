"""
Hermes Sentinel - 主入口
统一 WS 数据采集 -> 双引擎 -> 共振 -> AI 分析 -> 飞书推送
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import logging
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))

from sentinel.ws_client import BinanceWSClient
from sentinel.stream_router import StreamRouter
from sentinel.bootstrap import BootstrapLoader
from sentinel.engine.large_frame import LargeFrameEngine
from sentinel.engine.second_level import SecondLevelEngine
from sentinel.engine.resonance import ResonanceEngine
from sentinel.ai.deepseek import DeepSeekAnalyzer
from sentinel.data_collector import DataCollector
from sentinel.account_stream import AccountStream
from sentinel.push.feishu import FeishuPusher

log = logging.getLogger("sentinel")


def load_config() -> dict:
    config_path = Path(__file__).parent / "sentinel" / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    def _resolve_env(value):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            return os.environ.get(value[2:-1], "")
        return value

    def _walk(d):
        if isinstance(d, dict):
            return {k: _walk(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [_walk(v) for v in d]
        return _resolve_env(d)

    return _walk(config)


def setup_logging(config: dict):
    log_config = config.get("logging", {})
    level = getattr(logging, log_config.get("level", "INFO"))
    log_dir = Path(log_config.get("dir", "sentinel/logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "sentinel.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


class Sentinel:
    """Hermes Sentinel 主控制器"""

    def __init__(self, config: dict):
        self.config = config

        # DB
        db_path = Path(__file__).parent / config.get("storage", {}).get("db_path", "sentinel/sentinel.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row

        # 核心组件
        self.ws = BinanceWSClient(config)
        self.router = StreamRouter(config)
        self.bootstrap = BootstrapLoader(config)

        # 引擎
        self.large_engine = LargeFrameEngine(config, self.router, self.db)
        self.second_engine = SecondLevelEngine(config, self.router, self.db)
        self.resonance = ResonanceEngine(config, self.large_engine, self.second_engine, self.db)

        # AI + 推送
        self.analyzer = DeepSeekAnalyzer(config)
        self.pusher = FeishuPusher(config)

        # 数据采集（REST轮询弥补WS无法全市场订阅）
        self.collector = DataCollector(config, self.db, self.router)
        self.account_stream = AccountStream(config, self.db)

        # 统计
        self._stats = {
            "start_time": time.time(),
            "alerts_p0": 0, "alerts_p1": 0, "alerts_p2": 0,
        }

    # ── 回调连接 ──────────────────────────

    def _wire_callbacks(self):
        """串联各组件回调"""

        # Router -> Engines
        self.router.on_ticker(self.second_engine.on_ticker)
        self.router.on_trade(self.second_engine.on_trade)
        self.router.on_trade(self.large_engine.on_trade)
        self.router.on_kline(self.large_engine.on_kline)
        self.router.on_force_order(self.large_engine.on_force_order)

        # Second engine alerts -> Resonance
        self.second_engine.on_alert(self.resonance.on_small_alert)

        # Resonance alerts -> AI + Push
        self.resonance.on_alert(self._handle_alert)

    async def _handle_alert(self, alert: dict):
        """处理共振告警：AI 分析 + 推送 + 入库"""
        level = alert.get("level", "P1")
        self._stats[f"alerts_{level.lower()}"] += 1

        # P0/P1 送 AI 分析
        if level in ("P0", "P1") and self.analyzer.api_key:
            try:
                ai_result = await self.analyzer.analyze(alert)
                alert = self.analyzer.merge_alert(alert, ai_result)
            except Exception as e:
                log.error("AI analysis failed for %s: %s", alert.get("symbol"), e)

        # 入库
        self._save_alert(alert)

        # 推送
        await self.pusher.push_alert(alert)

    def _save_alert(self, alert: dict):
        """保存告警到 DB"""
        try:
            self.db.execute(
                """INSERT INTO alerts (symbol, timestamp, alert_level, zone, direction,
                   large_score, small_score, combined_score, large_patterns, small_patterns,
                   ai_direction, ai_confidence, ai_entry_price, ai_entry_backup_1, ai_entry_backup_2,
                   ai_tp_1, ai_tp_1_pct, ai_tp_2, ai_tp_2_pct, ai_tp_3, ai_tp_3_pct,
                   ai_sl_1, ai_sl_1_pct, ai_sl_2, ai_sl_2_pct, ai_sl_3, ai_sl_3_pct,
                   ai_summary, ai_full_analysis,
                   entry_price, market_snapshot)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    alert["symbol"], alert["timestamp"], alert.get("level"),
                    alert.get("zone", "realtime"), alert.get("direction"),
                    alert.get("large_score"), alert.get("small_score"), alert.get("combined_score"),
                    ",".join(alert.get("large_patterns", [])),
                    ",".join(alert.get("small_patterns", [])),
                    alert.get("ai_direction"), alert.get("ai_confidence"),
                    alert.get("ai_entry_price"), alert.get("ai_entry_backup_1"), alert.get("ai_entry_backup_2"),
                    alert.get("ai_tp_1"), alert.get("ai_tp_1_pct"),
                    alert.get("ai_tp_2"), alert.get("ai_tp_2_pct"),
                    alert.get("ai_tp_3"), alert.get("ai_tp_3_pct"),
                    alert.get("ai_sl_1"), alert.get("ai_sl_1_pct"),
                    alert.get("ai_sl_2"), alert.get("ai_sl_2_pct"),
                    alert.get("ai_sl_3"), alert.get("ai_sl_3_pct"),
                    alert.get("ai_summary"), alert.get("ai_analysis"),
                    alert.get("large_data", {}).get("price"),
                    str(alert.get("large_data", {}))[:2000],
                )
            )
            self.db.commit()
        except Exception as e:
            log.error("Failed to save alert: %s", e)

    # ── 订阅 ──────────────────────────────

    def _subscribe_streams(self):
        """订阅 WS streams"""
        # 全市场流（不区分币种）
        self.ws.subscribe("!bookTicker", lambda d: asyncio.create_task(self.router.handle_book_ticker(d)))
        self.ws.subscribe("!forceOrder", lambda d: asyncio.create_task(self.router.handle_force_order(d)))

        # 后续 bootstrap 完成后动态订阅各币种 kline/aggTrade

    # ── 心跳 ──────────────────────────────

    async def _heartbeat_loop(self):
        """定时心跳"""
        interval = self.config.get("feishu", {}).get("heartbeat_interval", 300)
        while True:
            await asyncio.sleep(interval)
            try:
                stats = {
                    "ws_connected": self.ws.stats.get("connected", False),
                    "uptime_min": int((time.time() - self._stats["start_time"]) / 60),
                    "large_cycles": self.large_engine.stats.get("cycles", 0),
                    "small_events": self.second_engine.stats.get("events_processed", 0),
                    "alerts_p0": self._stats["alerts_p0"],
                    "alerts_p1": self._stats["alerts_p1"],
                    "alerts_p2": self._stats["alerts_p2"],
                    "ambush_count": self.resonance.stats.get("ambush_count", 0),
                }
                await self.pusher.push_heartbeat(stats)
                log.info("Heartbeat sent: %s", stats)
            except Exception as e:
                log.error("Heartbeat error: %s", e)

    # ── 运行 ──────────────────────────────

    async def run(self):
        log.info("=== Hermes Sentinel Starting ===")

        # 启动推送队列
        await self.pusher.start()

        # 启动阶段：拉取历史 K 线
        symbols = []
        try:
            result = await self.bootstrap.load_all(self.db)
            count = result.get("count", 0) if isinstance(result, dict) else 0
            symbols = result.get("symbols", []) if isinstance(result, dict) else []
            log.info("Bootstrap: %d symbols, %d klines, %.1f min",
                     count, result.get("klines", 0), result.get("elapsed_min", 0))
        except Exception as e:
            log.error("Bootstrap failed (continuing): %s", e)

        # 启动 REST 数据采集器（K线/OI/资金费率/多空比轮询）
        if symbols:
            await self.collector.start(symbols)
            await self.account_stream.start()
            log.info("DataCollector started for %d symbols", len(symbols))

        # 注册 WS 订阅
        self._subscribe_streams()

        # 串联回调
        self._wire_callbacks()

        # 启动各引擎
        asyncio.create_task(self.large_engine.run())
        asyncio.create_task(self.resonance.run())
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._ambush_cycle())

        # 启动 WS 客户端 (阻塞)
        log.info("Starting WS client...")
        await self.ws.start()

    async def _ambush_cycle(self):
        """定期更新埋伏区"""
        while True:
            await asyncio.sleep(self.large_engine.interval)
            try:
                symbols = self.large_engine._get_active_symbols()
                self.resonance.update_ambush_zone(symbols)
            except Exception as e:
                log.error("Ambush cycle error: %s", e)

    async def shutdown(self):
        log.info("Shutting down...")
        await self.collector.stop()
        await self.account_stream.stop()
        await self.analyzer.close()
        await self.pusher.stop()
        self.db.close()


async def main():
    load_dotenv()
    config = load_config()
    setup_logging(config)
    sentinel = Sentinel(config)

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(sentinel.ws.stop()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(sentinel.ws.stop()))
    except NotImplementedError:
        pass

    try:
        await sentinel.run()
    except KeyboardInterrupt:
        log.info("Interrupted")
    finally:
        await sentinel.shutdown()


if __name__ == "__main__":
    asyncio.run(main())