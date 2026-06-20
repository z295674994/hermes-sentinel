"""
秒级模式检测器 — 6 种实时微结构模式
使用 DeepSeek V4 Pro 编写
"""
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


class SecondPatternDetector:
    """秒级模式检测，依赖环形缓冲区的实时数据"""

    def __init__(self, config: dict):
        self.weights = config.get("second_patterns", {})
        self.thresholds = config.get("second_thresholds", {})
        # 每个 symbol 缓存: 滚动价格/量窗口
        self._price_history: Dict[str, list] = defaultdict(list)  # 最近 300 秒价格
        self._volume_history: Dict[str, list] = defaultdict(list)  # 最近 300 秒量
        self._buffer_seconds = 300  # 5分钟基线窗口

    def feed_ticker(self, symbol: str, ticker: dict):
        """喂入 bookTicker 数据"""
        price = (ticker["bid"] + ticker["ask"]) / 2
        ts = ticker.get("timestamp", int(time.time() * 1000))
        s = symbol.lower()
        self._price_history[s].append({"price": price, "ts": ts})
        if len(self._price_history[s]) > 500:
            self._price_history[s].pop(0)

    def feed_trade(self, symbol: str, trade: dict):
        """喂入 aggTrade 数据"""
        s = symbol.lower()
        self._volume_history[s].append({
            "price": trade["price"],
            "quantity": trade["quantity"],
            "is_buyer_maker": trade.get("is_buyer_maker", False),
            "ts": trade.get("timestamp", int(time.time() * 1000)),
        })
        if len(self._volume_history[s]) > 1000:
            self._volume_history[s].pop(0)

    def detect_all(self, symbol: str, router) -> List[dict]:
        """检测全部 6 种秒级模式"""
        symbol = symbol.lower()
        results = []
        for sid in range(1, 7):
            detector = getattr(self, f"pattern_s{sid}", None)
            if detector:
                result = detector(symbol, router)
                if result and result.get("confidence", 0) > 0:
                    results.append(result)
        return sorted(results, key=lambda x: x["confidence"], reverse=True)

    def score_all(self, symbol: str, router) -> dict:
        """秒级评分"""
        matches = self.detect_all(symbol, router)
        total_score = 0
        direction = "neutral"
        long_score = 0
        short_score = 0

        for m in matches:
            sid = m["pattern_id"]
            weight = self.weights.get(f"pattern_s{sid}", 15)
            score = m["confidence"] * weight / 100
            total_score += score
            if m.get("direction") == "long":
                long_score += score
            elif m.get("direction") == "short":
                short_score += score

        total_score = min(100, total_score)
        if long_score > short_score * 1.3:
            direction = "long"
        elif short_score > long_score * 1.3:
            direction = "short"

        return {
            "total_score": round(total_score, 1),
            "direction": direction,
            "matches": matches,
            "match_count": len(matches),
        }

    # ── 辅助方法 ──────────────────────────

    def _get_recent_prices(self, symbol: str, seconds: int = 5) -> list:
        """获取最近 N 秒价格"""
        prices = self._price_history.get(symbol, [])
        cutoff = int(time.time() * 1000) - seconds * 1000
        return [p["price"] for p in prices if p["ts"] > cutoff]

    def _get_base_volume(self, symbol: str, seconds: int = 300) -> float:
        """获取基线窗口总成交量 (USDT)"""
        trades = self._volume_history.get(symbol, [])
        cutoff = int(time.time() * 1000) - seconds * 1000
        return sum(t["price"] * t["quantity"] for t in trades if t["ts"] > cutoff)

    def _get_recent_volume(self, symbol: str, seconds: int = 5) -> float:
        """获取最近 N 秒成交量"""
        trades = self._volume_history.get(symbol, [])
        cutoff = int(time.time() * 1000) - seconds * 1000
        return sum(t["price"] * t["quantity"] for t in trades if t["ts"] > cutoff)

    def _get_recent_trades(self, symbol: str, seconds: int = 5) -> list:
        """获取最近 N 秒成交"""
        trades = self._volume_history.get(symbol, [])
        cutoff = int(time.time() * 1000) - seconds * 1000
        return [t for t in trades if t["ts"] > cutoff]

    # ═══════════════════════════════════════
    # 6 种秒级模式
    # ═══════════════════════════════════════

    # S1: 放量扫货
    def pattern_s1(self, symbol: str, router) -> Optional[dict]:
        """5秒内成交量 > 前5分钟均量×3，且价格上行"""
        threshold = self.thresholds.get("volume_sweep_ratio", 3.0)
        recent_vol = self._get_recent_volume(symbol, 5)
        base_vol = self._get_base_volume(symbol, 300) / 60  # 5秒均量 = 5分钟总量/60
        prices = self._get_recent_prices(symbol, 5)

        if len(prices) < 3:
            return None

        vol_ratio = recent_vol / base_vol if base_vol > 0 else 0
        price_chg = (prices[-1] - prices[0]) / prices[0] * 100 if prices[0] > 0 else 0

        if vol_ratio > threshold and price_chg > 0.1:
            score = min(100, vol_ratio * 15 + price_chg * 15)
            return {
                "pattern_id": 1, "pattern_name": "放量扫货",
                "direction": "long", "confidence": round(score, 1),
                "reasons": [f"量{vol_ratio:.1f}x", f"涨{price_chg:.2f}%"],
                "score": round(score, 1), "vol_ratio": round(vol_ratio, 1),
            }
        return None

    # S2: 暴力砸盘
    def pattern_s2(self, symbol: str, router) -> Optional[dict]:
        """5秒内跌幅 > 2%，成交量激增"""
        threshold = self.thresholds.get("crash_dump_pct", 2.0)
        prices = self._get_recent_prices(symbol, 5)
        recent_vol = self._get_recent_volume(symbol, 5)
        base_vol = self._get_base_volume(symbol, 300) / 60

        if len(prices) < 3:
            return None

        price_chg = (prices[-1] - prices[0]) / prices[0] * 100 if prices[0] > 0 else 0
        vol_ratio = recent_vol / base_vol if base_vol > 0 else 0

        if price_chg < -threshold and vol_ratio > 1.5:
            score = min(100, abs(price_chg) * 15 + vol_ratio * 10)
            return {
                "pattern_id": 2, "pattern_name": "暴力砸盘",
                "direction": "short", "confidence": round(score, 1),
                "reasons": [f"跌{price_chg:.2f}%", f"量{vol_ratio:.1f}x"],
                "score": round(score, 1),
            }
        return None

    # S3: 大单出现
    def pattern_s3(self, symbol: str, router) -> Optional[dict]:
        """单笔成交 > 50,000 USDT"""
        threshold = self.thresholds.get("whale_trade_min", 50000)
        recent = self._get_recent_trades(symbol, 3)
        for t in recent:
            qty_usd = t["price"] * t["quantity"]
            if qty_usd >= threshold:
                direction = "short" if t["is_buyer_maker"] else "long"
                score = min(100, qty_usd / 2000)
                return {
                    "pattern_id": 3, "pattern_name": "大单出现",
                    "direction": direction, "confidence": round(score, 1),
                    "reasons": [f"${qty_usd/1000:.0f}K {'吃卖单' if direction == 'long' else '砸买单'}"],
                    "score": round(score, 1), "size_usd": round(qty_usd),
                }
        return None

    # S4: 簿口失衡
    def pattern_s4(self, symbol: str, router) -> Optional[dict]:
        """bid量/ask量 > 3 或 < 0.33"""
        threshold = self.thresholds.get("book_imbalance_ratio", 3.0)
        ticker = router.get_ticker(symbol)
        if not ticker or ticker.get("ask_qty", 0) <= 0:
            return None
        ratio = ticker["bid_qty"] / ticker["ask_qty"]
        if ratio > threshold:
            score = min(100, ratio * 15)
            return {
                "pattern_id": 4, "pattern_name": "簿口失衡(买盘强)",
                "direction": "long", "confidence": round(score, 1),
                "reasons": [f"bid/ask={ratio:.1f}"],
                "score": round(score, 1), "ratio": round(ratio, 1),
            }
        elif ratio < 1 / threshold:
            score = min(100, (1 / ratio) * 15)
            return {
                "pattern_id": 4, "pattern_name": "簿口失衡(卖盘强)",
                "direction": "short", "confidence": round(score, 1),
                "reasons": [f"bid/ask={ratio:.2f}"],
                "score": round(score, 1), "ratio": round(ratio, 2),
            }
        return None

    # S5: 突破阻力
    def pattern_s5(self, symbol: str, router) -> Optional[dict]:
        """连续突破前高"""
        prices = self._get_recent_prices(symbol, 30)
        if len(prices) < 10:
            return None
        # 简单判断：最近3个价 > 前20个价的最大值
        recent = prices[-3:]
        earlier = prices[:-3]
        if not earlier:
            return None
        max_before = max(earlier)
        if all(p > max_before for p in recent):
            break_pct = (recent[-1] - max_before) / max_before * 100
            score = min(100, break_pct * 20 + 40)
            return {
                "pattern_id": 5, "pattern_name": "突破阻力",
                "direction": "long", "confidence": round(score, 1),
                "reasons": [f"破{max_before:.4f}", f"涨幅{break_pct:.2f}%"],
                "score": round(score, 1),
            }
        return None

    # S6: 支撑测试
    def pattern_s6(self, symbol: str, router) -> Optional[dict]:
        """砸穿支撑后迅速恢复"""
        prices = self._get_recent_prices(symbol, 30)
        if len(prices) < 15:
            return None
        # 找最近低点，然后看是否恢复
        min_idx = prices.index(min(prices[-10:]))
        min_price = prices[min_idx]
        current = prices[-1]
        recovery = (current - min_price) / min_price * 100
        if recovery > 0.5 and min_idx > len(prices) - 8:  # 近期低点已恢复
            score = min(100, recovery * 30 + 30)
            return {
                "pattern_id": 6, "pattern_name": "支撑测试",
                "direction": "long", "confidence": round(score, 1),
                "reasons": [f"低{min_price:.4f}", f"恢复+{recovery:.2f}%"],
                "score": round(score, 1),
            }
        return None
