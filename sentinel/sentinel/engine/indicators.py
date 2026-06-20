"""
MTF 指标引擎 — 多时间框架技术指标
使用 DeepSeek V4 Pro 编写（从原 scanner 迁移+精简）
"""
import logging
from collections import defaultdict
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


class IndicatorsEngine:
    """多时间框架指标计算"""

    def __init__(self, db, router):
        self.db = db
        self.router = router

    # ── K 线查询 ──────────────────────────

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> list:
        """从 DB 获取 K 线"""
        table = "kline_1m" if interval == "1m" else "kline_large"
        if interval == "1m":
            rows = self.db.execute(
                "SELECT open_time, open, high, low, close, volume, quote_volume FROM kline_1m WHERE symbol=? ORDER BY open_time DESC LIMIT ?",
                (symbol.lower(), limit)
            ).fetchall()
        else:
            rows = self.db.execute(
                "SELECT open_time, open, high, low, close, volume, quote_volume FROM kline_large WHERE symbol=? AND interval=? ORDER BY open_time DESC LIMIT ?",
                (symbol.lower(), interval, limit)
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_price_series(self, symbol: str, interval: str, limit: int = 100) -> list:
        """获取收盘价序列"""
        klines = self.get_klines(symbol, interval, limit)
        return [k["close"] for k in klines]

    # ── 基础指标 ──────────────────────────

    def sma(self, series: list, period: int) -> Optional[float]:
        if len(series) < period:
            return None
        return sum(series[-period:]) / period

    def ema(self, series: list, period: int) -> Optional[float]:
        if len(series) < period:
            return None
        multiplier = 2 / (period + 1)
        ema_val = series[0]
        for price in series[1:]:
            ema_val = (price - ema_val) * multiplier + ema_val
        return ema_val

    def rsi(self, series: list, period: int = 14) -> Optional[float]:
        if len(series) < period + 1:
            return None
        gains = []
        losses = []
        for i in range(1, len(series)):
            diff = series[i] - series[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def atr(self, symbol: str, interval: str, period: int = 14) -> Optional[float]:
        klines = self.get_klines(symbol, interval, period + 1)
        if len(klines) < period:
            return None
        tr_values = []
        for i in range(1, len(klines)):
            high = klines[i]["high"]
            low = klines[i]["low"]
            prev_close = klines[i-1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)
        return sum(tr_values) / len(tr_values)

    def volatility(self, series: list) -> Optional[float]:
        """标准差波动率"""
        if len(series) < 2:
            return None
        mean = sum(series) / len(series)
        variance = sum((x - mean) ** 2 for x in series) / len(series)
        return variance ** 0.5 / mean if mean else 0

    # ── 价格变化 ──────────────────────────

    def price_change(self, symbol: str, interval: str, periods: int = 1) -> Optional[float]:
        """价格变化百分比"""
        klines = self.get_klines(symbol, interval, periods + 1)
        if len(klines) < periods + 1:
            return None
        return (klines[-1]["close"] - klines[0]["close"]) / klines[0]["close"] * 100

    # ── 成交量 ────────────────────────────

    def volume_growth(self, symbol: str, interval: str, short: int = 5, long: int = 20) -> Optional[float]:
        """成交量增长率"""
        klines = self.get_klines(symbol, interval, long + 1)
        if len(klines) < long:
            return None
        short_vol = sum(k["volume"] for k in klines[-short:]) / short
        long_vol = sum(k["volume"] for k in klines[-long:]) / long
        return short_vol / long_vol if long_vol > 0 else 1.0

    # ── 多时间框架 ────────────────────────

    def mtf_alignment(self, symbol: str, timeframes: list = None) -> float:
        """多时间框架方向一致性 0-100"""
        if timeframes is None:
            timeframes = ["1h", "4h", "1d"]
        aligned = 0
        total = 0
        for tf in timeframes:
            chg = self.price_change(symbol, tf, 1)
            if chg is not None:
                total += 1
                # 涨跌方向
                if chg > 0.5:
                    aligned += 1
                elif chg < -0.5:
                    aligned -= 1
        if total == 0:
            return 0
        return abs(aligned / total) * 100

    def trend_strength(self, symbol: str, interval: str) -> float:
        """趋势强度: ema12 vs ema26 的偏离程度"""
        prices = self.get_price_series(symbol, interval, 50)
        ema12 = self.ema(prices, 12)
        ema26 = self.ema(prices, 26)
        if ema12 and ema26 and ema26 > 0:
            return abs(ema12 - ema26) / ema26 * 100
        return 0

    # ── 支撑/阻力 ─────────────────────────

    def support_resistance(self, symbol: str, interval: str, lookback: int = 50) -> dict:
        """找最近的支撑和阻力位"""
        klines = self.get_klines(symbol, interval, lookback)
        if len(klines) < 20:
            return {"support": [], "resistance": []}
        closes = [k["close"] for k in klines]
        highs = [k["high"] for k in klines]
        lows = [k["low"] for k in klines]
        current = closes[-1]

        # 找局部极值
        supports = []
        resistances = []
        window = 5
        for i in range(window, len(lows) - window):
            if lows[i] == min(lows[i-window:i+window+1]) and lows[i] < current:
                supports.append(lows[i])
            if highs[i] == max(highs[i-window:i+window+1]) and highs[i] > current:
                resistances.append(highs[i])

        # 去重取最近
        supports = sorted(set(round(s, 4) for s in supports), reverse=True)[:3]
        resistances = sorted(set(round(r, 4) for r in resistances))[:3]

        return {"support": supports, "resistance": resistances}


class MTFIndicators:
    """多时间框架指标缓存"""

    def __init__(self, engine: IndicatorsEngine, symbol: str):
        self.engine = engine
        self.symbol = symbol
        self._cache: Dict[str, dict] = defaultdict(dict)

    def refresh(self) -> dict:
        """刷新该币种的全部指标"""
        data = {
            "symbol": self.symbol,
            "timestamp": 0,  # to be set by caller
            "price": self.engine.router.get_price(self.symbol) or 0,
        }
        # 价格变化
        for tf in ["1m", "1h", "4h", "1d"]:
            data[f"price_chg_{tf}"] = self.engine.price_change(self.symbol, tf, 1) or 0

        # 成交量
        data["volume_growth_1h"] = self.engine.volume_growth(self.symbol, "1h") or 1.0

        # 多时间框架
        data["mtf_alignment"] = self.engine.mtf_alignment(self.symbol)
        data["trend_strength"] = self.engine.trend_strength(self.symbol, "4h")
        data["rsi_1h"] = self.engine.rsi(self.engine.get_price_series(self.symbol, "1h", 30), 14) or 50
        data["rsi_4h"] = self.engine.rsi(self.engine.get_price_series(self.symbol, "4h", 30), 14) or 50

        # ATR
        atr_1h = self.engine.atr(self.symbol, "1h", 14)
        data["atr_1h"] = atr_1h or 0

        # 支撑阻力
        sr = self.engine.support_resistance(self.symbol, "4h")
        data["support"] = sr["support"]
        data["resistance"] = sr["resistance"]

        # CVD 从 router 的 trade buffer 计算
        trades = self.engine.router.get_trade_window(self.symbol, 900)  # 15分钟
        cvd = 0
        volume = 0
        for t in trades:
            qty_usd = t["price"] * t["quantity"]
            volume += qty_usd
            if t.get("is_buyer_maker"):
                cvd -= qty_usd
            else:
                cvd += qty_usd
        data["cvd"] = cvd
        data["cvd_volume"] = volume

        self._cache["latest"] = data
        return data

    @property
    def latest(self) -> dict:
        return self._cache.get("latest", {})
