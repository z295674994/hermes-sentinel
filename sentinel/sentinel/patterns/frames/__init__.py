"""
大级别模式检测器 — 13 种资金行为模式
使用 DeepSeek V4 Pro 编写（从原 scanner 迁移+扩展）
"""
import logging
from typing import Dict, List, Optional

log = logging.getLogger(__name__)


class PatternDetector:
    """大级别模式检测器，输入 MTF 指标数据，输出匹配模式"""

    def __init__(self, config: dict):
        self.weights = config.get("large_patterns", {})

    def detect_all(self, data: dict) -> List[dict]:
        """检测全部 13 种模式，返回匹配列表"""
        results = []
        for pid in range(1, 14):
            detector = getattr(self, f"pattern_{pid}", None)
            if detector:
                result = detector(data)
                if result and result.get("confidence", 0) > 0:
                    results.append(result)
        return sorted(results, key=lambda x: x["confidence"], reverse=True)

    def score_all(self, data: dict) -> dict:
        """评分：检测模式 + 加权计算总分"""
        matches = self.detect_all(data)
        total_score = 0
        direction = "neutral"
        long_score = 0
        short_score = 0

        for m in matches:
            pid = m["pattern_id"]
            weight = self.weights.get(f"pattern_{pid}", 10)
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
            "long_score": round(long_score, 1),
            "short_score": round(short_score, 1),
            "matches": matches,
            "match_count": len(matches),
        }

    # ── 辅助方法 ──────────────────────────

    def _norm_cvd(self, d: dict) -> float:
        """归一化 CVD: CVD/成交量，映射到 0-100"""
        cvd = d.get("cvd", 0)
        volume = d.get("cvd_volume", 1)
        if volume <= 0:
            return 0
        ratio = abs(cvd) / volume
        return min(100, ratio * 500)

    def _cvd_direction(self, d: dict) -> str:
        cvd = d.get("cvd", 0)
        return "up" if cvd > 0 else "down" if cvd < 0 else "flat"

    # ═══════════════════════════════════════
    # 13 种模式
    # ═══════════════════════════════════════

    # P1: 低位吸筹
    def pattern_1(self, d: dict) -> Optional[dict]:
        """条件: OI↑ CVD↑ 价格横盘 Funding<=0 波动率压缩"""
        score = 0.0
        reasons = []

        oi_chg = d.get("oi_change_pct", 0)
        price_chg_4h = d.get("price_chg_4h", 0) or 0
        cvd_score = self._norm_cvd(d)
        cvd_dir = self._cvd_direction(d)
        mtf = d.get("mtf_alignment", 0)
        rsi = d.get("rsi_4h", 50)

        # OI 增长
        if oi_chg > 20: score += 25; reasons.append(f"OI+{oi_chg:.0f}%")
        elif oi_chg > 10: score += 18; reasons.append(f"OI+{oi_chg:.0f}%")
        elif oi_chg > 5: score += 10

        # CVD 正值
        if cvd_score > 0 and cvd_dir == "up":
            score += min(20, cvd_score * 0.2); reasons.append("CVD↑")

        # 价格横盘或微涨
        if -3 < price_chg_4h < 8:
            score += 15; reasons.append("横盘/微涨")
        elif -5 < price_chg_4h <= -3:
            score += 10; reasons.append("微跌")

        # 低 RSI（底部区域）
        if rsi < 40: score += 10; reasons.append(f"RSI低{rsi:.0f}")

        # 多周期共振
        if mtf > 50: score += 15; reasons.append("多周期共振")
        elif mtf > 25: score += 8

        conf = min(100, score)
        return {
            "pattern_id": 1, "pattern_name": "低位吸筹",
            "direction": "long", "confidence": round(conf, 1),
            "reasons": reasons, "score": round(score, 1),
        } if conf >= 30 else None

    # P2: 高位派发
    def pattern_2(self, d: dict) -> Optional[dict]:
        """条件: OI↑ CVD↓ 价格横盘"""
        score = 0.0; reasons = []
        oi_chg = d.get("oi_change_pct", 0)
        cvd_dir = self._cvd_direction(d)
        price_chg_4h = d.get("price_chg_4h", 0) or 0
        rsi = d.get("rsi_4h", 50)

        if oi_chg > 10: score += 20; reasons.append(f"OI+{oi_chg:.0f}%")
        if cvd_dir == "down": score += 25; reasons.append("CVD↓")
        if -5 < price_chg_4h < 5: score += 15; reasons.append("横盘")
        if rsi > 60: score += 10; reasons.append(f"RSI高{rsi:.0f}")

        conf = min(100, score)
        return {
            "pattern_id": 2, "pattern_name": "高位派发",
            "direction": "short", "confidence": round(conf, 1),
            "reasons": reasons, "score": round(score, 1),
        } if conf >= 30 else None

    # P3: OI 顶背离
    def pattern_3(self, d: dict) -> Optional[dict]:
        """价格新高 OI 下降"""
        price_chg_1d = d.get("price_chg_1d", 0) or 0
        oi_chg = d.get("oi_change_pct", 0)
        if price_chg_1d > 5 and oi_chg < -3:
            conf = min(100, abs(oi_chg) * 5 + price_chg_1d)
            return {
                "pattern_id": 3, "pattern_name": "OI顶背离",
                "direction": "short", "confidence": round(conf, 1),
                "reasons": [f"价+{price_chg_1d:.1f}%", f"OI{oi_chg:.1f}%"],
                "score": round(abs(oi_chg) + price_chg_1d / 2, 1),
            }
        return None

    # P4: OI 底背离
    def pattern_4(self, d: dict) -> Optional[dict]:
        """价格新低 OI 上升"""
        price_chg_1d = d.get("price_chg_1d", 0) or 0
        oi_chg = d.get("oi_change_pct", 0)
        if price_chg_1d < -5 and oi_chg > 5:
            conf = min(100, oi_chg * 3 + abs(price_chg_1d))
            return {
                "pattern_id": 4, "pattern_name": "OI底背离",
                "direction": "long", "confidence": round(conf, 1),
                "reasons": [f"价{price_chg_1d:.1f}%", f"OI+{oi_chg:.1f}%"],
                "score": round(oi_chg + abs(price_chg_1d) / 2, 1),
            }
        return None

    # P5: CVD 正向背离
    def pattern_5(self, d: dict) -> Optional[dict]:
        """价格微跌 CVD 大升"""
        price_chg_1h = d.get("price_chg_1h", 0) or 0
        cvd_score = self._norm_cvd(d)
        cvd_dir = self._cvd_direction(d)
        if price_chg_1h < -0.5 and cvd_dir == "up" and cvd_score > 30:
            conf = min(100, cvd_score * 0.8 + abs(price_chg_1h) * 3)
            return {
                "pattern_id": 5, "pattern_name": "CVD正背离",
                "direction": "long", "confidence": round(conf, 1),
                "reasons": [f"价{price_chg_1h:.1f}%", f"CVD+{cvd_score:.0f}"],
                "score": round(cvd_score * 0.6 + abs(price_chg_1h) * 2, 1),
            }
        return None

    # P6: CVD 负向背离
    def pattern_6(self, d: dict) -> Optional[dict]:
        """价格微涨 CVD 大降"""
        price_chg_1h = d.get("price_chg_1h", 0) or 0
        cvd_score = self._norm_cvd(d)
        cvd_dir = self._cvd_direction(d)
        if price_chg_1h > 0.5 and cvd_dir == "down" and cvd_score > 30:
            conf = min(100, cvd_score * 0.8 + price_chg_1h * 3)
            return {
                "pattern_id": 6, "pattern_name": "CVD负背离",
                "direction": "short", "confidence": round(conf, 1),
                "reasons": [f"价+{price_chg_1h:.1f}%", f"CVD-{cvd_score:.0f}"],
                "score": round(cvd_score * 0.6 + price_chg_1h * 2, 1),
            }
        return None

    # P7: 多周期共振多
    def pattern_7(self, d: dict) -> Optional[dict]:
        """1h↑ 4h↑ 1d↑"""
        mtf = d.get("mtf_alignment", 0)
        trend = d.get("trend_strength", 0)
        if mtf > 60:
            conf = min(100, mtf * 0.8 + trend * 0.5)
            return {
                "pattern_id": 7, "pattern_name": "多周期共振多",
                "direction": "long", "confidence": round(conf, 1),
                "reasons": [f"MTF{mtf:.0f}", f"趋势{trend:.1f}"],
                "score": round(mtf * 0.7 + trend * 0.3, 1),
            }
        return None

    # P8: 多周期共振空
    def pattern_8(self, d: dict) -> Optional[dict]:
        """1h↓ 4h↓ 1d↓"""
        price_chg_1h = d.get("price_chg_1h", 0) or 0
        price_chg_4h = d.get("price_chg_4h", 0) or 0
        price_chg_1d = d.get("price_chg_1d", 0) or 0
        if price_chg_1h < -1 and price_chg_4h < -2 and price_chg_1d < -3:
            conf = min(100, abs(price_chg_1h + price_chg_4h + price_chg_1d) * 2)
            return {
                "pattern_id": 8, "pattern_name": "多周期共振空",
                "direction": "short", "confidence": round(conf, 1),
                "reasons": [f"1h{price_chg_1h:.1f}%", f"4h{price_chg_4h:.1f}%", f"1d{price_chg_1d:.1f}%"],
                "score": round(abs(price_chg_1h + price_chg_4h + price_chg_1d) * 1.5, 1),
            }
        return None

    # P9: 大单追踪
    def pattern_9(self, d: dict) -> Optional[dict]:
        """5分钟内 >=3 笔 >50kU 同向成交"""
        # 这个依赖 aggTrade 流，不在指标数据中，由 engine 单独处理
        whale_trades = d.get("whale_trades", [])
        if len(whale_trades) >= 3:
            buys = sum(1 for t in whale_trades if not t.get("is_buyer_maker"))
            sells = len(whale_trades) - buys
            if buys > sells:
                direction = "long"
                score = buys * 25
            else:
                direction = "short"
                score = sells * 25
            conf = min(100, score)
            return {
                "pattern_id": 9, "pattern_name": "大单追踪",
                "direction": direction, "confidence": round(conf, 1),
                "reasons": [f"{len(whale_trades)}笔", f"买{buys}卖{sells}"],
                "score": round(score, 1),
            } if conf >= 30 else None
        return None

    # P10: 清算级联
    def pattern_10(self, d: dict) -> Optional[dict]:
        """同方向连续强平 >= 5 笔"""
        liquidations = d.get("liquidations", [])
        if len(liquidations) >= 5:
            longs = sum(1 for l in liquidations if l.get("side") == "SELL")
            shorts = len(liquidations) - longs
            direction = "short" if longs > shorts else "long"
            conf = min(100, len(liquidations) * 12)
            return {
                "pattern_id": 10, "pattern_name": "清算级联",
                "direction": direction, "confidence": round(conf, 1),
                "reasons": [f"{len(liquidations)}笔强平"],
                "score": round(len(liquidations) * 10, 1),
            } if conf >= 30 else None
        return None

    # P11: OI+价格同跌（多头投降）
    def pattern_11(self, d: dict) -> Optional[dict]:
        oi_chg = d.get("oi_change_pct", 0)
        price_chg_4h = d.get("price_chg_4h", 0) or 0
        if oi_chg < -5 and price_chg_4h < -3:
            conf = min(100, abs(oi_chg) * 2 + abs(price_chg_4h) * 3)
            return {
                "pattern_id": 11, "pattern_name": "多头投降",
                "direction": "long",  # 潜在底部
                "confidence": round(conf, 1),
                "reasons": [f"OI{oi_chg:.1f}%", f"价{price_chg_4h:.1f}%"],
                "score": round(abs(oi_chg) * 2 + abs(price_chg_4h) * 2, 1),
            } if conf >= 30 else None
        return None

    # P12: OI+价格同涨（真突破）
    def pattern_12(self, d: dict) -> Optional[dict]:
        oi_chg = d.get("oi_change_pct", 0)
        price_chg_4h = d.get("price_chg_4h", 0) or 0
        if oi_chg > 5 and price_chg_4h > 3:
            conf = min(100, oi_chg * 2 + price_chg_4h * 3)
            return {
                "pattern_id": 12, "pattern_name": "真突破",
                "direction": "long",
                "confidence": round(conf, 1),
                "reasons": [f"OI+{oi_chg:.1f}%", f"价+{price_chg_4h:.1f}%"],
                "score": round(oi_chg * 2 + price_chg_4h * 2, 1),
            } if conf >= 30 else None
        return None

    # P13: 资金费率极端
    def pattern_13(self, d: dict) -> Optional[dict]:
        funding = d.get("funding_rate", 0)
        if funding > 0.08:
            conf = min(100, funding * 300)
            return {
                "pattern_id": 13, "pattern_name": "费率极端偏多",
                "direction": "short",  # 拥挤反转风险
                "confidence": round(conf, 1),
                "reasons": [f"费率{funding:.3%}"],
                "score": round(funding * 300, 1),
            } if conf >= 30 else None
        elif funding < -0.03:
            conf = min(100, abs(funding) * 500)
            return {
                "pattern_id": 13, "pattern_name": "费率极端偏空",
                "direction": "long",
                "confidence": round(conf, 1),
                "reasons": [f"费率{funding:.3%}"],
                "score": round(abs(funding) * 400, 1),
            } if conf >= 30 else None
        return None


class AccumulationScorer:
    """吸筹评分器 — 综合评分 + 方向判断"""

    def __init__(self, detector: PatternDetector):
        self.detector = detector

    def score(self, data: dict) -> dict:
        return self.detector.score_all(data)
