"""
DeepSeek AI 分析 — 告警分析 + 操作建议生成
使用 DeepSeek V4 Pro 编写
"""
import json
import logging
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

ANALYSIS_PROMPT = """你是专业加密货币合约交易分析师。基于以下实时市场数据，给出操作建议。

## 数据
- 交易对: {symbol}
- 当前价格: {price}
- 综合评分: {combined_score}/100 ({level})
- 大级别评分: {large_score}/100 方向: {large_dir}
- 小级别评分: {small_score}/100 方向: {small_dir}
- 大级别模式: {large_patterns}
- 小级别模式: {small_patterns}
- 支撑位: {supports}
- 阻力位: {resistances}
- OI变化: {oi_change}%
- CVD方向: {cvd_dir}
- 资金费率: {funding}
- 方向一致: {aligned}

## 要求
请返回 JSON 格式（不要其他任何内容）:

```json
{{
  "direction": "long/short/neutral",
  "confidence": 0-100,
  "summary": "一句话总结核心逻辑，不超过80字",
  
  "entry": {{
    "main": 实际可成交的挂单价(当前价±1-5%),
    "backup_1": 备用补仓价1(比main更优),
    "backup_2": 备用补仓价2(比main更优)
  }},
  
  "take_profit": [
    {{"price": 止盈价1, "pct": 涨幅%}},
    {{"price": 止盈价2, "pct": 涨幅%}},
    {{"price": 止盈价3, "pct": 涨幅%}}
  ],
  
  "stop_loss": [
    {{"price": 止损价1, "pct": 跌幅%}},
    {{"price": 止损价2, "pct": 跌幅%}},
    {{"price": 止损价3, "pct": 跌幅%}}
  ],
  
  "analysis": "200字以内的简短分析，说明为什么推荐做多/做空，结合大级别和小级别数据，说明支撑位和阻力位在哪里"
}}
```

**重要：**
- 入场价必须是实际可成交的价格（当前价附近），不能给出无法成交的极端价格
- 止盈止损按当前价的百分比计算，分3档
- 做多: TP在价格上方，SL在价格下方。做空: TP在价格下方，SL在价格上方
- 使用中文
"""


class DeepSeekAnalyzer:
    """DeepSeek 分析客户端"""

    def __init__(self, config: dict):
        ai_config = config.get("ai", {})
        self.api_key = ai_config.get("api_key", "")
        self.api_base = ai_config.get("api_base", "https://api.deepseek.com/v1")
        self.model = ai_config.get("model", "deepseek-chat")
        self.max_tokens = ai_config.get("max_tokens", 1200)
        self.temperature = ai_config.get("temperature", 0.3)
        self._session = None

    def _build_prompt(self, alert: dict) -> str:
        """构造分析 prompt"""
        large_data = alert.get("large_data", {})
        return ANALYSIS_PROMPT.format(
            symbol=alert["symbol"].upper(),
            price=alert.get("large_data", {}).get("price", alert.get("price", "N/A")),
            combined_score=alert.get("combined_score", 0),
            level=alert.get("level", "N/A"),
            large_score=alert.get("large_score", 0),
            large_dir=alert.get("large_direction", "neutral"),
            small_score=alert.get("small_score", 0),
            small_dir=alert.get("small_direction", "neutral"),
            large_patterns=", ".join(alert.get("large_patterns", [])),
            small_patterns=", ".join(alert.get("small_patterns", [])),
            supports=json.dumps(large_data.get("support", [])),
            resistances=json.dumps(large_data.get("resistance", [])),
            oi_change=large_data.get("oi_change_pct", 0),
            cvd_dir=large_data.get("cvd_direction", "flat"),
            funding=large_data.get("funding_rate", 0),
            aligned="是" if alert.get("directions_align") else "否",
        )

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=5)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def analyze(self, alert: dict, timeout: int = 30) -> Optional[dict]:
        """调用 DeepSeek 分析告警"""
        prompt = self._build_prompt(alert)
        url = f"{self.api_base}/chat/completions"

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "你是加密货币交易分析师。只返回JSON，不要其他内容。"},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            session = await self._get_session()
            async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        log.error("DeepSeek API error %d: %s", resp.status, text[:200])
                        return None

                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]

                    # 清理可能的 markdown 代码块
                    content = content.strip()
                    if content.startswith("```"):
                        content = content.split("```")[1]
                        if content.startswith("json"):
                            content = content[4:]
                    return json.loads(content)

        except Exception as e:
            log.error("DeepSeek API call failed: %s", e)
            return None

    def merge_alert(self, alert: dict, ai_result: Optional[dict]) -> dict:
        """合并告警和 AI 分析结果"""
        result = {**alert}

        if ai_result:
            result["ai_direction"] = ai_result.get("direction", "")
            result["ai_confidence"] = ai_result.get("confidence", 0)
            result["ai_summary"] = ai_result.get("summary", "")
            result["ai_analysis"] = ai_result.get("analysis", "")

            entry = ai_result.get("entry", {})
            result["ai_entry_price"] = entry.get("main")
            result["ai_entry_backup_1"] = entry.get("backup_1")
            result["ai_entry_backup_2"] = entry.get("backup_2")

            tp = ai_result.get("take_profit", [])
            for i, t in enumerate(tp[:3], 1):
                result[f"ai_tp_{i}"] = t.get("price")
                result[f"ai_tp_{i}_pct"] = t.get("pct")

            sl = ai_result.get("stop_loss", [])
            for i, s in enumerate(sl[:3], 1):
                result[f"ai_sl_{i}"] = s.get("price")
                result[f"ai_sl_{i}_pct"] = s.get("pct")
        else:
            # 无 AI 分析时使用默认值
            result["ai_summary"] = "AI分析暂不可用"
            result["ai_analysis"] = ""

        return result
