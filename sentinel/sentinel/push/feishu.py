"""
飞书推送 - 告警卡片格式化 + Webhook 发送
使用 DeepSeek V4 Pro 编写
"""
import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

import aiohttp

log = logging.getLogger(__name__)


class FeishuPusher:
    """飞书 Webhook 推送"""

    def __init__(self, config: dict):
        feishu_config = config.get("feishu", {})
        self.webhook_url = feishu_config.get("webhook_url", "")
        self.heartbeat_interval = feishu_config.get("heartbeat_interval", 300)
        self._last_heartbeat = 0
        self._running = False
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    def format_alert(self, alert: dict) -> dict:
        """格式化为飞书交互卡片消息"""
        symbol = alert["symbol"].upper()
        level = alert.get("level", "P1")
        direction = alert.get("direction", "neutral")
        large_score = alert.get("large_score", 0)
        small_score = alert.get("small_score", 0)
        combined_score = alert.get("combined_score", 0)

        dir_emoji = "\U0001f4c8" if direction == "long" else "\U0001f4c9" if direction == "short" else "\u2796"
        dir_text = "做多" if direction == "long" else "做空" if direction == "short" else "观望"
        level_color = {"P0": "red", "P1": "orange", "P2": "yellow"}.get(level, "default")

        if level == "P0":
            action = f"强烈{dir_text} - 大小级别高度共振"
        elif level == "P1":
            action = f"关注{dir_text} - 信号较强"
        else:
            action = f"观望 - 信号偏弱"

        lines = []
        lines.append(f"{'[P0]' if level == 'P0' else '[P1]' if level == 'P1' else '[P2]'} {symbol} {dir_emoji}")
        lines.append(f"综合评分: {combined_score:.1f}/100")
        lines.append(f"大级别: {large_score:.1f}分 | 小级别: {small_score:.1f}分")
        lines.append(f"操作建议: {action}")
        lines.append("")

        if alert.get("ai_entry_price"):
            lines.append("[入场价位]")
            lines.append(f"  主入场: {alert['ai_entry_price']}")
            if alert.get("ai_entry_backup_1"):
                lines.append(f"  备用1: {alert['ai_entry_backup_1']}")
            if alert.get("ai_entry_backup_2"):
                lines.append(f"  备用2: {alert['ai_entry_backup_2']}")
            lines.append("")

        if alert.get("ai_tp_1"):
            lines.append("[止盈目标]")
            for i in range(1, 4):
                pk = f"ai_tp_{i}"
                pctk = f"ai_tp_{i}_pct"
                if alert.get(pk):
                    arrow = "UP" if direction == "long" else "DOWN"
                    lines.append(f"  TP{i}: {alert[pk]} ({arrow}{alert.get(pctk, 0) or 0:.1f}%)")
            lines.append("")

        if alert.get("ai_sl_1"):
            lines.append("[止损价位]")
            for i in range(1, 4):
                pk = f"ai_sl_{i}"
                pctk = f"ai_sl_{i}_pct"
                if alert.get(pk):
                    arrow = "DOWN" if direction == "long" else "UP"
                    lines.append(f"  SL{i}: {alert[pk]} ({arrow}{alert.get(pctk, 0) or 0:.1f}%)")
            lines.append("")

        if alert.get("ai_analysis"):
            lines.append(f"[AI分析] {alert['ai_analysis']}")
            lines.append("")

        if alert.get("large_patterns"):
            lines.append(f"大级别信号: {', '.join(alert['large_patterns'])}")
        if alert.get("small_patterns"):
            lines.append(f"小级别信号: {', '.join(alert['small_patterns'])}")

        ts = alert.get("timestamp", time.time())
        lines.append(f"--- {time.strftime('%H:%M:%S', time.localtime(ts))}")

        content = "\n".join(lines)

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"{chr(0x1f534) if level == 'P0' else chr(0x1f7e1) if level == 'P1' else chr(0x1f7e2)} {symbol} - {dir_text}"
                    },
                    "template": level_color,
                },
                "elements": [
                    {"tag": "markdown", "content": content}
                ],
            },
        }

    def format_heartbeat(self, stats: dict) -> dict:
        """格式化心跳消息"""
        lines = []
        lines.append("[Sentinel 心跳]")
        ws_status = "在线" if stats.get("ws_connected") else "离线"
        lines.append(f"WS: {ws_status} | 运行: {stats.get('uptime_min', 0)}分钟")
        lines.append(f"大级别周期: {stats.get('large_cycles', 0)} | 秒级事件: {stats.get('small_events', 0)}")
        lines.append(f"告警: P0={stats.get('alerts_p0', 0)} P1={stats.get('alerts_p1', 0)} P2={stats.get('alerts_p2', 0)}")
        lines.append(f"埋伏区: {stats.get('ambush_count', 0)}个币")
        lines.append(f"--- {time.strftime('%Y-%m-%d %H:%M:%S')}")

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": "Sentinel 心跳"},
                    "template": "blue",
                },
                "elements": [
                    {"tag": "markdown", "content": "\n".join(lines)}
                ],
            },
        }

    async def send(self, payload: dict):
        """发送飞书消息"""
        if not self.webhook_url:
            log.warning("Feishu webhook URL not configured")
            return False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload,
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return True
                    text = await resp.text()
                    log.error("Feishu push failed %d: %s", resp.status, text[:200])
                    return False
        except Exception as e:
            log.error("Feishu push error: %s", e)
            return False

    async def push_alert(self, alert: dict):
        card = self.format_alert(alert)
        await self._queue.put(card)

    async def push_heartbeat(self, stats: dict):
        card = self.format_heartbeat(stats)
        await self.send(card)

    async def _consume(self):
        while self._running:
            try:
                card = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self.send(card)
                self._queue.task_done()
                await asyncio.sleep(0.5)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.error("Queue consume error: %s", e)

    async def start(self):
        self._running = True
        asyncio.create_task(self._consume())
        log.info("Feishu pusher started")

    async def stop(self):
        self._running = False
        log.info("Feishu pusher stopped")
