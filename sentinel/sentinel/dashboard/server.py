"""仪表盘 HTTP + WebSocket 服务"""
import asyncio
import json
import logging
import time
from pathlib import Path

from aiohttp import web

log = logging.getLogger(__name__)

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hermes Sentinel</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#090d13;color:#e0e6ed;min-height:100vh}
.header{background:#111827;padding:16px 24px;border-bottom:1px solid #1f2937;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:20px;color:#60a5fa}
.status{display:flex;gap:12px;font-size:13px}
.status-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:4px}
.status-dot.green{background:#22c55e}.status-dot.red{background:#ef4444}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px;max-width:1600px;margin:0 auto}
.card{background:#111827;border:1px solid #1f2937;border-radius:8px;padding:16px}
.card h2{font-size:15px;color:#9ca3af;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px}
.alert-item{background:#1a2332;border-radius:6px;padding:12px;margin-bottom:8px;border-left:3px solid #374151}
.alert-item.P0{border-left-color:#ef4444}.alert-item.P1{border-left-color:#f59e0b}.alert-item.P2{border-left-color:#22c55e}
.alert-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.alert-symbol{font-weight:700;font-size:15px}
.alert-badge{font-size:11px;padding:2px 8px;border-radius:4px;font-weight:700}
.alert-badge.P0{background:#ef444420;color:#fca5a5}
.alert-badge.P1{background:#f59e0b20;color:#fcd34d}
.alert-badge.P2{background:#22c55e20;color:#86efac}
.alert-score{font-size:12px;color:#9ca3af}
.alert-info{font-size:12px;color:#6b7280;margin-top:4px}
.ambush-item{display:flex;align-items:center;gap:8px;padding:8px;background:#1a2332;border-radius:6px;margin-bottom:6px}
.ambush-state{font-size:11px;padding:2px 8px;border-radius:4px}
.ambush-state.ready{background:#ef444420;color:#fca5a5}
.ambush-state.loading{background:#f59e0b20;color:#fcd34d}
.ambush-state.building{background:#3b82f620;color:#93c5fd}
.ambush-symbol{font-weight:600;font-size:14px}
.ambush-score{font-size:12px;color:#9ca3af;margin-left:auto}
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
.stat-box{background:#1a2332;border-radius:6px;padding:12px;text-align:center}
.stat-value{font-size:24px;font-weight:700;color:#60a5fa}
.stat-label{font-size:11px;color:#6b7280;margin-top:4px}
</style>
</head>
<body>
<div class="header">
<h1>Hermes Sentinel</h1>
<div class="status"><span id="wsStatus"><span class="status-dot red"></span>离线</span><span id="uptime"></span></div>
</div>
<div class="grid">
<div class="card">
<h2>实时告警</h2><div id="alerts"></div>
</div>
<div class="card">
<h2>埋伏区</h2><div id="ambush"></div>
</div>
<div class="card">
<h2>统计</h2>
<div class="stats-grid">
<div class="stat-box"><div class="stat-value" id="statP0">0</div><div class="stat-label">P0 告警</div></div>
<div class="stat-box"><div class="stat-value" id="statP1">0</div><div class="stat-label">P1 告警</div></div>
<div class="stat-box"><div class="stat-value" id="statCycles">0</div><div class="stat-label">扫描周期</div></div>
<div class="stat-box"><div class="stat-value" id="statAmbush">0</div><div class="stat-label">埋伏币种</div></div>
</div>
</div>
<div class="card">
<h2>最近信号</h2><div id="signals"></div>
</div>
</div>
<script>
const ws = new WebSocket(`ws://${location.hostname}:8889`);
ws.onopen = () => document.getElementById('wsStatus').innerHTML = '<span class="status-dot green"></span>在线';
ws.onclose = () => document.getElementById('wsStatus').innerHTML = '<span class="status-dot red"></span>离线';
ws.onmessage = (e) => {
const d = JSON.parse(e.data);
if(d.alerts) updateAlerts(d.alerts);
if(d.ambush) updateAmbush(d.ambush);
if(d.stats) updateStats(d.stats);
};
function updateAlerts(alerts) {
document.getElementById('alerts').innerHTML = alerts.slice(0,5).map(a =>
`<div class="alert-item ${a.level}">
<div class="alert-header"><span class="alert-symbol">${a.symbol}</span><span class="alert-badge ${a.level}">${a.level}</span></div>
<div class="alert-score">综合:${a.combined_score} 大:${a.large_score} 小:${a.small_score}</div>
<div class="alert-info">${a.direction} | ${(a.large_patterns||[]).join(',')}</div>
</div>`).join('') || '<p style="color:#6b7280">暂无告警</p>';
}
function updateAmbush(ambush) {
document.getElementById('ambush').innerHTML = ambush.slice(0,10).map(a =>
`<div class="ambush-item"><span class="ambush-state ${a.state}">${a.state==='ready'?'待爆发':a.state==='loading'?'蓄力中':'建仓中'}</span><span class="ambush-symbol">${a.symbol}</span><span class="ambush-score">${a.large_score}分</span></div>`
).join('') || '<p style="color:#6b7280">暂无埋伏</p>';
}
function updateStats(s) {
document.getElementById('statP0').textContent = s.alerts_p0||0;
document.getElementById('statP1').textContent = s.alerts_p1||0;
document.getElementById('statCycles').textContent = s.cycles||0;
document.getElementById('statAmbush').textContent = s.ambush_count||0;
document.getElementById('uptime').textContent = (s.uptime_min||0)+'min';
}
</script>
</body>
</html>"""


class Dashboard:
    def __init__(self, config, sentinel):
        self.config = config
        self.sentinel = sentinel
        dash_config = config.get("dashboard", {})
        self.host = dash_config.get("host", "0.0.0.0")
        self.port = dash_config.get("port", 8888)
        self.ws_port = dash_config.get("ws_port", 8889)
        self._ws_clients = set()

    async def handle_http(self, request):
        return web.Response(text=HTML_TEMPLATE, content_type="text/html")

    async def handle_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_clients.add(ws)
        try:
            async for msg in ws:
                pass
        finally:
            self._ws_clients.discard(ws)
        return ws

    async def broadcast(self):
        """定时广播状态"""
        while True:
            await asyncio.sleep(2)
            if not self._ws_clients:
                continue
            try:
                # 收集数据
                scores = self.sentinel.large_engine.get_top_scores(10)
                ambush = self.sentinel.resonance.get_ambush_symbols()
                
                data = {
                    "alerts": [
                        {
                            "symbol": s["symbol"].upper(),
                            "level": "P1",
                            "combined_score": s.get("total_score", 0),
                            "large_score": s.get("total_score", 0),
                            "small_score": 0,
                            "direction": s.get("direction", "neutral"),
                            "large_patterns": [m["pattern_name"] for m in s.get("matches", [])],
                        }
                        for s in scores
                    ],
                    "ambush": [
                        {
                            "symbol": a["symbol"].upper(),
                            "state": a.get("state", "building"),
                            "large_score": a.get("large_score", 0),
                        }
                        for a in ambush
                    ],
                    "stats": {
                        "alerts_p0": self.sentinel._stats.get("alerts_p0", 0),
                        "alerts_p1": self.sentinel._stats.get("alerts_p1", 0),
                        "cycles": self.sentinel.large_engine.stats.get("cycles", 0),
                        "ambush_count": len(ambush),
                        "uptime_min": int((time.time() - self.sentinel._stats["start_time"]) / 60),
                    },
                }
                msg = json.dumps(data)
                for ws in list(self._ws_clients):
                    try:
                        await ws.send_str(msg)
                    except Exception:
                        self._ws_clients.discard(ws)
            except Exception as e:
                log.error("Broadcast error: %s", e)

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self.handle_http)
        app.router.add_get("/ws", self.handle_ws)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        log.info("Dashboard at http://%s:%d", self.host, self.port)
        
        asyncio.create_task(self.broadcast())
