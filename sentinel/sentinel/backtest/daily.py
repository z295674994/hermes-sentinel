"""
每日回测 — 验证昨日告警胜率（实时区 + 埋伏区）
支持 AI 权重优化建议
使用 DeepSeek V4 Pro 编写
"""
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta


def backtest_zone(db, zone: str, yesterday: str) -> dict:
    """回测指定分区的昨日告警"""
    alerts = db.execute(
        """SELECT * FROM alerts WHERE zone=? 
           AND date(timestamp,'unixepoch')=? AND backtested_at IS NULL""",
        (zone, yesterday)
    ).fetchall()
    
    if not alerts:
        return {"zone": zone, "total": 0, "wins": 0, "win_rate": 0}
    
    wins = 0
    losses = 0
    total_pnl = 0
    max_win = 0
    max_loss = 0
    
    for alert in alerts:
        symbol = alert["symbol"]
        ts = alert["timestamp"]
        direction = alert["direction"]
        entry_price = alert["entry_price"] or 0
        
        if not entry_price:
            continue
        
        # 获取多时间窗口价格
        prices = {}
        for offset_min, col in [
            (5, "price_5min"), (15, "price_15min"), (60, "price_1h"),
            (240, "price_4h"), (1440, "price_24h")
        ]:
            target_ts = ts + offset_min * 60
            row = db.execute(
                "SELECT close FROM kline_1m WHERE symbol=? AND open_time <= ? ORDER BY open_time DESC LIMIT 1",
                (symbol, target_ts * 1000)
            ).fetchone()
            if row:
                prices[col] = row[0]
        
        # 计算期间最优/最差价
        all_prices = []
        for col in ["price_5min", "price_15min", "price_1h", "price_4h", "price_24h"]:
            if col in prices:
                all_prices.append(prices[col])
        
        max_fav = max(all_prices) if all_prices else entry_price
        max_adv = min(all_prices) if all_prices else entry_price
        
        # 判断胜负 (5分钟方向)
        price_5min = prices.get("price_5min", entry_price)
        pnl = (price_5min - entry_price) / entry_price * 100
        is_win = (direction == "long" and pnl > 0) or (direction == "short" and pnl < 0)
        
        if is_win:
            wins += 1
            total_pnl += abs(pnl)
            max_win = max(max_win, abs(pnl))
        else:
            losses += 1
            total_pnl -= abs(pnl)
            max_loss = max(max_loss, abs(pnl))
        
        # 更新告警回测字段
        db.execute(
            """UPDATE alerts SET 
               price_5min=?, price_15min=?, price_1h=?, price_4h=?, price_24h=?,
               max_favorable=?, max_adverse=?,
               is_win=?, pnl_pct=?, backtested_at=?
               WHERE id=?""",
            (prices.get("price_5min"), prices.get("price_15min"),
             prices.get("price_1h"), prices.get("price_4h"), prices.get("price_24h"),
             max_fav, max_adv,
             1 if is_win else 0, round(pnl, 2), int(time.time()), alert["id"])
        )
    
    db.commit()
    
    total = wins + losses
    win_rate = wins / total * 100 if total > 0 else 0
    avg_pnl = total_pnl / total if total > 0 else 0
    
    result = {
        "zone": zone,
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "avg_pnl": round(avg_pnl, 2),
        "max_win": round(max_win, 2),
        "max_loss": round(max_loss, 2),
    }
    return result


def compute_pattern_stats(db, zone: str, yesterday: str) -> dict:
    """按模式统计胜率"""
    alerts = db.execute(
        """SELECT large_patterns, is_win FROM alerts 
           WHERE zone=? AND date(timestamp,'unixepoch')=?""",
        (zone, yesterday)
    ).fetchall()
    
    pattern_stats = {}
    for a in alerts:
        patterns = (a["large_patterns"] or "").split(",")
        for p in patterns:
            p = p.strip()
            if not p:
                continue
            if p not in pattern_stats:
                pattern_stats[p] = {"total": 0, "wins": 0}
            pattern_stats[p]["total"] += 1
            if a["is_win"]:
                pattern_stats[p]["wins"] += 1
    
    for p in pattern_stats:
        s = pattern_stats[p]
        s["win_rate"] = round(s["wins"] / s["total"] * 100, 1) if s["total"] > 0 else 0
    
    return pattern_stats


def backtest_daily(db_path: str):
    """完整每日回测"""
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    results = []
    for zone in ["realtime", "ambush"]:
        result = backtest_zone(db, zone, yesterday)
        results.append(result)
        print(f"[{zone}] {result}")
    
    # 按模式统计
    for zone in ["realtime", "ambush"]:
        pattern_stats = compute_pattern_stats(db, zone, yesterday)
        if pattern_stats:
            print(f"
[{zone} 模式胜率]")
            for p, s in sorted(pattern_stats.items(), key=lambda x: x[1]["win_rate"], reverse=True):
                print(f"  {p}: {s['wins']}/{s['total']} = {s['win_rate']}%")
    
    # 保存汇总
    for r in results:
        if r["total"] == 0:
            continue
        db.execute(
            """INSERT INTO backtest_summaries 
               (date, zone, period, total_alerts, wins, win_rate, avg_pnl, max_pnl, max_drawdown, pattern_stats)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (yesterday, r["zone"], "daily", r["total"], r["wins"],
             r["win_rate"], r["avg_pnl"], r["max_win"], r["max_loss"],
             json.dumps({"pattern_stats": compute_pattern_stats(db, r["zone"], yesterday)}))
        )
    
    db.commit()
    db.close()
    
    return results


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "sentinel/sentinel.db"
    backtest_daily(db_path)
