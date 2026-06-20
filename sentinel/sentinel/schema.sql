-- Hermes Sentinel DB Schema v2.0
-- 使用 DeepSeek V4 Pro 编写

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-64000;  -- 64MB cache
PRAGMA temp_store=MEMORY;

-- ============================================
-- K线缓存表
-- ============================================

-- 1m K线 (滚存90天，回测核心数据)
CREATE TABLE IF NOT EXISTS kline_1m (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    open_time INTEGER NOT NULL,      -- K线开盘时间(ms)
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    quote_volume REAL NOT NULL,
    trades_count INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(symbol, open_time)
);
CREATE INDEX IF NOT EXISTS idx_kline_1m_sym_time ON kline_1m(symbol, open_time);

-- 大级别 K线 (永久存储)
CREATE TABLE IF NOT EXISTS kline_large (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,          -- 1h/4h/1d/1w
    open_time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    quote_volume REAL NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(symbol, interval, open_time)
);
CREATE INDEX IF NOT EXISTS idx_kline_large_sym_int_time ON kline_large(symbol, interval, open_time);

-- ============================================
-- 评分快照表
-- ============================================
CREATE TABLE IF NOT EXISTS score_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    large_score REAL,                -- 大级别评分 0-100
    small_score REAL,                -- 小级别评分 0-100
    combined_score REAL,             -- 综合评分
    large_patterns TEXT,             -- JSON: 触发的大级别模式
    small_patterns TEXT,             -- JSON: 触发的小级别模式
    oi_change_pct REAL,              -- OI变化%
    cvd_direction TEXT,              -- CVD方向: up/down/flat
    funding_rate REAL,               -- 资金费率
    long_short_ratio REAL,           -- 多空比
    price REAL,                      -- 当前价格
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_scores_sym_time ON score_snapshots(symbol, timestamp);

-- ============================================
-- 埋伏区表
-- ============================================
CREATE TABLE IF NOT EXISTS ambush_zone (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    state TEXT NOT NULL,             -- building/loading/ready
    entered_at INTEGER NOT NULL,     -- 进入埋伏区时间
    last_updated INTEGER NOT NULL,
    large_score REAL,                -- 最近大级别评分
    oi_trend TEXT,                   -- OI趋势
    cvd_trend TEXT,                  -- CVD趋势
    consecutive_signals INTEGER,     -- 连续正信号次数
    exited_at INTEGER,               -- 退出时间(NULL=仍在)
    exit_reason TEXT,                -- 退出原因
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_ambush_sym ON ambush_zone(symbol, exited_at);

-- ============================================
-- 告警表 (含回测字段)
-- ============================================
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    alert_level TEXT NOT NULL,       -- P0/P1/P2
    zone TEXT NOT NULL,              -- ambush/realtime
    direction TEXT NOT NULL,         -- long/short
    
    -- 评分详情
    large_score REAL,
    small_score REAL,
    combined_score REAL,
    large_patterns TEXT,             -- JSON
    small_patterns TEXT,             -- JSON
    
    -- AI 分析
    ai_direction TEXT,               -- AI判断方向
    ai_confidence REAL,              -- AI置信度
    ai_entry_price REAL,             -- AI推荐入场价
    ai_entry_backup_1 REAL,          -- 备用入场1
    ai_entry_backup_2 REAL,          -- 备用入场2
    ai_tp_1 REAL,                    -- 第一止盈
    ai_tp_1_pct REAL,                -- 第一止盈%
    ai_tp_2 REAL,
    ai_tp_2_pct REAL,
    ai_tp_3 REAL,
    ai_tp_3_pct REAL,
    ai_sl_1 REAL,                    -- 第一止损
    ai_sl_1_pct REAL,
    ai_sl_2 REAL,
    ai_sl_2_pct REAL,
    ai_sl_3 REAL,
    ai_sl_3_pct REAL,
    ai_support TEXT,                 -- JSON: 支撑位列表
    ai_resistance TEXT,              -- JSON: 阻力位列表
    ai_summary TEXT,                 -- AI分析摘要
    ai_full_analysis TEXT,           -- AI完整分析原文
    
    -- 告警时市场快照
    entry_price REAL,
    market_snapshot TEXT,            -- JSON: 完整市场数据
    
    -- 回测字段 (事后填充)
    price_5min REAL,
    price_15min REAL,
    price_1h REAL,
    price_4h REAL,
    price_24h REAL,
    max_favorable REAL,              -- 期间最优价
    max_adverse REAL,                -- 期间最差价
    is_win INTEGER DEFAULT 0,       -- 是否盈利 0/1
    pnl_pct REAL,                    -- 收益率%
    backtested_at INTEGER,           -- 回测时间戳
    
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_alerts_sym_time ON alerts(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_level ON alerts(alert_level, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_zone ON alerts(zone, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_backtested ON alerts(backtested_at);

-- ============================================
-- 回测汇总表
-- ============================================
CREATE TABLE IF NOT EXISTS backtest_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,              -- 回测日期 YYYY-MM-DD
    zone TEXT NOT NULL,              -- realtime/ambush
    period TEXT NOT NULL,            -- daily/weekly
    
    -- 统计
    total_alerts INTEGER,
    wins INTEGER,
    win_rate REAL,
    avg_pnl REAL,
    max_pnl REAL,
    max_drawdown REAL,
    sharpe_ratio REAL,
    
    -- 按模式统计 (JSON)
    pattern_stats TEXT,
    
    -- 按币种统计 (JSON)
    symbol_stats TEXT,
    
    -- AI 优化
    ai_suggestions TEXT,             -- DeepSeek优化建议
    parameter_changes TEXT,          -- JSON: 建议参数调整
    applied INTEGER DEFAULT 0,      -- 是否已应用
    
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_backtest_date ON backtest_summaries(date, zone);

-- ============================================
-- 系统状态表
-- ============================================
CREATE TABLE IF NOT EXISTS system_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    ws_connected INTEGER DEFAULT 1,
    ws_uptime_seconds INTEGER,
    messages_received INTEGER,
    messages_dropped INTEGER,
    engine_large_cycles INTEGER,
    engine_small_events INTEGER,
    alerts_total INTEGER,
    alerts_p0 INTEGER,
    alerts_p1 INTEGER,
    alerts_p2 INTEGER,
    ambush_count INTEGER,
    cpu_percent REAL,
    memory_mb REAL,
    created_at INTEGER DEFAULT (strftime('%s','now'))
);

-- ============================================
-- OI 历史表 (每15分钟写入)
-- ============================================
CREATE TABLE IF NOT EXISTS oi_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    open_interest REAL NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_oi_sym_time ON oi_history(symbol, timestamp);

-- ============================================
-- 资金费率历史表 (每15分钟写入)
-- ============================================
CREATE TABLE IF NOT EXISTS funding_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    mark_price REAL,
    funding_rate REAL NOT NULL,
    next_funding_time INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_funding_sym_time ON funding_history(symbol, timestamp);

-- ============================================
-- 多空比历史表 (每15分钟写入)
-- ============================================
CREATE TABLE IF NOT EXISTS lsr_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    long_ratio REAL NOT NULL,
    short_ratio REAL NOT NULL,
    long_short_ratio REAL NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(symbol, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_lsr_sym_time ON lsr_history(symbol, timestamp);

-- ============================================
-- 持仓快照表 (账户WS实时写入)
-- ============================================
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    position_amount REAL NOT NULL,
    entry_price REAL,
    unrealized_pnl REAL DEFAULT 0,
    margin_type TEXT DEFAULT 'cross',
    leverage INTEGER DEFAULT 1,
    created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_positions_sym_time ON positions(symbol, timestamp);
