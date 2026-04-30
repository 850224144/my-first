-- v2.4.0 SQLite 增量迁移
-- 使用前请先备份你的数据库。
-- 由于不知道你当前表结构，以下语句采用 CREATE TABLE IF NOT EXISTS 和“附加表”方式，避免直接破坏原表。

CREATE TABLE IF NOT EXISTS xgb_market_indicator (
    trade_date TEXT PRIMARY KEY,
    rise_count INTEGER,
    fall_count INTEGER,
    flat_count INTEGER,
    limit_up_count INTEGER,
    limit_down_count INTEGER,
    limit_up_broken_count INTEGER,
    limit_up_broken_ratio REAL,
    source TEXT DEFAULT 'xgb',
    stale INTEGER DEFAULT 0,
    raw_json TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS xgb_pool_daily (
    trade_date TEXT,
    pool_name TEXT,
    symbol TEXT,
    stock_name TEXT,
    price REAL,
    change_percent REAL,
    volume REAL,                 -- 手
    amount REAL,                 -- 元
    turnover_ratio REAL,         -- 百分比数值
    circulating_market_cap REAL, -- 元
    limit_up_days INTEGER,
    first_limit_up_time INTEGER,
    last_limit_up_time INTEGER,
    surge_reason TEXT,
    source TEXT DEFAULT 'xgb',
    stale INTEGER DEFAULT 0,
    raw_json TEXT,
    created_at TEXT,
    PRIMARY KEY (trade_date, pool_name, symbol)
);

CREATE TABLE IF NOT EXISTS theme_heat_daily (
    trade_date TEXT,
    theme_name TEXT,
    limit_up_count INTEGER,
    continuous_limit_up_count INTEGER,
    strong_stock_count INTEGER,
    broken_count INTEGER,
    limit_down_count INTEGER,
    max_limit_up_days INTEGER,
    theme_heat_score REAL,
    raw_json TEXT,
    created_at TEXT,
    PRIMARY KEY (trade_date, theme_name)
);

CREATE TABLE IF NOT EXISTS signal_score_daily (
    trade_date TEXT,
    symbol TEXT,
    stock_name TEXT,
    daily_2buy_score REAL,
    sector_score REAL,
    sector_state TEXT,
    leader_type TEXT,
    leader_score REAL,
    weekly_score REAL,
    weekly_state TEXT,
    theme_name TEXT,
    theme_heat_score REAL,
    yuanjun_score REAL,
    yuanjun_state TEXT,
    divergence_score REAL,
    divergence_count INTEGER,
    rescue_candle_score REAL,
    risk_pct REAL,
    signal_status TEXT,
    signal_reasons TEXT,
    risk_flags TEXT,
    created_at TEXT,
    PRIMARY KEY (trade_date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_xgb_pool_daily_symbol
ON xgb_pool_daily(symbol);

CREATE INDEX IF NOT EXISTS idx_signal_score_daily_symbol
ON signal_score_daily(symbol);
