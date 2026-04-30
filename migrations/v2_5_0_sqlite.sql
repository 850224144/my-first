-- v2.5.0 SQLite 增量迁移
-- 执行前请备份数据库。
-- 本迁移尽量采用新增表方式，避免破坏已有结构。

CREATE TABLE IF NOT EXISTS final_signal_daily (
    trade_date TEXT,
    symbol TEXT,
    stock_name TEXT,
    signal_status TEXT,
    signal_level TEXT,
    entry_type TEXT,
    should_write_paper_trade INTEGER,
    planned_buy_price REAL,
    stop_loss REAL,
    target_1 REAL,
    target_2 REAL,
    time_stop_days INTEGER,

    daily_2buy_score REAL,
    sector_score REAL,
    leader_score REAL,
    weekly_score REAL,
    yuanjun_score REAL,
    rescue_candle_score REAL,
    risk_pct REAL,

    theme_name TEXT,
    sector_state TEXT,
    leader_type TEXT,
    weekly_state TEXT,
    yuanjun_state TEXT,
    divergence_count INTEGER,

    signal_reasons TEXT,
    risk_flags TEXT,
    blocking_flags TEXT,
    downgrade_flags TEXT,
    upgrade_reasons TEXT,
    raw_json TEXT,
    created_at TEXT,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS open_recheck_daily (
    trade_date TEXT,
    symbol TEXT,
    stock_name TEXT,
    open_status TEXT,
    entry_type TEXT,
    should_write_paper_trade INTEGER,
    open_price REAL,
    current_price REAL,
    planned_buy_price REAL,
    stop_loss REAL,
    reasons TEXT,
    risk_flags TEXT,
    raw_json TEXT,
    created_at TEXT,
    PRIMARY KEY (trade_date, symbol, open_status)
);

CREATE TABLE IF NOT EXISTS paper_trade_ext (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    stock_name TEXT,
    signal_status TEXT,
    entry_type TEXT,
    buy_date TEXT,
    sellable_date TEXT,
    buy_price REAL,
    planned_buy_price REAL,
    stop_loss REAL,
    target_1 REAL,
    target_2 REAL,
    time_stop_days INTEGER,
    risk_pct REAL,

    daily_2buy_score REAL,
    sector_score REAL,
    leader_score REAL,
    weekly_score REAL,
    yuanjun_score REAL,
    theme_name TEXT,
    leader_type TEXT,
    yuanjun_state TEXT,

    signal_reasons TEXT,
    risk_flags TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS notification_state (
    trade_date TEXT,
    symbol TEXT,
    channel TEXT,
    last_status TEXT,
    last_key_hash TEXT,
    last_sent_at TEXT,
    PRIMARY KEY (trade_date, symbol, channel)
);

CREATE INDEX IF NOT EXISTS idx_final_signal_daily_status
ON final_signal_daily(trade_date, signal_status);

CREATE INDEX IF NOT EXISTS idx_paper_trade_ext_symbol
ON paper_trade_ext(symbol, buy_date);
