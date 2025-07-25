-- Enhanced database schema for efficient 10-year data storage
-- Includes partitioning strategy and optimized indexes

-- ============================================
-- Partitioned Price Tables (by year for efficiency)
-- ============================================

-- Master price table with partitioning info
CREATE TABLE IF NOT EXISTS price_partitions (
    partition_name TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    start_date TEXT,
    end_date TEXT,
    record_count INTEGER,
    size_mb REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Create yearly partitioned tables dynamically
-- Example: prices_2014, prices_2015, etc.

-- Template for yearly price tables
CREATE TABLE IF NOT EXISTS prices_template (
    symbol TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL NOT NULL,
    volume REAL,
    volume_usd REAL,
    market_cap REAL,
    source TEXT NOT NULL,
    exchange TEXT,
    timeframe TEXT DEFAULT '1d',
    is_interpolated BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (symbol, timestamp, source)
);

-- ============================================
-- Enhanced Coin Tracking
-- ============================================

CREATE TABLE IF NOT EXISTS coin_registry (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT UNIQUE,
    rank INTEGER,
    categories JSON,
    tags JSON,
    launch_date TEXT,
    max_supply REAL,
    circulating_supply REAL,
    total_supply REAL,
    platform TEXT,
    contract_addresses JSON,
    explorers JSON,
    websites JSON,
    social_links JSON,
    white_paper TEXT,
    technical_doc TEXT,
    source_code TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    is_trading BOOLEAN DEFAULT TRUE,
    added_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Coin aliases for handling rebrands/symbol changes
CREATE TABLE IF NOT EXISTS coin_aliases (
    current_symbol TEXT NOT NULL,
    old_symbol TEXT NOT NULL,
    change_date TEXT,
    reason TEXT,
    PRIMARY KEY (current_symbol, old_symbol)
);

-- ============================================
-- Market Metrics Tables
-- ============================================

-- Aggregated daily metrics for fast queries
CREATE TABLE IF NOT EXISTS daily_metrics (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    volume_usd REAL,
    market_cap REAL,
    volatility REAL,
    return_1d REAL,
    return_7d REAL,
    return_30d REAL,
    rsi_14 REAL,
    ma_50 REAL,
    ma_200 REAL,
    sharpe_ratio_30d REAL,
    PRIMARY KEY (symbol, date)
);

-- Market dominance tracking
CREATE TABLE IF NOT EXISTS market_dominance (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market_cap REAL,
    dominance_pct REAL,
    rank INTEGER,
    PRIMARY KEY (date, symbol)
);

-- ============================================
-- Enhanced On-Chain Data
-- ============================================

CREATE TABLE IF NOT EXISTS on_chain_metrics (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    chain TEXT NOT NULL,
    active_addresses INTEGER,
    new_addresses INTEGER,
    zero_balance_addresses INTEGER,
    unique_senders INTEGER,
    unique_receivers INTEGER,
    large_tx_count INTEGER,
    whale_concentration REAL,
    exchange_inflow REAL,
    exchange_outflow REAL,
    exchange_netflow REAL,
    total_fees REAL,
    avg_fee REAL,
    median_fee REAL,
    hash_rate REAL,
    difficulty REAL,
    PRIMARY KEY (symbol, date, chain)
);

-- DEX liquidity tracking
CREATE TABLE IF NOT EXISTS dex_liquidity (
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    chain TEXT NOT NULL,
    token0_symbol TEXT,
    token1_symbol TEXT,
    timestamp TEXT NOT NULL,
    reserve0 REAL,
    reserve1 REAL,
    total_liquidity_usd REAL,
    volume_24h REAL,
    fees_24h REAL,
    apy REAL,
    il_risk REAL,
    PRIMARY KEY (pool_address, timestamp)
);

-- ============================================
-- Social & Sentiment Tracking
-- ============================================

CREATE TABLE IF NOT EXISTS social_metrics (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    platform TEXT NOT NULL,
    followers_count INTEGER,
    followers_change INTEGER,
    posts_count INTEGER,
    engagement_rate REAL,
    sentiment_score REAL,
    mentions_count INTEGER,
    positive_mentions INTEGER,
    negative_mentions INTEGER,
    influence_score REAL,
    PRIMARY KEY (symbol, date, platform)
);

-- Reddit-specific metrics
CREATE TABLE IF NOT EXISTS reddit_metrics (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    subreddit TEXT,
    subscribers INTEGER,
    active_users INTEGER,
    posts_count INTEGER,
    comments_count INTEGER,
    avg_score REAL,
    avg_sentiment REAL,
    top_post_score INTEGER,
    PRIMARY KEY (symbol, date, subreddit)
);

-- ============================================
-- Developer Activity
-- ============================================

CREATE TABLE IF NOT EXISTS github_metrics (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    repo_url TEXT,
    stars INTEGER,
    forks INTEGER,
    watchers INTEGER,
    commits_count INTEGER,
    active_developers INTEGER,
    issues_open INTEGER,
    issues_closed INTEGER,
    pull_requests INTEGER,
    code_changes INTEGER,
    PRIMARY KEY (symbol, date, repo_url)
);

-- ============================================
-- Correlation & Relationships
-- ============================================

CREATE TABLE IF NOT EXISTS coin_correlations (
    symbol1 TEXT NOT NULL,
    symbol2 TEXT NOT NULL,
    period TEXT NOT NULL,
    end_date TEXT NOT NULL,
    correlation REAL,
    cointegration_score REAL,
    lead_lag_days INTEGER,
    PRIMARY KEY (symbol1, symbol2, period, end_date)
);

-- ============================================
-- Data Quality & Maintenance
-- ============================================

CREATE TABLE IF NOT EXISTS data_gaps (
    symbol TEXT NOT NULL,
    data_type TEXT NOT NULL,
    gap_start TEXT NOT NULL,
    gap_end TEXT,
    gap_days INTEGER,
    filled BOOLEAN DEFAULT FALSE,
    fill_method TEXT,
    PRIMARY KEY (symbol, data_type, gap_start)
);

CREATE TABLE IF NOT EXISTS data_anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    data_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    value REAL,
    expected_range_min REAL,
    expected_range_max REAL,
    z_score REAL,
    handled BOOLEAN DEFAULT FALSE,
    handling_method TEXT
);

-- ============================================
-- Optimized Indexes
-- ============================================

-- Price data indexes
CREATE INDEX idx_prices_symbol_date ON prices_template(symbol, timestamp DESC);
CREATE INDEX idx_prices_date ON prices_template(timestamp DESC);
CREATE INDEX idx_prices_source ON prices_template(source);

-- Metrics indexes
CREATE INDEX idx_daily_metrics_symbol ON daily_metrics(symbol);
CREATE INDEX idx_daily_metrics_date ON daily_metrics(date DESC);
CREATE INDEX idx_daily_metrics_returns ON daily_metrics(return_30d DESC);

-- Social indexes
CREATE INDEX idx_social_symbol_date ON social_metrics(symbol, date DESC);
CREATE INDEX idx_social_sentiment ON social_metrics(sentiment_score);

-- On-chain indexes
CREATE INDEX idx_onchain_symbol ON on_chain_metrics(symbol);
CREATE INDEX idx_onchain_flows ON on_chain_metrics(exchange_netflow);

-- ============================================
-- Views for Common Queries
-- ============================================

-- Current market overview
CREATE VIEW IF NOT EXISTS market_overview AS
SELECT 
    cr.symbol,
    cr.name,
    cr.rank,
    dm.close as price,
    dm.volume_usd,
    dm.market_cap,
    dm.return_1d,
    dm.return_7d,
    dm.return_30d,
    dm.volatility,
    sm.sentiment_score
FROM coin_registry cr
LEFT JOIN daily_metrics dm ON cr.symbol = dm.symbol 
    AND dm.date = (SELECT MAX(date) FROM daily_metrics WHERE symbol = cr.symbol)
LEFT JOIN (
    SELECT symbol, AVG(sentiment_score) as sentiment_score
    FROM social_metrics
    WHERE date > date('now', '-7 days')
    GROUP BY symbol
) sm ON cr.symbol = sm.symbol
WHERE cr.is_active = TRUE
ORDER BY cr.rank;

-- Trending coins
CREATE VIEW IF NOT EXISTS trending_coins AS
SELECT 
    dm.symbol,
    cr.name,
    dm.close as current_price,
    dm.return_1d,
    dm.return_7d,
    dm.volume_usd,
    dm.volume_usd / LAG(dm.volume_usd, 7) OVER (PARTITION BY dm.symbol ORDER BY dm.date) as volume_change_7d,
    sm.mentions_count,
    sm.sentiment_score
FROM daily_metrics dm
JOIN coin_registry cr ON dm.symbol = cr.symbol
LEFT JOIN (
    SELECT symbol, 
           SUM(mentions_count) as mentions_count,
           AVG(sentiment_score) as sentiment_score
    FROM social_metrics
    WHERE date > date('now', '-24 hours')
    GROUP BY symbol
) sm ON dm.symbol = sm.symbol
WHERE dm.date = (SELECT MAX(date) FROM daily_metrics)
AND dm.return_7d > 0.1  -- 10% gain
ORDER BY dm.volume_usd DESC
LIMIT 50;

-- ============================================
-- Triggers for Data Maintenance
-- ============================================

-- Auto-update timestamps
CREATE TRIGGER update_coin_timestamp 
AFTER UPDATE ON coin_registry
BEGIN
    UPDATE coin_registry SET updated_date = CURRENT_TIMESTAMP WHERE symbol = NEW.symbol;
END;

-- Track data gaps
CREATE TRIGGER detect_data_gaps
AFTER INSERT ON prices_template
BEGIN
    -- Logic to detect and log gaps would go here
    -- This is a placeholder for the concept
END;
