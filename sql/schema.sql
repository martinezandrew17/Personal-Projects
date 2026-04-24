-- =============================================================================
-- real-time-financial-dashboard — database schema
-- Run once to initialise:
-- psql -U finuser -d findb -f sql/schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Watchlist: defines which tickers the pipeline tracks
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS watchlist (
    ticker  VARCHAR(12) PRIMARY KEY, 
    name    VARCHAR(100) NOT NULL
    asset_type  VARCHAR(10) NOT NULL CHECK (asset_type IN ('stock', 'crypto')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the default tickers so the pipeline has something to fetch immediately
INSERT INTO watchlist (tcker, name, asset_type) VALUES
    ('AAPL',    'Apple Inc.',       'stock'),
    ('TSLA',    'Tesla Inc.',       'stock'),
    ('MSFT',    'Microsoft Corp.',  'stock'),
    ('GOOGL',   'Alphabet Inc.',    'stock'),
    ('BTC-USD', 'Bitcoin',          'crypto'),
    ('ETH-USD', 'Ethereum',         'crypto')
ON CONFLICT (ticker) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Prices: time-series OHLCV data — one row per ticker per minute
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    id  BIGSERIAL   PRIMARY KEY,
    ticker  VARCHAR(12) NOT NULL REFERENCES watchlist(ticker),
    timestamp   TIMESTAMPTZ NOT NULL,
    open    NUMERIC(18,6),
    high    NUMERIC(18,6),
    low     NUMERIC(18,6),
    close   NUMERIC(18,6),
    volume  BIGINT, 

    -- prevents duplicate rows if the fetcher runs twice in the same minute
    UNIQUE (ticker, timestamp)
);

-- This index makes the dashboard's most common query fast:
-- "SELECT ... FROM prices WHERE ticker = 'AAPL' ORDER BY timestamp DESC LIMIT 200"
-- Without it, Postgres scans the entire table every time.
CREATE INDEX IF NOT EXISTS idx_prices_ticker_time
    ON prices (ticker, timestamp DESC);

-- ---------------------------------------------------------------------------
-- View: latest price per ticker (used by the dashboard overview panel)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW latest_prices AS 
SELECT DISTINCT ON (ticker)
    ticker, 
    timestamp,
    open,
    high,
    low,
    close,
    volume,
FROM prices
ORDER BY ticker, timestamp DESC;

CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    asset_type VARCHAR(20) NOT NULL,
    name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id BIGSERIAL PRIMARY KEY,
    asset_id INT NOT NULL REFERENCES assets(id),
    price NUMERIC(18,8) NOT NULL,
    open NUMERIC(18,8),
    high NUMERIC(18,8),
    low NUMERIC(18,8),
    volume NUMERIC(20,2),
    source VARCHAR(50) NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);