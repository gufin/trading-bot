CREATE TABLE IF NOT EXISTS Users(
    user_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(32),
    lang VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS strategy (
    strategy_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS timeframes (
    timeframe_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS user_strategies (
    user_strategies_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES Users(user_id),
    strategy_id BIGINT REFERENCES strategy(strategy_id),
    timeframe_id BIGINT REFERENCES timeframes(timeframe_id)
);

CREATE TABLE IF NOT EXISTS tickers (
    ticker_id BIGSERIAL PRIMARY KEY,
    figi VARCHAR(64),
    classCode VARCHAR(64),
    currency  VARCHAR(64),
    name VARCHAR(64) NOT NULL,
    disable BOOLEAN DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS user_tickers (
    user_tickers_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES Users(user_id),
    ticker_id BIGINT REFERENCES tickers(ticker_id)
);

CREATE TABLE IF NOT EXISTS candles (
    candl_id BIGSERIAL PRIMARY KEY,
    ticker_id BIGINT REFERENCES tickers(ticker_id),
    interval VARCHAR(64),
    timestamp_column TIMESTAMP,
    open NUMERIC(10, 3),
    high NUMERIC(10, 3),
    low NUMERIC(10, 3),
    close NUMERIC(10, 3)
);

CREATE TABLE IF NOT EXISTS ema_to_calc (
    ema_to_calc_id BIGSERIAL PRIMARY KEY,
    interval VARCHAR(64),
    span INT
);

CREATE TABLE IF NOT EXISTS ema (
    ema_id BIGSERIAL PRIMARY KEY,
    ticker_id BIGINT REFERENCES tickers(ticker_id),
    interval VARCHAR(64),
    span INT,
    timestamp_column TIMESTAMP,
    ema NUMERIC(10, 3),
    atr NUMERIC(10, 3),
    UNIQUE (ticker_id, interval, span, timestamp_column)
);

CREATE TABLE IF NOT EXISTS ema_cross (
    ema_cross_id BIGSERIAL PRIMARY KEY,
    ticker_id BIGINT REFERENCES tickers(ticker_id),
    interval VARCHAR(64),
    span INT,
    timestamp_column TIMESTAMP,
    UNIQUE (ticker_id, interval, span, timestamp_column)
);

-- ALTER TABLE tickers
-- ADD CONSTRAINT unique_ticker_name UNIQUE (name);
--
-- ALTER TABLE user_strategies
-- ADD CONSTRAINT unique_user_strategy_timeframe UNIQUE (user_id, strategy_id, timeframe_id);

ALTER TABLE candles
ADD CONSTRAINT unique_candle_combination UNIQUE (ticker_id, interval, timestamp_column);