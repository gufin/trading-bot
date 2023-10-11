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
