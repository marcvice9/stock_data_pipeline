CREATE SCHEMA IF NOT EXISTS stage;

CREATE TABLE IF NOT EXISTS stage.raw_stock (
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'), 
    date DATE,
    ticker TEXT,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    close_price REAL,
    volume BIGINT
);