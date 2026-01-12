-- Pool configuration reference table
-- Stores position type, LPN symbol, decimals, and protocol name for each loan pool
-- Eliminates hardcoded CTEs in queries

CREATE TABLE IF NOT EXISTS "pool_config" (
    "pool_id" VARCHAR(128) PRIMARY KEY,
    "position_type" VARCHAR(10) NOT NULL,
    "lpn_symbol" VARCHAR(20) NOT NULL,
    "lpn_decimals" BIGINT NOT NULL,
    "label" VARCHAR(50) NOT NULL,
    "protocol" VARCHAR(50)
)
