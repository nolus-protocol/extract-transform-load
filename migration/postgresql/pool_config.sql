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
);

-- Add protocol column if it doesn't exist (for existing deployments)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pool_config' AND column_name = 'protocol'
    ) THEN
        ALTER TABLE "pool_config" ADD COLUMN "protocol" VARCHAR(50);
    END IF;
END $$
