-- V003: Pre-computed columns for LS_Opening
-- Eliminates runtime CTEs and LATERAL JOINs
-- These columns are populated at write time for new records and backfilled for existing ones

ALTER TABLE "LS_Opening" ADD COLUMN IF NOT EXISTS "LS_position_type" VARCHAR(10);
ALTER TABLE "LS_Opening" ADD COLUMN IF NOT EXISTS "LS_lpn_symbol" VARCHAR(20);
ALTER TABLE "LS_Opening" ADD COLUMN IF NOT EXISTS "LS_lpn_decimals" BIGINT;
ALTER TABLE "LS_Opening" ADD COLUMN IF NOT EXISTS "LS_opening_price" DECIMAL(39, 18);
ALTER TABLE "LS_Opening" ADD COLUMN IF NOT EXISTS "LS_liquidation_price_at_open" DECIMAL(39, 18);

-- Index on position_type for filtering Long/Short positions efficiently
CREATE INDEX IF NOT EXISTS idx_ls_opening_position_type ON "LS_Opening" ("LS_position_type");
