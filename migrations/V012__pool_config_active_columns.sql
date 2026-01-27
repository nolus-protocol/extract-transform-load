-- Add active/deprecated tracking and stable currency fields to pool_config
-- These columns are required by the dynamic protocol discovery system
-- introduced in the protocol/currency registry feature.

ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "is_active" BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "first_seen_at" TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "deprecated_at" TIMESTAMPTZ;
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "stable_currency_symbol" VARCHAR(50);
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "stable_currency_decimals" BIGINT;
