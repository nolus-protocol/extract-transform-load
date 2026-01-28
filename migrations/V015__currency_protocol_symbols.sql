-- Add bank_symbol and dex_symbol to currency_protocol (per-protocol denoms)
ALTER TABLE "currency_protocol" ADD COLUMN IF NOT EXISTS "bank_symbol" VARCHAR(256);
ALTER TABLE "currency_protocol" ADD COLUMN IF NOT EXISTS "dex_symbol" VARCHAR(256);

-- Backfill bank_symbol from currency_registry into currency_protocol
UPDATE "currency_protocol" cp
SET "bank_symbol" = cr."bank_symbol"
FROM "currency_registry" cr
WHERE cp."ticker" = cr."ticker"
  AND cr."bank_symbol" IS NOT NULL;

-- Drop bank_symbol from currency_registry (now lives per-protocol)
ALTER TABLE "currency_registry" DROP COLUMN IF EXISTS "bank_symbol";
