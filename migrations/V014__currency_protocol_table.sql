-- Migration: Create currency_protocol junction table
-- Replaces the single last_seen_protocol + group columns in currency_registry
-- with a proper many-to-many relationship between currencies and protocols.

-- 1. Create junction table
CREATE TABLE IF NOT EXISTS "currency_protocol" (
    "ticker" VARCHAR(20) NOT NULL REFERENCES "currency_registry"("ticker"),
    "protocol" VARCHAR(100) NOT NULL,
    "group" VARCHAR(20),
    PRIMARY KEY ("ticker", "protocol")
);

CREATE INDEX IF NOT EXISTS idx_currency_protocol_ticker ON "currency_protocol" ("ticker");
CREATE INDEX IF NOT EXISTS idx_currency_protocol_protocol ON "currency_protocol" ("protocol");

-- 2. Seed from existing currency_registry data
INSERT INTO "currency_protocol" ("ticker", "protocol", "group")
SELECT "ticker", "last_seen_protocol", "group"
FROM "currency_registry"
WHERE "last_seen_protocol" IS NOT NULL
ON CONFLICT DO NOTHING;

-- 3. Drop old columns from currency_registry
ALTER TABLE "currency_registry" DROP COLUMN IF EXISTS "last_seen_protocol";
ALTER TABLE "currency_registry" DROP COLUMN IF EXISTS "group";
