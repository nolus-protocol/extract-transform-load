-- V007: Currency and Protocol Registry Tables
-- These tables store all currencies and protocols ever seen, including deprecated ones.
-- This enables dynamic configuration while preserving historical data integrity.

-- ============================================================================
-- CURRENCY REGISTRY
-- Stores all currencies ever encountered, with active/deprecated status
-- ============================================================================

CREATE TABLE IF NOT EXISTS "currency_registry" (
    "ticker" VARCHAR(20) PRIMARY KEY,
    "bank_symbol" VARCHAR(256),                -- IBC denom (nullable for historical seeds)
    "decimal_digits" SMALLINT NOT NULL DEFAULT 6,
    "group" VARCHAR(20),                       -- lpn, lease, native (nullable for historical)
    "is_active" BOOLEAN NOT NULL DEFAULT false,
    "first_seen_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "deprecated_at" TIMESTAMPTZ,
    "last_seen_protocol" VARCHAR(100)
);

-- ============================================================================
-- PROTOCOL REGISTRY
-- Stores all protocols ever encountered, with active/deprecated status
-- ============================================================================

CREATE TABLE IF NOT EXISTS "protocol_registry" (
    "protocol_name" VARCHAR(100) PRIMARY KEY,
    "network" VARCHAR(50),
    "dex" VARCHAR(100),
    "leaser_contract" VARCHAR(100),
    "lpp_contract" VARCHAR(100),               -- This is the pool_id
    "oracle_contract" VARCHAR(100),
    "profit_contract" VARCHAR(100),
    "reserve_contract" VARCHAR(100),
    "lpn_symbol" VARCHAR(20) NOT NULL,
    "position_type" VARCHAR(10) NOT NULL DEFAULT 'Long',
    "is_active" BOOLEAN NOT NULL DEFAULT false,
    "first_seen_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "deprecated_at" TIMESTAMPTZ
);

-- ============================================================================
-- SEED CURRENCY REGISTRY FROM EXISTING DATA
-- Captures all currencies that exist in historical records
-- ============================================================================

-- Seed from MP_Asset: Extract unique currencies from historical price data
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active", "first_seen_at")
SELECT DISTINCT 
    "MP_asset_symbol",
    6,  -- Default decimals, will be updated by app startup for active currencies
    false,  -- Mark as inactive initially, app will activate those found in oracle
    COALESCE(MIN("MP_asset_timestamp"), NOW())
FROM "MP_Asset"
WHERE "MP_asset_symbol" IS NOT NULL AND "MP_asset_symbol" != ''
GROUP BY "MP_asset_symbol"
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Opening asset symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_asset_symbol", 6, false
FROM "LS_Opening"
WHERE "LS_asset_symbol" IS NOT NULL AND "LS_asset_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Opening collateral symbols  
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_cltr_symbol", 6, false
FROM "LS_Opening"
WHERE "LS_cltr_symbol" IS NOT NULL AND "LS_cltr_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Opening LPN symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_lpn_symbol", 6, false
FROM "LS_Opening"
WHERE "LS_lpn_symbol" IS NOT NULL AND "LS_lpn_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Repayment payment symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_payment_symbol", 6, false
FROM "LS_Repayment"
WHERE "LS_payment_symbol" IS NOT NULL AND "LS_payment_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Liquidation symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_amnt_symbol", 6, false
FROM "LS_Liquidation"
WHERE "LS_amnt_symbol" IS NOT NULL AND "LS_amnt_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_payment_symbol", 6, false
FROM "LS_Liquidation"
WHERE "LS_payment_symbol" IS NOT NULL AND "LS_payment_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from LS_Close_Position symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_amnt_symbol", 6, false
FROM "LS_Close_Position"
WHERE "LS_amnt_symbol" IS NOT NULL AND "LS_amnt_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "LS_payment_symbol", 6, false
FROM "LS_Close_Position"
WHERE "LS_payment_symbol" IS NOT NULL AND "LS_payment_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- Seed from pool_config LPN symbols
INSERT INTO "currency_registry" ("ticker", "decimal_digits", "is_active")
SELECT DISTINCT "lpn_symbol", 6, false
FROM "pool_config"
WHERE "lpn_symbol" IS NOT NULL AND "lpn_symbol" != ''
ON CONFLICT ("ticker") DO NOTHING;

-- ============================================================================
-- UPDATE KNOWN DECIMAL VALUES
-- Override defaults for currencies with non-standard decimals
-- ============================================================================

UPDATE "currency_registry" SET "decimal_digits" = 8 WHERE "ticker" IN ('ALL_BTC', 'WBTC', 'CRO');
UPDATE "currency_registry" SET "decimal_digits" = 9 WHERE "ticker" = 'ALL_SOL';
UPDATE "currency_registry" SET "decimal_digits" = 12 WHERE "ticker" = 'PICA';
UPDATE "currency_registry" SET "decimal_digits" = 18 WHERE "ticker" IN ('WETH', 'EVMOS', 'INJ', 'DYM', 'CUDOS', 'ALL_ETH');

-- ============================================================================
-- SEED PROTOCOL REGISTRY FROM EXISTING DATA
-- Captures all protocols that exist in historical records
-- ============================================================================

-- Seed from pool_config (existing protocol data with full info)
INSERT INTO "protocol_registry" (
    "protocol_name", 
    "lpp_contract", 
    "lpn_symbol", 
    "position_type", 
    "is_active"
)
SELECT 
    "protocol",
    "pool_id",
    "lpn_symbol",
    "position_type",
    false  -- Mark inactive, app will activate those found in admin contract
FROM "pool_config"
WHERE "protocol" IS NOT NULL AND "protocol" != ''
ON CONFLICT ("protocol_name") DO UPDATE SET
    "lpp_contract" = COALESCE(EXCLUDED."lpp_contract", "protocol_registry"."lpp_contract"),
    "lpn_symbol" = COALESCE(EXCLUDED."lpn_symbol", "protocol_registry"."lpn_symbol"),
    "position_type" = COALESCE(EXCLUDED."position_type", "protocol_registry"."position_type");

-- Seed from MP_Asset protocols (captures any protocol that ever had prices)
INSERT INTO "protocol_registry" ("protocol_name", "lpn_symbol", "position_type", "is_active")
SELECT DISTINCT 
    "Protocol",
    'UNKNOWN',  -- Will be updated by app startup for active protocols
    'Long',     -- Default, will be corrected by app startup
    false
FROM "MP_Asset"
WHERE "Protocol" IS NOT NULL AND "Protocol" != ''
ON CONFLICT ("protocol_name") DO NOTHING;

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_currency_registry_active ON "currency_registry" ("is_active");
CREATE INDEX IF NOT EXISTS idx_currency_registry_ticker ON "currency_registry" ("ticker");

CREATE INDEX IF NOT EXISTS idx_protocol_registry_active ON "protocol_registry" ("is_active");
CREATE INDEX IF NOT EXISTS idx_protocol_registry_lpp ON "protocol_registry" ("lpp_contract");
CREATE INDEX IF NOT EXISTS idx_protocol_registry_name ON "protocol_registry" ("protocol_name");
