-- Restore currency_protocol rows for deprecated protocols.
-- These were incorrectly deleted by remove_deprecated() on startup.
-- Reconstruct from LS_Opening (historical leases) + protocol_registry.

-- Step 1: Insert lease asset currencies for deprecated protocols
-- Join LS_Opening with protocol_registry via lpp_contract to find
-- which tickers appeared in leases for each deprecated protocol.
INSERT INTO "currency_protocol" ("ticker", "protocol", "group", "bank_symbol", "dex_symbol")
SELECT DISTINCT
    ls."LS_asset_symbol",
    pr."protocol_name",
    'lease',
    '',
    ''
FROM "LS_Opening" ls
JOIN "protocol_registry" pr ON ls."LS_loan_pool_id" = pr."lpp_contract"
WHERE pr."is_active" = false
  AND ls."LS_asset_symbol" IN (
      SELECT "ticker" FROM "currency_registry" WHERE "ticker" = ls."LS_asset_symbol"
  )
ON CONFLICT ("ticker", "protocol") DO NOTHING;

-- Step 2: Insert LPN currencies for deprecated protocols
-- Each protocol has an lpn_symbol that must also be in currency_protocol.
INSERT INTO "currency_protocol" ("ticker", "protocol", "group", "bank_symbol", "dex_symbol")
SELECT
    pr."lpn_symbol",
    pr."protocol_name",
    'lpn',
    '',
    ''
FROM "protocol_registry" pr
WHERE pr."is_active" = false
  AND pr."lpn_symbol" IS NOT NULL
  AND EXISTS (SELECT 1 FROM "currency_registry" cr WHERE cr."ticker" = pr."lpn_symbol")
ON CONFLICT ("ticker", "protocol") DO NOTHING;

-- Step 3: Insert NLS (native) for deprecated protocols
-- NLS was available in every protocol as the native currency.
INSERT INTO "currency_protocol" ("ticker", "protocol", "group", "bank_symbol", "dex_symbol")
SELECT
    'NLS',
    pr."protocol_name",
    'native',
    'unls',
    ''
FROM "protocol_registry" pr
WHERE pr."is_active" = false
ON CONFLICT ("ticker", "protocol") DO NOTHING;
