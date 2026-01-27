-- V013: Backfill pool_config stable_currency columns
-- The stable_currency_symbol and stable_currency_decimals columns (added in V012)
-- are NULL for pools that existed before the dynamic protocol discovery feature.
-- This migration populates them for all existing pools.

-- Long pools: stable currency is the same as the LPN currency
UPDATE pool_config
SET stable_currency_symbol = lpn_symbol,
    stable_currency_decimals = lpn_decimals
WHERE position_type = 'Long'
  AND stable_currency_symbol IS NULL;

-- Short pools: all existing Short pools use USDC_NOBLE as their stable currency
-- (the oracle's stable_currency for Osmosis-based Short protocols)
UPDATE pool_config
SET stable_currency_symbol = 'USDC_NOBLE',
    stable_currency_decimals = 1000000
WHERE position_type = 'Short'
  AND stable_currency_symbol IS NULL;
