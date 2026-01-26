-- Backfill liquidation prices for existing records
-- This updates LS_liquidation_price from MP_Asset prices at the time of each liquidation

UPDATE "LS_Liquidation" liq
SET "LS_liquidation_price" = (
    SELECT "MP_price_in_stable"
    FROM "MP_Asset" mp
    WHERE mp."MP_asset_symbol" = liq."LS_amnt_symbol"
      AND mp."MP_asset_timestamp" <= liq."LS_timestamp"
    ORDER BY mp."MP_asset_timestamp" DESC
    LIMIT 1
)
WHERE liq."LS_liquidation_price" IS NULL;
