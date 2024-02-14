CREATE TABLE IF NOT EXISTS "MP_Asset" (
  "MP_asset_symbol" VARCHAR(20) NOT NULL,
  "MP_asset_timestamp" TIMESTAMPTZ NOT NULL,
  "MP_price_in_stable" DECIMAL(39, 18),
  "Protocol" VARCHAR(256) NOT NULL,
  PRIMARY KEY ("MP_asset_symbol", "MP_asset_timestamp", "Protocol")
);