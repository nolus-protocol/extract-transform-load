CREATE TABLE IF NOT EXISTS "MP_Asset_Mapping" (
  "MP_asset_symbol" VARCHAR(20) NOT NULL,
  "MP_asset_symbol_coingecko" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("MP_asset_symbol")
);