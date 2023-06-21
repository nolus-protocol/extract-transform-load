CREATE TABLE IF NOT EXISTS `MP_Asset_State` (
  `MP_asset_symbol` VARCHAR(20) NOT NULL,
  `MP_timestamp` TIMESTAMP NOT NULL,
  `MP_price_open` DECIMAL(39, 18),
  `MP_price_high` DECIMAL(39, 18),
  `MP_price_low` DECIMAL(39, 18),
  `MP_price_close` DECIMAL(39, 18),
  `MP_volume` DECIMAL(39, 0),
  `MP_marketcap` DECIMAL(39, 0),
  PRIMARY KEY (`MP_asset_symbol`, `MP_timestamp`)
);