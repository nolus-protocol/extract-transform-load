CREATE TABLE IF NOT EXISTS `MP_Yield` (
  `MP_yield_symbol` VARCHAR(20) NOT NULL,
  `MP_yield_timestamp` TIMESTAMP NOT NULL,
  `MP_apy_permilles`  INT NOT NULL,
  PRIMARY KEY (`MP_yield_symbol`, `MP_yield_timestamp`)
);