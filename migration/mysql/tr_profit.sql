CREATE TABLE IF NOT EXISTS `TR_Profit` (
  `TR_Profit_height` BIGINT NOT NULL,
  `TR_Profit_idx` INT NOT NULL AUTO_INCREMENT,
  `TR_Profit_timestamp` TIMESTAMP NOT NULL,
  `TR_Profit_amnt_stable` DECIMAL(39, 0) NOT NULL,
  `TR_Profit_amnt_nls` DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY (
    `TR_Profit_idx`,
    `TR_Profit_height`
  )
);