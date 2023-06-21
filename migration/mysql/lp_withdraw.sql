CREATE TABLE IF NOT EXISTS `LP_Withdraw` (
  `LP_withdraw_height` BIGINT NOT NULL,
  `LP_withdraw_idx` INT NOT NULL AUTO_INCREMENT,
  `LP_address_id` VARCHAR(44) NOT NULL,
  `LP_timestamp` TIMESTAMP,
  `LP_Pool_id` VARCHAR(64) NOT NULL,
  `LP_amnt_stable` DECIMAL(39, 0) NOT NULL,
  `LP_amnt_asset` DECIMAL(39, 0) NOT NULL,
  `LP_amnt_receipts` DECIMAL(39, 0) NOT NULL,
  `LP_deposit_close` BOOLEAN DEFAULT false NOT NULL,
  PRIMARY KEY (`LP_withdraw_idx`, `LP_withdraw_height`),
  FOREIGN KEY (`LP_Pool_id`) REFERENCES `LP_Pool`(`LP_Pool_id`)
);