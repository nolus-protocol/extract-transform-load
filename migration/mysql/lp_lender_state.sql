CREATE TABLE IF NOT EXISTS `LP_Lender_State` (
  `LP_Lender_id` VARCHAR(44) NOT NULL,
  `LP_Pool_id` VARCHAR(64) NOT NULL,
  `LP_timestamp` TIMESTAMP NOT NULL,
  `LP_Lender_stable` DECIMAL(39, 0) NOT NULL,
  `LP_Lender_asset` DECIMAL(39, 0) NOT NULL,
  `LP_Lender_receipts` DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY (`LP_Lender_id`, `LP_Pool_id`, `LP_timestamp`),
  FOREIGN KEY (`LP_Pool_id`) REFERENCES `LP_Pool`(`LP_Pool_id`)
);