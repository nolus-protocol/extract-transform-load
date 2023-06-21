CREATE TABLE IF NOT EXISTS `LS_Liquidation` (
  `LS_liquidation_height` BIGINT NOT NULL,
  `LS_liquidation_idx` INT NOT NULL AUTO_INCREMENT,
  `LS_contract_id` VARCHAR(64) NOT NULL,
  `LS_symbol` VARCHAR(20) NOT NULL,
  `LS_amnt_stable` DECIMAL(39, 0) NOT NULL,
  `LS_timestamp` TIMESTAMP NOT NULL,
  `LS_transaction_type` VARCHAR(1) NOT NULL,
  `LS_prev_margin_stable` DECIMAL(39, 0),
  `LS_prev_interest_stable` DECIMAL(39, 0),
  `LS_current_margin_stable` DECIMAL(39, 0),
  `LS_current_interest_stable` DECIMAL(39, 0),
  `LS_principal_stable` DECIMAL(39, 0),
  PRIMARY KEY (`LS_liquidation_idx`, `LS_liquidation_height`)
);