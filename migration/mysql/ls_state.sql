CREATE TABLE IF NOT EXISTS `LS_State` (
  `LS_contract_id` VARCHAR(64) NOT NULL,
  `LS_timestamp` TIMESTAMP NOT NULL,
  `LS_amnt_stable` DECIMAL(39, 0) NOT NULL,
  `LS_prev_margin_stable` DECIMAL(39, 0),
  `LS_prev_interest_stable` DECIMAL(39, 0),
  `LS_current_margin_stable` DECIMAL(39, 0),
  `LS_current_interest_stable` DECIMAL(39, 0),
  `LS_principal_stable` DECIMAL(39, 0),
  PRIMARY KEY (`LS_contract_id`, `LS_timestamp`)
);