CREATE TABLE IF NOT EXISTS `TR_Rewards_Distribution` (
  `TR_Rewards_height` BIGINT NOT NULL,
  `TR_Rewards_idx` INT NOT NULL AUTO_INCREMENT,
  `TR_Rewards_Pool_id` VARCHAR(64) NOT NULL,
  `TR_Rewards_timestamp` TIMESTAMP NOT NULL,
  `TR_Rewards_amnt_stable` DECIMAL(39, 0) NOT NULL,
  `TR_Rewards_amnt_nls` DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY (
    `TR_Rewards_idx`,
    `TR_Rewards_height`,
    `TR_Rewards_Pool_id`
  )
);