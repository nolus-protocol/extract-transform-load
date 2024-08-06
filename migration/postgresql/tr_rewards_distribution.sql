CREATE TABLE IF NOT EXISTS "TR_Rewards_Distribution" (
  "TR_Rewards_height" BIGINT NOT NULL,
  "TR_Rewards_idx" SERIAL,
  "TR_Rewards_Pool_id" VARCHAR(64) NOT NULL,
  "TR_Rewards_timestamp" TIMESTAMPTZ NOT NULL,
  "TR_Rewards_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "TR_Rewards_amnt_nls" DECIMAL(39, 0) NOT NULL,
  "Event_Block_Index" INT NOT NULL,
  "Tx_Hash" VARCHAR(64),
  PRIMARY KEY (
    "TR_Rewards_height",
    "TR_Rewards_Pool_id",
    "Event_Block_Index"
  )
);