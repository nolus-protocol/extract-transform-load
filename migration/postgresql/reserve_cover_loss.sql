CREATE TABLE IF NOT EXISTS "Reserve_Cover_Loss" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "Event_Block_Index" INT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
    "LS_contract_id",
    "Event_Block_Index",
    "Tx_Hash"
  )
);