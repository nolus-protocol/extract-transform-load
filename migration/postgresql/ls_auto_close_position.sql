CREATE TABLE IF NOT EXISTS "LS_Auto_Close_Position" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_Close_Strategy" VARCHAR(64) NOT NULL,
  "LS_Close_Strategy_Ltv" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("Tx_Hash", "LS_contract_id", "LS_timestamp")
);