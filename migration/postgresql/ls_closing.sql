CREATE TABLE IF NOT EXISTS "LS_Closing" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) PRIMARY KEY NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL
);