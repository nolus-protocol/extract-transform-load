CREATE TABLE IF NOT EXISTS "LS_Slippage_Anomaly" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_address_id" VARCHAR(64) NOT NULL,
  "LS_asset_symbol" VARCHAR(20) NOT NULL,
  "LS_max_slipagge" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("Tx_Hash", "LS_contract_id", "LS_timestamp")
);