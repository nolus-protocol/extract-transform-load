CREATE TABLE IF NOT EXISTS "LS_Opening" (
  "LS_contract_id" VARCHAR(64) PRIMARY KEY NOT NULL,
  "LS_address_id" VARCHAR(44) NOT NULL,
  "LS_asset_symbol" VARCHAR(20) NOT NULL,
  "LS_interest" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_loan_pool_id" VARCHAR(64) NOT NULL,
  "LS_loan_amnt" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LS_loan_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_loan_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LS_cltr_symbol" VARCHAR(20) NOT NULL,
  "LS_cltr_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_cltr_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LS_native_amnt_stable" DECIMAL(39, 0),
  "LS_native_amnt_nolus" DECIMAL(39, 0),
  "Tx_Hash" VARCHAR(64)
);