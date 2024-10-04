CREATE TABLE IF NOT EXISTS "TR_Profit" (
  "TR_Profit_height" BIGINT NOT NULL,
  "TR_Profit_idx" SERIAL,
  "TR_Profit_timestamp" TIMESTAMPTZ NOT NULL,
  "TR_Profit_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "TR_Profit_amnt_nls" DECIMAL(39, 0) NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY (
    "TR_Profit_height",
    "TR_Profit_idx"
  )
);