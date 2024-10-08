CREATE TABLE IF NOT EXISTS "LS_Loan_Closing" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_pnl" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "Type" VARCHAR(20) NOT NULL,
  "Active" BOOLEAN DEFAULT false NOT NULL,
  "Block"  BIGINT NOT NULL,
  PRIMARY KEY ("LS_contract_id")
);