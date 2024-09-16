CREATE TABLE IF NOT EXISTS "LS_Loan_Closing" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "Type" VARCHAR(20) NOT NULL,
  PRIMARY KEY ("LS_contract_id")
);