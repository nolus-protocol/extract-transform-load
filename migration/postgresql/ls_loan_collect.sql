CREATE TABLE IF NOT EXISTS "LS_Loan_Collect" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(68) NOT NULL,
  "LS_amount" DECIMAL(39, 0) NOT NULL,
  "LS_amount_stable" DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY ("LS_contract_id", "LS_symbol")
);