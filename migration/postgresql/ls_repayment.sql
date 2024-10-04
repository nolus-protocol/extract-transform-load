CREATE TABLE IF NOT EXISTS "LS_Repayment" (
  "LS_repayment_height" BIGINT NOT NULL,
  "LS_repayment_idx" SERIAL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_payment_symbol" VARCHAR(20) NOT NULL,
  "LS_payment_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_payment_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_loan_close" BOOLEAN NOT NULL DEFAULT false,
  "LS_prev_margin_stable" DECIMAL(39, 0),
  "LS_prev_interest_stable" DECIMAL(39, 0),
  "LS_current_margin_stable" DECIMAL(39, 0),
  "LS_current_interest_stable" DECIMAL(39, 0),
  "LS_principal_stable" DECIMAL(39, 0),
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("LS_repayment_height", "LS_repayment_idx")
);