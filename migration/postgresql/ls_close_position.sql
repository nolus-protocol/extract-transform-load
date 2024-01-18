CREATE TABLE IF NOT EXISTS "LS_Close_Position" (
  "LS_position_height" BIGINT NOT NULL,
  "LS_position_idx" SERIAL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(20) NOT NULL,
  "LS_change" DECIMAL(39, 0) NOT NULL,
  "LS_amount_amount" DECIMAL(39, 0) NOT NULL,
  "LS_amount_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_loan_close" BOOLEAN NOT NULL DEFAULT false,
  "LS_prev_margin_stable" DECIMAL(39, 0),
  "LS_prev_interest_stable" DECIMAL(39, 0),
  "LS_current_margin_stable" DECIMAL(39, 0),
  "LS_current_interest_stable" DECIMAL(39, 0),
  "LS_principal_stable" DECIMAL(39, 0),
  PRIMARY KEY ("LS_position_height", "LS_position_idx")
);