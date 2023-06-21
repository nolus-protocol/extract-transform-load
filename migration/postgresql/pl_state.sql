CREATE TABLE IF NOT EXISTS "PL_State" (
  "PL_timestamp" TIMESTAMPTZ PRIMARY KEY NOT NULL,
  "PL_pools_TVL_stable" DECIMAL(42, 0) NOT NULL,
  "PL_pools_borrowed_stable" DECIMAL(42, 0) NOT NULL,
  "PL_pools_yield_stable" DECIMAL(42, 0) NOT NULL,
  "PL_LS_count_open" BIGINT NOT NULL,
  "PL_LS_count_closed" BIGINT NOT NULL,
  "PL_LS_count_opened" BIGINT NOT NULL,
  "PL_IN_LS_cltr_amnt_opened_stable" DECIMAL(42, 0) NOT NULL,
  "PL_LP_count_open" BIGINT NOT NULL,
  "PL_LP_count_closed" BIGINT NOT NULL,
  "PL_LP_count_opened" BIGINT NOT NULL,
  "PL_OUT_LS_loan_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_prev_margin_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_prev_interest_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_current_margin_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_current_interest_stable" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LS_rep_principal_stable" DECIMAL(42, 0) NOT NULL,
  "PL_OUT_LS_cltr_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_OUT_LS_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_native_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_native_amnt_nolus" DECIMAL(42, 0) NOT NULL,
  "PL_IN_LP_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_OUT_LP_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_TR_profit_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_TR_profit_amnt_nls" DECIMAL(42, 0) NOT NULL,
  "PL_TR_tax_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "PL_TR_tax_amnt_nls" DECIMAL(39, 0) NOT NULL,
  "PL_OUT_TR_rewards_amnt_stable" DECIMAL(42, 0) NOT NULL,
  "PL_OUT_TR_rewards_amnt_nls" DECIMAL(42, 0) NOT NULL
);