-- V001: Initial schema
-- Core tables for ETL service

-- LP_Pool must be created first (referenced by foreign keys)
CREATE TABLE IF NOT EXISTS "LP_Pool" (
  "LP_Pool_id" VARCHAR(64) PRIMARY KEY NOT NULL,
  "LP_symbol" VARCHAR(20) NOT NULL,
  "LP_status" BOOLEAN NOT NULL
);

-- Action history for tracking aggregation tasks
CREATE TABLE IF NOT EXISTS "action_history" (
  "action_type" VARCHAR(1) NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("action_type", "created_at")
);

-- Block tracking for sync progress
CREATE TABLE IF NOT EXISTS "block" (
  "id" BIGINT NOT NULL,
  PRIMARY KEY ("id")
);

-- Market prices
CREATE TABLE IF NOT EXISTS "MP_Asset" (
  "MP_asset_symbol" VARCHAR(20) NOT NULL,
  "MP_asset_timestamp" TIMESTAMPTZ NOT NULL,
  "MP_price_in_stable" DECIMAL(39, 18) NOT NULL,
  "Protocol" VARCHAR(256) NOT NULL,
  PRIMARY KEY ("MP_asset_symbol", "MP_asset_timestamp", "Protocol")
);

CREATE TABLE IF NOT EXISTS "MP_Yield" (
  "MP_yield_symbol" VARCHAR(20) NOT NULL,
  "MP_yield_timestamp" TIMESTAMPTZ NOT NULL,
  "MP_apy_permilles" INT NOT NULL,
  PRIMARY KEY ("MP_yield_symbol", "MP_yield_timestamp")
);

-- LP tables
CREATE TABLE IF NOT EXISTS "LP_Deposit" (
  "LP_deposit_height" BIGINT NOT NULL,
  "LP_deposit_idx" SERIAL,
  "LP_address_id" VARCHAR(64) NOT NULL,
  "LP_timestamp" TIMESTAMPTZ NOT NULL,
  "LP_Pool_id" VARCHAR(64) NOT NULL,
  "LP_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LP_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LP_amnt_receipts" DECIMAL(39, 0) NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("LP_deposit_height", "LP_deposit_idx"),
  FOREIGN KEY ("LP_Pool_id") REFERENCES "LP_Pool"("LP_Pool_id")
);

CREATE TABLE IF NOT EXISTS "LP_Lender_State" (
  "LP_Lender_id" VARCHAR(64) NOT NULL,
  "LP_Pool_id" VARCHAR(64) NOT NULL,
  "LP_timestamp" TIMESTAMPTZ NOT NULL,
  "LP_Lender_stable" DECIMAL(39, 0) NOT NULL,
  "LP_Lender_asset" DECIMAL(39, 0) NOT NULL,
  "LP_Lender_receipts" DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY ("LP_Lender_id", "LP_Pool_id", "LP_timestamp"),
  FOREIGN KEY ("LP_Pool_id") REFERENCES "LP_Pool"("LP_Pool_id")
);

CREATE TABLE IF NOT EXISTS "LP_Pool_State" (
  "LP_Pool_id" VARCHAR(64) NOT NULL,
  "LP_Pool_timestamp" TIMESTAMPTZ NOT NULL,
  "LP_Pool_total_value_locked_stable" DECIMAL(39, 0) NOT NULL,
  "LP_Pool_total_value_locked_asset" DECIMAL(39, 0) NOT NULL,
  "LP_Pool_total_issued_receipts" DECIMAL(39, 0) NOT NULL,
  "LP_Pool_total_borrowed_stable" DECIMAL(39, 0) NOT NULL,
  "LP_Pool_total_borrowed_asset" DECIMAL(39, 0) NOT NULL,
  "LP_Pool_total_yield_stable" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LP_Pool_total_yield_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LP_Pool_min_utilization_threshold" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  PRIMARY KEY ("LP_Pool_id", "LP_Pool_timestamp"),
  FOREIGN KEY ("LP_Pool_id") REFERENCES "LP_Pool"("LP_Pool_id")
);

CREATE TABLE IF NOT EXISTS "LP_Withdraw" (
  "LP_withdraw_height" BIGINT NOT NULL,
  "LP_withdraw_idx" SERIAL,
  "LP_address_id" VARCHAR(64) NOT NULL,
  "LP_timestamp" TIMESTAMPTZ,
  "LP_Pool_id" VARCHAR(64) NOT NULL,
  "LP_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LP_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LP_amnt_receipts" DECIMAL(39, 0) NOT NULL,
  "LP_deposit_close" BOOLEAN DEFAULT false NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("LP_withdraw_height", "LP_withdraw_idx"),
  FOREIGN KEY ("LP_Pool_id") REFERENCES "LP_Pool"("LP_Pool_id")
);

-- LS (Lease) tables
CREATE TABLE IF NOT EXISTS "LS_Opening" (
  "LS_contract_id" VARCHAR(64) PRIMARY KEY NOT NULL,
  "LS_address_id" VARCHAR(64) NOT NULL,
  "LS_asset_symbol" VARCHAR(20) NOT NULL,
  "LS_interest" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_loan_pool_id" VARCHAR(64) NOT NULL,
  "LS_loan_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_loan_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_loan_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LS_cltr_symbol" VARCHAR(20) NOT NULL,
  "LS_cltr_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_cltr_amnt_asset" DECIMAL(39, 0) NOT NULL,
  "LS_native_amnt_stable" DECIMAL(39, 0),
  "LS_native_amnt_nolus" DECIMAL(39, 0),
  "LS_lpn_loan_amnt" DECIMAL(39, 0) NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS "LS_State" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_prev_margin_stable" DECIMAL(39, 0),
  "LS_prev_interest_stable" DECIMAL(39, 0),
  "LS_current_margin_stable" DECIMAL(39, 0),
  "LS_current_interest_stable" DECIMAL(39, 0),
  "LS_principal_stable" DECIMAL(39, 0),
  "LS_lpn_loan_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_prev_margin_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LS_prev_interest_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LS_current_margin_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LS_current_interest_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  "LS_principal_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0,
  PRIMARY KEY ("LS_contract_id", "LS_timestamp")
);

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

CREATE TABLE IF NOT EXISTS "LS_Liquidation" (
  "LS_liquidation_height" BIGINT NOT NULL,
  "LS_liquidation_idx" SERIAL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_amnt_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_payment_symbol" VARCHAR(20) NOT NULL,
  "LS_payment_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_payment_amnt_stable" DECIMAL(39, 0),
  "LS_loan_close" BOOLEAN NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_transaction_type" VARCHAR(64) NOT NULL,
  "LS_prev_margin_stable" DECIMAL(39, 0),
  "LS_prev_interest_stable" DECIMAL(39, 0),
  "LS_current_margin_stable" DECIMAL(39, 0),
  "LS_current_interest_stable" DECIMAL(39, 0),
  "LS_principal_stable" DECIMAL(39, 0),
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("LS_liquidation_height", "LS_liquidation_idx")
);

CREATE TABLE IF NOT EXISTS "LS_Close_Position" (
  "LS_position_height" BIGINT NOT NULL,
  "LS_position_idx" SERIAL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_change" DECIMAL(39, 0) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_amnt_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "LS_loan_close" BOOLEAN NOT NULL DEFAULT false,
  "LS_prev_margin_stable" DECIMAL(39, 0),
  "LS_prev_interest_stable" DECIMAL(39, 0),
  "LS_current_margin_stable" DECIMAL(39, 0),
  "LS_current_interest_stable" DECIMAL(39, 0),
  "LS_principal_stable" DECIMAL(39, 0),
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_payment_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_payment_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_payment_symbol" VARCHAR(20) NOT NULL,
  PRIMARY KEY ("LS_position_height", "LS_position_idx")
);

CREATE TABLE IF NOT EXISTS "LS_Closing" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) PRIMARY KEY NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS "LS_Auto_Close_Position" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_Close_Strategy" VARCHAR(64) NOT NULL,
  "LS_Close_Strategy_Ltv" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("Tx_Hash", "LS_contract_id", "LS_timestamp")
);

CREATE TABLE IF NOT EXISTS "LS_Liquidation_Warning" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_address_id" VARCHAR(64) NOT NULL,
  "LS_asset_symbol" VARCHAR(20) NOT NULL,
  "LS_level" SMALLINT,
  "LS_ltv" SMALLINT,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("Tx_Hash", "LS_contract_id", "LS_timestamp")
);

CREATE TABLE IF NOT EXISTS "LS_Loan_Closing" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "LS_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "LS_pnl" DECIMAL(39, 0) NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  "Type" VARCHAR(20) NOT NULL,
  "Active" BOOLEAN DEFAULT false NOT NULL,
  "Block" BIGINT NOT NULL,
  PRIMARY KEY ("LS_contract_id")
);

CREATE TABLE IF NOT EXISTS "LS_Loan_Collect" (
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(68) NOT NULL,
  "LS_amount" DECIMAL(39, 0) NOT NULL,
  "LS_amount_stable" DECIMAL(39, 0) NOT NULL,
  PRIMARY KEY ("LS_contract_id", "LS_symbol")
);

CREATE TABLE IF NOT EXISTS "LS_Slippage_Anomaly" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_address_id" VARCHAR(64) NOT NULL,
  "LS_asset_symbol" VARCHAR(20) NOT NULL,
  "LS_max_slipagge" SMALLINT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("Tx_Hash", "LS_contract_id", "LS_timestamp")
);

-- Treasury tables
CREATE TABLE IF NOT EXISTS "TR_Profit" (
  "TR_Profit_height" BIGINT NOT NULL,
  "TR_Profit_idx" SERIAL,
  "TR_Profit_timestamp" TIMESTAMPTZ NOT NULL,
  "TR_Profit_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "TR_Profit_amnt_nls" DECIMAL(39, 0) NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("TR_Profit_height", "TR_Profit_idx")
);

CREATE TABLE IF NOT EXISTS "TR_Rewards_Distribution" (
  "TR_Rewards_height" BIGINT NOT NULL,
  "TR_Rewards_idx" SERIAL,
  "TR_Rewards_Pool_id" VARCHAR(64) NOT NULL,
  "TR_Rewards_timestamp" TIMESTAMPTZ NOT NULL,
  "TR_Rewards_amnt_stable" DECIMAL(39, 0) NOT NULL,
  "TR_Rewards_amnt_nls" DECIMAL(39, 0) NOT NULL,
  "Event_Block_Index" INT NOT NULL,
  "Tx_Hash" VARCHAR(64) NOT NULL,
  PRIMARY KEY ("TR_Rewards_height", "TR_Rewards_Pool_id", "Event_Block_Index")
);

CREATE TABLE IF NOT EXISTS "TR_State" (
  "TR_timestamp" TIMESTAMPTZ PRIMARY KEY NOT NULL,
  "TR_amnt_stable" DECIMAL(39,0) NOT NULL,
  "TR_amnt_nls" DECIMAL(39,0) NOT NULL
);

-- Platform state
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

-- Raw blockchain messages
CREATE TABLE IF NOT EXISTS "raw_message" (
  "index" INT NOT NULL,
  "from" VARCHAR(128) NOT NULL,
  "to" VARCHAR(128) NOT NULL,
  "tx_hash" VARCHAR(64) NOT NULL,
  "type" VARCHAR(64) NOT NULL,
  "value" TEXT,
  "block" BIGINT NOT NULL,
  "fee_amount" DECIMAL(39, 0),
  "fee_denom" VARCHAR(68),
  "memo" TEXT,
  "timestamp" TIMESTAMPTZ NOT NULL,
  "rewards" TEXT DEFAULT NULL,
  "code" INT,
  PRIMARY KEY ("index", "tx_hash")
);

-- Reserve cover loss
CREATE TABLE IF NOT EXISTS "Reserve_Cover_Loss" (
  "Tx_Hash" VARCHAR(64) NOT NULL,
  "LS_contract_id" VARCHAR(64) NOT NULL,
  "LS_symbol" VARCHAR(20) NOT NULL,
  "LS_amnt" DECIMAL(39, 0) NOT NULL,
  "Event_Block_Index" INT NOT NULL,
  "LS_timestamp" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("LS_contract_id", "Event_Block_Index", "Tx_Hash")
);

-- Push notification subscriptions
CREATE TABLE IF NOT EXISTS subscription (
  active BOOLEAN DEFAULT true NOT NULL,
  address VARCHAR(44) NOT NULL,
  p256dh VARCHAR(87) NOT NULL,
  auth VARCHAR(22) NOT NULL,
  endpoint TEXT NOT NULL,
  expiration TIMESTAMPTZ DEFAULT NULL,
  ip VARCHAR(45),
  user_agent TEXT,
  PRIMARY KEY (address, p256dh, auth)
);

-- Pool configuration
CREATE TABLE IF NOT EXISTS "pool_config" (
  "pool_id" VARCHAR(128) PRIMARY KEY,
  "position_type" VARCHAR(10) NOT NULL,
  "lpn_symbol" VARCHAR(20) NOT NULL,
  "lpn_decimals" BIGINT NOT NULL,
  "label" VARCHAR(50) NOT NULL,
  "protocol" VARCHAR(50)
);
