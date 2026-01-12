-- V004: Performance indexes for common query patterns

-- LS_Opening indexes
CREATE INDEX IF NOT EXISTS idx_ls_opening_timestamp ON "LS_Opening" ("LS_timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_ls_opening_address ON "LS_Opening" ("LS_address_id");
CREATE INDEX IF NOT EXISTS idx_ls_opening_pool ON "LS_Opening" ("LS_loan_pool_id");
CREATE INDEX IF NOT EXISTS idx_ls_opening_asset ON "LS_Opening" ("LS_asset_symbol");

-- MP_Asset indexes
CREATE INDEX IF NOT EXISTS idx_mp_asset_symbol_time ON "MP_Asset" ("MP_asset_symbol", "MP_asset_timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_mp_asset_protocol_time ON "MP_Asset" ("Protocol", "MP_asset_timestamp" DESC);

-- LS_State indexes
CREATE INDEX IF NOT EXISTS idx_ls_state_timestamp ON "LS_State" ("LS_timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_ls_state_contract ON "LS_State" ("LS_contract_id");
CREATE INDEX IF NOT EXISTS idx_ls_state_contract_timestamp ON "LS_State" ("LS_contract_id", "LS_timestamp" DESC);

-- LP_Deposit indexes
CREATE INDEX IF NOT EXISTS idx_lp_deposit_address ON "LP_Deposit" ("LP_address_id");
CREATE INDEX IF NOT EXISTS idx_lp_deposit_timestamp ON "LP_Deposit" ("LP_timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_lp_deposit_pool ON "LP_Deposit" ("LP_Pool_id");

-- LP_Withdraw indexes
CREATE INDEX IF NOT EXISTS idx_lp_withdraw_address ON "LP_Withdraw" ("LP_address_id");
CREATE INDEX IF NOT EXISTS idx_lp_withdraw_timestamp ON "LP_Withdraw" ("LP_timestamp" DESC);

-- LS_Repayment indexes
CREATE INDEX IF NOT EXISTS idx_ls_repayment_contract ON "LS_Repayment" ("LS_contract_id");
CREATE INDEX IF NOT EXISTS idx_ls_repayment_timestamp ON "LS_Repayment" ("LS_timestamp" DESC);

-- LS_Liquidation indexes
CREATE INDEX IF NOT EXISTS idx_ls_liquidation_contract ON "LS_Liquidation" ("LS_contract_id");
CREATE INDEX IF NOT EXISTS idx_ls_liquidation_timestamp ON "LS_Liquidation" ("LS_timestamp" DESC);

-- LS_Close_Position indexes
CREATE INDEX IF NOT EXISTS idx_ls_close_position_contract ON "LS_Close_Position" ("LS_contract_id");
CREATE INDEX IF NOT EXISTS idx_ls_close_position_timestamp ON "LS_Close_Position" ("LS_timestamp" DESC);

-- LP_Lender_State indexes
CREATE INDEX IF NOT EXISTS idx_lp_lender_state_address ON "LP_Lender_State" ("LP_address_id");
CREATE INDEX IF NOT EXISTS idx_lp_lender_state_timestamp ON "LP_Lender_State" ("LP_timestamp" DESC);

-- LP_Pool_State indexes
CREATE INDEX IF NOT EXISTS idx_lp_pool_state_timestamp ON "LP_Pool_State" ("LP_Pool_timestamp" DESC);

-- raw_message indexes
CREATE INDEX IF NOT EXISTS idx_from ON raw_message("from");
CREATE INDEX IF NOT EXISTS idx_to ON raw_message("to");

-- subscription index
CREATE INDEX IF NOT EXISTS idx_auth ON subscription("auth");
