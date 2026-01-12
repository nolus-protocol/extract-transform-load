-- V005: Performance optimization indexes v2
-- Addresses slow queries identified in ETL logs (January 2026)

-- Block table - Gap detection query optimization
CREATE INDEX IF NOT EXISTS idx_block_id ON block (id);

-- LS_State - Unrealized PnL & Positions queries optimization
CREATE INDEX IF NOT EXISTS idx_ls_state_timestamp_contract_covering
    ON "LS_State" ("LS_timestamp" DESC, "LS_contract_id")
    INCLUDE ("LS_amnt_stable", "LS_principal_stable",
             "LS_prev_margin_stable", "LS_current_margin_stable",
             "LS_prev_interest_stable", "LS_current_interest_stable");

CREATE INDEX IF NOT EXISTS idx_ls_state_active_positions
    ON "LS_State" ("LS_timestamp", "LS_contract_id")
    WHERE "LS_amnt_stable" > 0;

-- LS_Opening - Join optimization
CREATE INDEX IF NOT EXISTS idx_ls_opening_contract_id
    ON "LS_Opening" ("LS_contract_id");

CREATE INDEX IF NOT EXISTS idx_ls_opening_contract_covering
    ON "LS_Opening" ("LS_contract_id")
    INCLUDE ("LS_address_id", "LS_asset_symbol", "LS_loan_pool_id",
             "LS_cltr_symbol", "LS_cltr_amnt_stable", "LS_timestamp");

-- MP_Asset - Price lookups optimization
CREATE INDEX IF NOT EXISTS idx_mp_asset_protocol_symbol_time
    ON "MP_Asset" ("Protocol", "MP_asset_symbol", "MP_asset_timestamp" DESC)
    INCLUDE ("MP_price_in_stable");

CREATE INDEX IF NOT EXISTS idx_mp_asset_symbol_time_range
    ON "MP_Asset" ("MP_asset_symbol", "MP_asset_timestamp")
    INCLUDE ("MP_price_in_stable");

-- LS_Liquidation - Liquidations query optimization
CREATE INDEX IF NOT EXISTS idx_ls_liquidation_timestamp_contract
    ON "LS_Liquidation" ("LS_timestamp" DESC, "LS_contract_id")
    INCLUDE ("LS_amnt_symbol", "LS_transaction_type", "LS_payment_amnt_stable",
             "LS_loan_close");

-- LS_Repayment - Repayment aggregation optimization
CREATE INDEX IF NOT EXISTS idx_ls_repayment_contract_covering
    ON "LS_Repayment" ("LS_contract_id")
    INCLUDE ("LS_prev_margin_stable", "LS_prev_interest_stable",
             "LS_current_margin_stable", "LS_current_interest_stable",
             "LS_principal_stable", "LS_timestamp");

-- LS_Close_Position - Position close lookups
CREATE INDEX IF NOT EXISTS idx_ls_close_position_contract_timestamp
    ON "LS_Close_Position" ("LS_contract_id", "LS_timestamp" DESC)
    INCLUDE ("LS_change", "LS_loan_close");

-- LS_Loan_Closing - Loan close tracking
CREATE INDEX IF NOT EXISTS idx_ls_loan_closing_contract_timestamp
    ON "LS_Loan_Closing" ("LS_contract_id", "LS_timestamp" DESC)
    INCLUDE ("LS_amnt");

-- LP_Pool_State - Pool utilization queries
CREATE INDEX IF NOT EXISTS idx_lp_pool_state_pool_timestamp
    ON "LP_Pool_State" ("LP_Pool_id", "LP_Pool_timestamp" DESC)
    INCLUDE ("LP_Pool_total_value_locked_stable", "LP_Pool_total_borrowed_stable");

-- pool_config - Frequent joins
CREATE INDEX IF NOT EXISTS idx_pool_config_pool_id
    ON pool_config (pool_id)
    INCLUDE (lpn_decimals, lpn_symbol, position_type, protocol, label);

-- LS_Closing - Closed lease exclusion
CREATE INDEX IF NOT EXISTS idx_ls_closing_contract
    ON "LS_Closing" ("LS_contract_id");

-- Update statistics for query planner optimization
ANALYZE "LS_State";
ANALYZE "LS_Opening";
ANALYZE "LS_Repayment";
ANALYZE "LS_Liquidation";
ANALYZE "LS_Close_Position";
ANALYZE "LS_Loan_Closing";
ANALYZE "LS_Closing";
ANALYZE "LP_Pool_State";
ANALYZE "MP_Asset";
ANALYZE pool_config;
ANALYZE block;
