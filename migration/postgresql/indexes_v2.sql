-- Performance optimization indexes v2
-- Addresses slow queries identified in ETL logs (January 2026)
-- Run this migration to improve query performance

-- ============================================================================
-- 1. BLOCK TABLE - Gap Detection Query (10s -> <1s)
-- ============================================================================
-- The gap detection query uses LAG() window function over all blocks
-- Adding an index on id and ensuring it's the primary key helps

-- Ensure block.id has proper index (should be PK but verify)
CREATE INDEX IF NOT EXISTS idx_block_id ON block (id);

-- ============================================================================
-- 2. LS_STATE TABLE - Unrealized PnL & Positions Queries (120s -> <5s)
-- ============================================================================
-- Critical: These queries use MAX(LS_timestamp) then filter by it
-- The existing idx_ls_state_timestamp helps, but we need covering indexes

-- Composite index for the "Latest_Aggregation" + "Latest_States" pattern
-- This is the most critical optimization
CREATE INDEX IF NOT EXISTS idx_ls_state_timestamp_contract_covering 
    ON "LS_State" ("LS_timestamp" DESC, "LS_contract_id")
    INCLUDE ("LS_amnt_stable", "LS_principal_stable", 
             "LS_prev_margin_stable", "LS_current_margin_stable",
             "LS_prev_interest_stable", "LS_current_interest_stable");

-- Index for filtering active positions (LS_amnt_stable > 0)
CREATE INDEX IF NOT EXISTS idx_ls_state_active_positions 
    ON "LS_State" ("LS_timestamp", "LS_contract_id") 
    WHERE "LS_amnt_stable" > 0;

-- ============================================================================
-- 3. LS_OPENING TABLE - Join Optimization
-- ============================================================================
-- LS_Opening is joined frequently with LS_State on contract_id
-- Also joined with pool_config on LS_loan_pool_id

-- Primary lookup index (may already exist as PK)
CREATE INDEX IF NOT EXISTS idx_ls_opening_contract_id 
    ON "LS_Opening" ("LS_contract_id");

-- Covering index for the common join pattern in positions queries
CREATE INDEX IF NOT EXISTS idx_ls_opening_contract_covering
    ON "LS_Opening" ("LS_contract_id")
    INCLUDE ("LS_address_id", "LS_asset_symbol", "LS_loan_pool_id", 
             "LS_cltr_symbol", "LS_cltr_amnt_stable", "LS_timestamp");

-- ============================================================================
-- 4. MP_ASSET TABLE - Price Lookups (used in LATERAL JOINs)
-- ============================================================================
-- Realized PnL queries use LATERAL JOINs with time-range lookups on MP_Asset
-- The existing idx_mp_asset_symbol_time helps but we need more specific indexes

-- Index for protocol-filtered latest price lookups
CREATE INDEX IF NOT EXISTS idx_mp_asset_protocol_symbol_time 
    ON "MP_Asset" ("Protocol", "MP_asset_symbol", "MP_asset_timestamp" DESC)
    INCLUDE ("MP_price_in_stable");

-- Index for time-range lookups in LATERAL JOINs (realized PnL)
CREATE INDEX IF NOT EXISTS idx_mp_asset_symbol_time_range
    ON "MP_Asset" ("MP_asset_symbol", "MP_asset_timestamp")
    INCLUDE ("MP_price_in_stable");

-- ============================================================================
-- 5. LS_LIQUIDATION TABLE - Liquidations Query (18s -> <3s)
-- ============================================================================
-- Query filters by timestamp range and joins with LS_Opening

-- Composite index for time-filtered liquidation queries
CREATE INDEX IF NOT EXISTS idx_ls_liquidation_timestamp_contract
    ON "LS_Liquidation" ("LS_timestamp" DESC, "LS_contract_id")
    INCLUDE ("LS_amnt_symbol", "LS_transaction_type", "LS_payment_amnt_stable", 
             "LS_loan_close", "LS_loan_amnt_asset");

-- ============================================================================
-- 6. LS_REPAYMENT TABLE - Repayment Aggregation
-- ============================================================================
-- Repayments are summed by contract_id in multiple queries

-- Covering index for repayment aggregation
CREATE INDEX IF NOT EXISTS idx_ls_repayment_contract_covering
    ON "LS_Repayment" ("LS_contract_id")
    INCLUDE ("LS_prev_margin_stable", "LS_prev_interest_stable",
             "LS_current_margin_stable", "LS_current_interest_stable",
             "LS_principal_stable", "LS_timestamp");

-- ============================================================================
-- 7. LS_CLOSE_POSITION TABLE - Position Close Lookups
-- ============================================================================
-- Used in realized PnL calculations

CREATE INDEX IF NOT EXISTS idx_ls_close_position_contract_timestamp
    ON "LS_Close_Position" ("LS_contract_id", "LS_timestamp" DESC)
    INCLUDE ("LS_change", "LS_loan_close");

-- ============================================================================
-- 8. LS_LOAN_CLOSING TABLE - Loan Close Tracking
-- ============================================================================
-- Used to find closed loans for realized PnL

CREATE INDEX IF NOT EXISTS idx_ls_loan_closing_contract_timestamp
    ON "LS_Loan_Closing" ("LS_contract_id", "LS_timestamp" DESC)
    INCLUDE ("LS_amnt");

-- ============================================================================
-- 9. LP_POOL_STATE TABLE - Pool Utilization Queries (4s -> <1s)
-- ============================================================================
-- Queries look for latest state per pool

CREATE INDEX IF NOT EXISTS idx_lp_pool_state_pool_timestamp
    ON "LP_Pool_State" ("LP_Pool_id", "LP_Pool_timestamp" DESC)
    INCLUDE ("LP_Pool_total_value_locked_stable", "LP_Pool_total_borrowed_stable");

-- ============================================================================
-- 10. POOL_CONFIG TABLE - Frequent Joins
-- ============================================================================
-- pool_config is joined on pool_id in most queries

CREATE INDEX IF NOT EXISTS idx_pool_config_pool_id 
    ON pool_config (pool_id)
    INCLUDE (lpn_decimals, lpn_symbol, position_type, protocol, label);

-- ============================================================================
-- 11. LS_CLOSING TABLE - Closed Lease Exclusion
-- ============================================================================
-- Used in get_active_states to exclude closed leases

CREATE INDEX IF NOT EXISTS idx_ls_closing_contract 
    ON "LS_Closing" ("LS_contract_id");

-- ============================================================================
-- ANALYZE TABLES
-- ============================================================================
-- Update statistics after creating indexes for query planner optimization

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
