-- V006: Add unique constraints for ON CONFLICT clauses
-- These constraints are required for the ON CONFLICT DO NOTHING clauses to work properly

-- LS_Liquidation: ON CONFLICT ("LS_liquidation_height", "LS_contract_id")
CREATE UNIQUE INDEX IF NOT EXISTS "LS_Liquidation_height_contract_unique" 
ON "LS_Liquidation" ("LS_liquidation_height", "LS_contract_id");

-- LP_Deposit: ON CONFLICT ("LP_deposit_height", "LP_address_id", "LP_timestamp", "LP_Pool_id")
CREATE UNIQUE INDEX IF NOT EXISTS "LP_Deposit_height_address_timestamp_pool_unique"
ON "LP_Deposit" ("LP_deposit_height", "LP_address_id", "LP_timestamp", "LP_Pool_id");

-- LP_Withdraw: ON CONFLICT ("LP_withdraw_height", "LP_address_id", "LP_timestamp", "LP_Pool_id")
CREATE UNIQUE INDEX IF NOT EXISTS "LP_Withdraw_height_address_timestamp_pool_unique"
ON "LP_Withdraw" ("LP_withdraw_height", "LP_address_id", "LP_timestamp", "LP_Pool_id");

-- TR_Profit: ON CONFLICT ("TR_Profit_height", "TR_Profit_timestamp")
CREATE UNIQUE INDEX IF NOT EXISTS "TR_Profit_height_timestamp_unique"
ON "TR_Profit" ("TR_Profit_height", "TR_Profit_timestamp");

-- LS_Repayment: ON CONFLICT ("LS_repayment_height", "LS_contract_id", "LS_timestamp")
CREATE UNIQUE INDEX IF NOT EXISTS "LS_Repayment_height_contract_timestamp_unique"
ON "LS_Repayment" ("LS_repayment_height", "LS_contract_id", "LS_timestamp");

-- LS_Close_Position: ON CONFLICT ("LS_position_height", "LS_contract_id")
CREATE UNIQUE INDEX IF NOT EXISTS "LS_Close_Position_height_contract_unique"
ON "LS_Close_Position" ("LS_position_height", "LS_contract_id");
