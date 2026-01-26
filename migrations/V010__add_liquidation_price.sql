-- Add liquidation_price column to LS_Liquidation
-- This stores the asset price at the time of liquidation, avoiding expensive runtime lookups

ALTER TABLE "LS_Liquidation" 
ADD COLUMN IF NOT EXISTS "LS_liquidation_price" NUMERIC(40,18);

-- Add collateral columns that are needed for down_payment calculation
ALTER TABLE "LS_Liquidation"
ADD COLUMN IF NOT EXISTS "LS_cltr_symbol" VARCHAR(20),
ADD COLUMN IF NOT EXISTS "LS_cltr_amnt_stable" NUMERIC(39,0);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_ls_liquidation_price 
ON "LS_Liquidation"("LS_liquidation_price") 
WHERE "LS_liquidation_price" IS NOT NULL;
