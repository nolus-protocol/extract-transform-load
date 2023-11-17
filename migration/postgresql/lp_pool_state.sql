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