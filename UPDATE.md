17.11.2023

ALTER TABLE "LP_Pool_State" ADD COLUMN "LP_Pool_min_utilization_threshold" DECIMAL(39, 0) NOT NULL DEFAULT 0;

15.01.2024

ALTER TABLE "MP_Asset_Mapping" DROP CONSTRAINT "MP_Asset_Mapping_pkey";

ALTER TABLE "MP_Asset_Mapping" ADD CONSTRAINT "MP_Asset_Mapping_pkey" PRIMARY KEY ("MP_asset_symbol","MP_asset_symbol_coingecko");