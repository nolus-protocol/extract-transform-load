17.11.2023

ALTER TABLE "LP_Pool_State" ADD COLUMN "LP_Pool_min_utilization_threshold" DECIMAL(39, 0) NOT NULL DEFAULT 0;

15.01.2024

ALTER TABLE "MP_Asset_Mapping" DROP CONSTRAINT "MP_Asset_Mapping_pkey";
ALTER TABLE "MP_Asset_Mapping" ADD CONSTRAINT "MP_Asset_Mapping_pkey" PRIMARY KEY ("MP_asset_symbol","MP_asset_symbol_coingecko");

DELETE FROM "TR_Rewards_Distribution";
ALTER TABLE "TR_Rewards_Distribution" ADD COLUMN "Event_Block_Index" INT NOT NULL;
ALTER TABLE "TR_Rewards_Distribution" DROP CONSTRAINT "TR_Rewards_Distribution_pkey";
ALTER TABLE "TR_Rewards_Distribution" ADD CONSTRAINT "TR_Rewards_Distribution_pkey" PRIMARY KEY ("TR_Rewards_height","Event_Block_Index", "TR_Rewards_Pool_id");
DELETE FROM block;

30.01.2024

select \* from "MP_Asset" where "MP_asset_symbol" = 'WBTC' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 100
WHERE "MP_asset_symbol" = 'WBTC' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

select \* from "MP_Asset" where "MP_asset_symbol" = 'WETH' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 1000000000000
WHERE "MP_asset_symbol" = 'WETH' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

select \* from "MP_Asset" where "MP_asset_symbol" = 'EVMOS' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 1000000000000
WHERE "MP_asset_symbol" = 'EVMOS' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

select \* from "MP_Asset" where "MP_asset_symbol" = 'DYDX' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 1000000000000
WHERE "MP_asset_symbol" = 'DYDX' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

select \* from "MP_Asset" where "MP_asset_symbol" = 'CRO' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 100
WHERE "MP_asset_symbol" = 'CRO' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

select \* from "MP_Asset" where "MP_asset_symbol" = 'INJ' ORDER BY "MP_asset_timestamp" DESC;

UPDATE "MP_Asset"
SET "MP_price_in_stable" = "MP_price_in_stable" \* 1000000000000
WHERE "MP_asset_symbol" = 'INJ' AND "MP_asset_timestamp" >= '2024-01-22 16:04:17.362439+00';

---

block 3605559 , 2024-01-20 00:11:03+02

DELETE FROM "LS_Close_Position" WHERE "LS_timestamp" >= '2024-01-20 00:11:03+02';
DELETE FROM "LS_Liquidation" WHERE "LS_timestamp" >= '2024-01-20 00:11:03+02';
DELETE FROM "LS_Opening" WHERE "LS_timestamp" >= '2024-01-20 00:11:03+02';
DELETE FROM "LS_Repayment" WHERE "LS_timestamp" >= '2024-01-20 00:11:03+02';
DELETE FROM "block" WHERE "id" >= 3605559;

finished at 2024-01-31 12:24:04

LS_loan_amnt_stable at 2024-02-12 22:43:04

---

13.02.2024

select COUNT(\*) from "MP_Asset" where "MP_asset_symbol" = 'NEUTRON';
UPDATE "MP_Asset" set "MP_asset_symbol" = 'NTRN' WHERE "MP_asset_symbol" = 'NEUTRON';

ALTER TABLE "MP_Asset" ADD COLUMN "Protocol" VARCHAR(256);

UPDATE "MP_Asset"
SET "Protocol" = 'OSMOSIS-OSMOSIS-USDC_AXELAR' WHERE "MP_asset_symbol" IN ('NLS','USDC','OSMO','ST_OSMO', 'ATOM', 'ST_ATOM', 'WETH', 'WBTC', 'AKT', 'AXL', 'JUNO', 'EVMOS', 'STK_ATOM', 'SCRT', 'CRO', 'TIA', 'STARS', 'Q_ATOM', 'STRD', 'INJ');

UPDATE "MP_Asset"
SET "Protocol" = 'NEUTRON-ASTROPORT-USDC_AXELAR' WHERE "MP_asset_symbol" IN ('NTRN','USDC_AXELAR', 'DYDX');

ALTER TABLE "MP_Asset" ALTER COLUMN "Protocol" SET NOT NULL;
ALTER TABLE "MP_Asset" DROP CONSTRAINT "MP_Asset_pkey";
ALTER TABLE "MP_Asset" ADD CONSTRAINT "MP_Asset_pkey" PRIMARY KEY ("MP_asset_symbol", "MP_asset_timestamp", "Protocol");

22.02.2024 NEW CURRENCIES

(stride-staked-tia,stTIA,6),
(jackal-protocol,JKL,6),
(milkyway-staked-tia,milkTIA,6),
(levana-protocol,LVN,6),
(quasar-2,QSR,6),
(picasso,PICA,12),
(dymension,DYM,18),

06.06.2024

ALTER TABLE "LS_State" ADD COLUMN "LS_amnt" DECIMAL(39, 0) NOT NULL DEFAULT 0;


26.07.2024
