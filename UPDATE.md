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

DROP FROM "block";
INSERT INTO "block" (id) VALUES (1994921),(3659894);

ALTER TABLE "LP_Deposit" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LP_Withdraw" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LS_Close_Position" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LS_Closing" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LS_Liquidation" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LS_Opening" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "LS_Repayment" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "TR_Profit" ADD COLUMN "Tx_Hash" VARCHAR(64);
ALTER TABLE "TR_Rewards_Distribution" ADD COLUMN "Tx_Hash" VARCHAR(64);

21.08.2024

ALTER TABLE "raw_message" ALTER COLUMN "fee_denom" TYPE VARCHAR(68);

15.09.2024

ALTER TABLE "LS_Repayment" ADD COLUMN "LS_payment" DECIMAL(39, 0);
ALTER TABLE "LS_Repayment" RENAME COLUMN "LS_symbol" TO "LS_payment_symbol";
ALTER TABLE "LS_Repayment" RENAME COLUMN "LS_amnt_stable" TO "LS_payment_amnt_stable";
ALTER TABLE "LS_Repayment" RENAME COLUMN "LS_payment" TO "LS_payment_amnt";

ALTER TABLE "LS_Opening" ADD COLUMN "LS_loan_amnt" DECIMAL(39, 0) NOT NULL DEFAULT 0;
ALTER TABLE "LS_Opening" ADD COLUMN "LS_lpn_loan_amnt" DECIMAL(39, 0) NOT NULL DEFAULT 0;

ALTER TABLE "LS_Liquidation" ADD COLUMN "LS_amnt" DECIMAL(39, 0);
ALTER TABLE "LS_Liquidation" ADD COLUMN "LS_payment_amnt" DECIMAL(39, 0);
ALTER TABLE "LS_Liquidation" ADD COLUMN "LS_payment_amnt_stable" DECIMAL(39, 0);
ALTER TABLE "LS_Liquidation" ADD COLUMN "LS_loan_close" BOOLEAN;
ALTER TABLE "LS_Liquidation"
  RENAME COLUMN "LS_symbol" TO "LS_payment_symbol";
ALTER TABLE "LS_Liquidation" ADD COLUMN "LS_amnt_symbol" VARCHAR(20);

ALTER TABLE "LS_Close_Position" RENAME COLUMN "LS_amnt_stable" TO "LS_payment_amnt_stable";
ALTER TABLE "LS_Close_Position" ADD COLUMN "LS_amnt_stable" DECIMAL(39, 0);
ALTER TABLE "LS_Close_Position" ADD COLUMN "LS_payment_amnt" DECIMAL(39, 0);
ALTER TABLE "LS_Close_Position" RENAME COLUMN "LS_amount_amount" TO "LS_amnt";
ALTER TABLE "LS_Close_Position" RENAME COLUMN "LS_amount_symbol" TO "LS_amnt_symbol";
ALTER TABLE "LS_Close_Position" RENAME COLUMN "LS_symbol" TO "LS_payment_symbol";

ALTER TABLE "LS_State" ADD COLUMN "LS_lpn_loan_amnt" DECIMAL(39, 0) NOT NULL DEFAULT 0;

./nolusd q wasm contract-state smart nolus1x8dyqec8kx75rf5zfmfs0lyatw24fasssygrdq7kpaw86te9emwsp8xeqf '{}' --output json --node http://10.133.133.41:26602 --height 793010
./nolusd q wasm contract-state smart nolus1x8dyqec8kx75rf5zfmfs0lyatw24fasssygrdq7kpaw86te9emwsp8xeqf '{}' --output json --node http://10.133.133.41:26602 --height 3658999
./nolusd q wasm contract-state smart nolus1x8dyqec8kx75rf5zfmfs0lyatw24fasssygrdq7kpaw86te9emwsp8xeqf '{}' --output json --node http://10.133.133.41:26602 --height 3659000
./nolusd q wasm contract-state smart nolus1p4lqunauqgstt6ydszx59y3pg2tkaxlnujl9m5ldz7nqcrn6tjzq3geava '{}' --output json --node http://10.133.133.41:26602 --height 402768
./nolusd q tx 3D3F23B67706B32183DEC9B480DC5054D4BFB06C90958EA66B92ADF8F86C7014 --output json --node http://10.133.133.41:26602
./nolusd q block 1994921 --output json --node http://10.133.133.41:26602

./nolusd q tx c178f256d36c5c1f58221d81358544da6c761baec2a8cc9b66c81421de526464 --output json --node https://nolus.rpc.kjnodes.com
./nolusd q tx c178f256d36c5c1f58221d81358544da6c761baec2a8cc9b66c81421de526464 --output json --node https://rpc.lavenderfive.com:443/nolus

https://pirin-cl.nolus.network:26657/tx?hash=%22c178f256d36c5c1f58221d81358544da6c761baec2a8cc9b66c81421de526464%22&prove=true

insert into block(id) values (1994921);
insert into block(id) values (3659894);

DELETE FROM "block" WHERE "id" >= 6871800;
insert into block(id) values (6896464);

insert into 
  "LS_Repayment"
  (
    "LS_repayment_height",
    "LS_contract_id",
    "LS_payment_symbol",
    "LS_payment_amnt",
    "LS_payment_amnt_stable",
    "LS_timestamp",
    "LS_loan_close",
    "LS_prev_margin_stable",
    "LS_prev_interest_stable",
    "LS_current_margin_stable",
    "LS_current_interest_stable",
    "LS_principal_stable",
    "Tx_Hash"
  )
  
  values

  (  
  1994921,
  'nolus1zkzrmqkrswrq42wpxs5fvevd76hvzww9v7rx27vafkd8qmfqrj9sr9pc47',f
  'USDC',
  '6905',
  '6905',
  to_timestamp(1696388400363/ 1000),
  false,
  0,
  0,
  1959,
  4946,
  0,
  '17E4F89760D5DEBC10745CFD795C4298332A1268A36762B7C15ABD21736CFF35'
  )

end 6871800;

HOST=nolus.rpc.kjnodes.com
GRPC_HOST=https://nolus.grpc.kjnodes.com

1.10.2024
SELECT * FROM "LS_Repayment" WHERE  "LS_repayment_height" = 1994921 AND "LS_contract_id" = 'nolus1zkzrmqkrswrq42wpxs5fvevd76hvzww9v7rx27vafkd8qmfqrj9sr9pc47';

UPDATE "LS_Repayment" SET "Tx_Hash" = '17E4F89760D5DEBC10745CFD795C4298332A1268A36762B7C15ABD21736CFF35', "LS_payment_amnt" = 6905 WHERE  "LS_repayment_height" = 1994921 AND "LS_contract_id" = 'nolus1zkzrmqkrswrq42wpxs5fvevd76hvzww9v7rx27vafkd8qmfqrj9sr9pc47';

ALTER TABLE "LP_Deposit" ALTER COLUMN "Tx_Hash" SET NOT NULL;
ALTER TABLE "LP_Withdraw" ALTER COLUMN "Tx_Hash" SET NOT NULL;

ALTER TABLE "LS_Close_Position" ALTER COLUMN "LS_amnt_stable" SET NOT NULL;
ALTER TABLE "LS_Close_Position" ALTER COLUMN "LS_payment_amnt" SET NOT NULL;
ALTER TABLE "LS_Close_Position" ALTER COLUMN "LS_payment_symbol" SET NOT NULL;
ALTER TABLE "LS_Close_Position" ALTER COLUMN "Tx_Hash" SET NOT NULL;

ALTER TABLE "LS_Liquidation" ALTER COLUMN "Tx_Hash" SET NOT NULL;
ALTER TABLE "LS_Liquidation" ALTER COLUMN "LS_amnt" SET NOT NULL;
ALTER TABLE "LS_Liquidation" ALTER COLUMN "LS_payment_symbol" SET NOT NULL;
ALTER TABLE "LS_Liquidation" ALTER COLUMN "LS_payment_amnt" SET NOT NULL;
ALTER TABLE "LS_Liquidation" ALTER COLUMN "LS_payment_amnt_stable" SET NOT NULL;
ALTER TABLE "LS_Liquidation" ALTER COLUMN "LS_loan_close" SET NOT NULL;

ALTER TABLE "LS_Opening" ALTER COLUMN "Tx_Hash" SET NOT NULL;
ALTER TABLE "LS_Opening" ALTER COLUMN "LS_loan_amnt" DROP DEFAULT;
ALTER TABLE "LS_Opening" ALTER COLUMN "LS_lpn_loan_amnt" DROP DEFAULT;

ALTER TABLE "LS_Repayment" ALTER COLUMN "Tx_Hash" SET NOT NULL;
ALTER TABLE "LS_Repayment" ALTER COLUMN "LS_payment_amnt" SET NOT NULL;

ALTER TABLE "LS_State" ALTER COLUMN "LS_lpn_loan_amnt" DROP DEFAULT;

ALTER TABLE "Reserve_Cover_Loss" ALTER COLUMN "Tx_Hash" SET NOT NULL;

ALTER TABLE "MP_Asset" ALTER COLUMN "MP_price_in_stable" SET NOT NULL;

ALTER TABLE "TR_Profit" ALTER COLUMN "Tx_Hash" SET NOT NULL;

ALTER TABLE "TR_Rewards_Distribution" ALTER COLUMN "Tx_Hash" SET NOT NULL;

ALTER TABLE "LS_Closing" ALTER COLUMN "Tx_Hash" SET NOT NULL;

update/v3/ls_loan_amnt
update/v3/ls_lpn_loan_amnt

UPDATE "LS_Loan_Closing" SET "Active" = false;

UPDATE "LS_Loan_Closing" SET "Active" = false WHERE "LS_contract_id" = 'nolus16wpqsayglk9pkcvwuswsvxpl5gzv6uwmseq0lw8pvg6vkyp0wfaq9kgnq0';
SELECT FROM "LS_Repayment" WHERE "LS_payment_amnt" IS NULL;


15.10.2024

ALTER TABLE "raw_message" ADD COLUMN "rewards" TEXT DEFAULT NULL;

05.11.2024

UPDATE "LS_Opening" 
SET "LS_loan_amnt_stable" = "LS_loan_amnt_asset"
WHERE "LS_loan_pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5' AND "LS_loan_amnt_stable" != "LS_loan_amnt_asset";

UPDATE "LS_Opening" 
SET "LS_loan_amnt_stable" = "LS_loan_amnt_asset"
WHERE "LS_loan_pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' AND "LS_loan_amnt_stable" != "LS_loan_amnt_asset";

UPDATE "LS_Loan_Closing" SET "Active" = false;


25.11.2024

in .env
MP_ASSET_INTERVAL_IN_SEC=20

in database
UPDATE "LS_Loan_Closing" SET "Active" = false;

28.11.2024

update "LS_Loan_Closing" set "Active" = false where "Block" > 8000000;

28.11.2024

ALTER TABLE "LS_State" ADD COLUMN  "LS_prev_margin_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0;
ALTER TABLE "LS_State" ADD COLUMN  "LS_prev_interest_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0;
ALTER TABLE "LS_State" ADD COLUMN  "LS_current_margin_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0;
ALTER TABLE "LS_State" ADD COLUMN  "LS_current_interest_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0;
ALTER TABLE "LS_State" ADD COLUMN  "LS_principal_asset" DECIMAL(39, 0) NOT NULL DEFAULT 0;

03.12.2024

update "LS_Loan_Closing" set "Active" = false where "Type" = 'liquidation';
update "LS_Loan_Closing" set "Active" = false where "Block" > 7000000;

13.12.2024

ALTER TABLE "LP_Deposit" ALTER COLUMN "LP_address_id" TYPE VARCHAR(64);
ALTER TABLE "LP_Withdraw" ALTER COLUMN "LP_address_id" TYPE VARCHAR(64);
ALTER TABLE "LP_Lender_State" ALTER COLUMN "LP_Lender_id" TYPE VARCHAR(64);
ALTER TABLE "LS_Liquidation_Warning" ALTER COLUMN "LS_address_id" TYPE VARCHAR(64);
ALTER TABLE "LS_Opening" ALTER COLUMN "LS_address_id" TYPE VARCHAR(64);

18.12.2024

CREATE INDEX idx_from ON raw_message("from");
CREATE INDEX idx_to ON raw_message("to");


15.07.2025

CREATE INDEX idx_auth ON subscription("auth");
update .env
add certs

26.08.2025

ALTER TABLE "LP_Pool" ADD COLUMN  "LP_status" BOOLEAN NOT NULL DEFAULT true;
UPDATE "LP_Pool" SET "LP_status" = false WHERE "LP_Pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990';

17.09.2025

UPDATE "LP_Pool" SET "LP_status" = false WHERE "LP_Pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94';
