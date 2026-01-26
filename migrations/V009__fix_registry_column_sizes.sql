-- V008: Fix registry column sizes
-- The dex field can contain JSON objects, and contract addresses are 64 chars

-- Increase dex column to accommodate JSON objects like:
-- {"Astroport":{"router_address":"neutron1..."}}
ALTER TABLE "protocol_registry" 
    ALTER COLUMN "dex" TYPE VARCHAR(512);

-- Increase contract columns to be safe (addresses are ~64 chars but let's be generous)
ALTER TABLE "protocol_registry" 
    ALTER COLUMN "leaser_contract" TYPE VARCHAR(128),
    ALTER COLUMN "lpp_contract" TYPE VARCHAR(128),
    ALTER COLUMN "oracle_contract" TYPE VARCHAR(128),
    ALTER COLUMN "profit_contract" TYPE VARCHAR(128),
    ALTER COLUMN "reserve_contract" TYPE VARCHAR(128);
