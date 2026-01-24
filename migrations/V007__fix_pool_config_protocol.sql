-- V007: Fix pool_config protocol values
-- The protocol values for two pools were swapped in V002

UPDATE pool_config 
SET protocol = CASE 
    WHEN pool_id = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6' THEN 'NEUTRON-ASTROPORT-USDC_NOBLE'
    WHEN pool_id = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf' THEN 'OSMOSIS-OSMOSIS-USDC_NOBLE'
END
WHERE pool_id IN (
    'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6',
    'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf'
);
