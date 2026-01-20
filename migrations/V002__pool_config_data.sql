-- V002: Pool configuration seed data
-- Insert pool configurations (upsert to handle re-runs)

INSERT INTO "pool_config" ("pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label", "protocol") VALUES
    ('nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE', 'NEUTRON-ASTROPORT-USDC_NOBLE'),
    ('nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5', 'Long', 'USDC', 1000000, 'USDC', 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
    ('nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94', 'Long', 'USDC_AXELAR', 1000000, 'USDC_AXELAR', 'NEUTRON-ASTROPORT-USDC_AXELAR'),
    ('nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE (Neutron)', 'OSMOSIS-OSMOSIS-USDC_NOBLE'),
    ('nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'Short', 'ST_ATOM', 1000000, 'ST_ATOM (Short)', 'OSMOSIS-OSMOSIS-ST_ATOM'),
    ('nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short', 'ALL_BTC', 100000000, 'ALL_BTC (Short)', 'OSMOSIS-OSMOSIS-ALL_BTC'),
    ('nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short', 'ALL_SOL', 1000000000, 'ALL_SOL (Short)', 'OSMOSIS-OSMOSIS-ALL_SOL'),
    ('nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short', 'AKT', 1000000, 'AKT (Short)', 'OSMOSIS-OSMOSIS-AKT'),
    ('nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short', 'ATOM', 1000000, 'ATOM (Short)', 'OSMOSIS-OSMOSIS-ATOM'),
    ('nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short', 'OSMO', 1000000, 'OSMO (Short)', 'OSMOSIS-OSMOSIS-OSMO')
ON CONFLICT ("pool_id") DO UPDATE SET
    "position_type" = EXCLUDED."position_type",
    "lpn_symbol" = EXCLUDED."lpn_symbol",
    "lpn_decimals" = EXCLUDED."lpn_decimals",
    "label" = EXCLUDED."label",
    "protocol" = EXCLUDED."protocol";
