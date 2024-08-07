# Extract Transform Load

Define Nolus data necessary for further analysis and implement an agent extracting it, transforming and loading into a relational SQL database.

## HOW TO

### PostgreSQL

### Linux

sudo -i -u postgres

### OSX

psql -U <CURRENTLY_LOGGED_IN_MAC_USERNAME> postgres

psql

1. CREATE DATABASE database_name;
2. GRANT ALL PRIVILEGES ON DATABASE database_name to user_name;
3. GRANT ALL ON SCHEMA public TO user_name;
4. Copy .env.example to .env and set necessary settings
5. Add in COINGECKO_INFO_URL, COINGECKO_PRICES_URL, COINGECKO_MARKET_DATA_RANGE_URL config for PRO_API_KEY

### Test config:

```
HOST=pirin-cl.nolus.network:26657
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=16
SUPPORTED_CURRENCIES=(nolus,NLS,6),(usd-coin,USDC,6),(osmosis,OSMO,6),(stride-staked-osmo,ST_OSMO,6),(cosmos,ATOM,6),(stride-staked-atom,ST_ATOM,6),(weth,WETH,18),(wrapped-bitcoin,WBTC,8),(akash-network,AKT,6),(axelar,AXL,6),(juno-network,JUNO,6),(evmos,EVMOS,18),(stkatom,STK_ATOM,6),(secret,SCRT,6),(crypto-com-chain,CRO,8),(celestia,TIA,6),(stargaze,STARS,6),(cosmos,Q_ATOM,6),(neutron-3,NTRN,6),(usd-coin,USDC_AXELAR,6),(dydx-chain,DYDX,18),(stride,STRD,6),(injective-protocol,INJ,18),(stride-staked-tia,ST_TIA,6),(jackal-protocol,JKL,6),(milkyway-staked-tia,MILK_TIA,6),(levana-protocol,LVN,6),(quasar-2,QSR,6),(picasso,PICA,12),(dymension,DYM,18),(usd-coin,USDC_NOBLE,6),(cudos,CUDOS,18)
STABLE_CURRENCY=usd
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
CACHE_INTERVAL_IN_MINUTES=60
LP_POOLS=(nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6,USDC_NOBLE),(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,USDC),(nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94,USDC_AXELAR),(nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf,USDC_NOBLE)
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz
SERVER_HOST=127.0.0.1
PORT=8080
ALLOWED_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://app-dev.nolus.io,https://app.nolus.io
TIMEOUT=300
MAX_TASKS=64
ADMIN_CONTRACT=nolus1gurgpv8savnfw66lckwzn4zk7fp394lpe667dhu7aw48u40lj6jsqxf8nd
IGNORE_PROTOCOLS=
INITIAL_PROTOCOL=OSMOSIS-OSMOSIS-USDC_AXELAR
LPN_PRICE=1
LPNS=USDC,USDC_AXELAR,USDC_NOBLE
LPN_DECIMALS=6
SOCKET_RECONNECT_INTERVAL=5
COINGECKO_INFO_URL=https://pro-api.coingecko.com/api/v3/coins/$0?localization=false&market_data=false&tickers=false&developer_data=false&community_data=false&x_cg_pro_api_key=PRO_API_KEY
COINGECKO_PRICES_URL=https://pro-api.coingecko.com/api/v3/simple/price?ids=$0&vs_currencies=$1&x_cg_pro_api_key=PRO_API_KEY
COINGECKO_MARKET_DATA_RANGE_URL=https://pro-api.coingecko.com/api/v3/coins/$0/market_chart/range?vs_currency=$1&from=$2&to=$3&x_cg_pro_api_key=PRO_API_KEY
```

### Testnet config:

```
HOST=rila-cl.nolus.network:26657
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=16
SUPPORTED_CURRENCIES=(nolus,NLS,6),(usd-coin,USDC,6),(usd-coin,USDC_AXELAR,6),(osmosis,OSMO,6),(cosmos,ATOM,6),(akash-network,AKT,6),(juno-network,JUNO,6),(neutron-3,NTRN,6),(usd-coin,USDC_NOBLE,6)
STABLE_CURRENCY=usd
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
LP_POOLS=(nolus1z4v8pfm096cjnv6cn89rckws8q6skrfs0r4fw0wgf72llu5pt25qywtmla,USDC),(nolus1sk9nscycmlfqpycz0ef70plyk00d3tdyepyq0kfdezduckpsq4sskuflsp,USDC_AXELAR),(nolus1xmtvsclh3tp47aelxf3huzf5v7wqzgrvchjdkr4nac2xxuh4x5uqpjh6yn,USDC_NOBLE),(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,USDC)
CACHE_INTERVAL_IN_MINUTES=60
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz
SERVER_HOST=127.0.0.1
PORT=8080
ALLOWED_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://app-dev.nolus.io
TIMEOUT=300
MAX_TASKS=64
ADMIN_CONTRACT=nolus1gurgpv8savnfw66lckwzn4zk7fp394lpe667dhu7aw48u40lj6jsqxf8nd
IGNORE_PROTOCOLS=OSMOSIS-OSMOSIS-USDC_AXELAR,osmosis-axlusdc
INITIAL_PROTOCOL=OSMOSIS-OSMOSIS-USDC-1
LPN_PRICE=1
LPNS=USDC,USDC_AXELAR,USDC_NOBLE
LPN_DECIMALS=6
SOCKET_RECONNECT_INTERVAL=5
COINGECKO_INFO_URL=https://pro-api.coingecko.com/api/v3/coins/$0?localization=false&market_data=false&tickers=false&developer_data=false&community_data=false&x_cg_pro_api_key=PRO_API_KEY
COINGECKO_PRICES_URL=https://pro-api.coingecko.com/api/v3/simple/price?ids=$0&vs_currencies=$1&x_cg_pro_api_key=PRO_API_KEY
COINGECKO_MARKET_DATA_RANGE_URL=https://pro-api.coingecko.com/api/v3/coins/$0/market_chart/range?vs_currency=$1&from=$2&to=$3&x_cg_pro_api_key=PRO_API_KEY
```

TESTNET CURRENCIES WHEN FEEDERS NOT RETURN CORRECT DATA

INSERT INTO "MP_Asset"
("MP_asset_symbol", "MP_asset_timestamp", "MP_price_in_stable", "Protocol")
VALUES
('OSMO', NOW(), 0, 'OSMOSIS'),
('USDC', NOW(), 0, 'OSMOSIS'),
('ATOM', NOW(), 0, 'OSMOSIS'),
('NLS', NOW(), 0, 'OSMOSIS'),
('AKT', NOW(), 0, 'OSMOSIS'),
('JUNO', NOW(), 0, 'OSMOSIS'),
('NTRN', NOW(), 0, 'NEUTRON'),
('NLS', NOW(), 0, 'NEUTRON'),
('USDC_AXELAR', NOW(), 0, 'NEUTRON'),
('ATOM', NOW(), 0, 'NEUTRON');

DO $FN$
BEGIN
FOR counter IN 1..6300000 LOOP
EXECUTE $$ INSERT INTO block(id) VALUES ($1) RETURNING id $$
USING counter;
END LOOP;
END;
$FN$;

GRANT ALL ON block TO nolus;
GRANT ALL ON "MP_Asset" TO nolus;

PATH: /lib/systemd/system/etl.service
sudo systemctl enable etl

### BUILD

```
cargo 1.75.0 or higher

cargo build --release
```

### DAEMON

```
[Unit]
Description=ETL
After=network.target

[Service]
Type=simple
Restart=always
User=root
RestartSec=10
ExecStart=/etl/target/release/etl

[Install]
WantedBy=multi-user.target
```

```
sudo -S systemctl daemon-reload
sudo -S systemctl enable etl
sudo systemctl start etl
```
