# Extract Transform Load
Define Nolus data necessary for further analysis and implement an agent extracting it, transforming and loading into a relational SQL database.

## HOW TO

### PostgreSQL/Mysql

1. CREATE DATABASE database_name;
2. GRANT ALL PRIVILEGES ON DATABASE database_name to user_name;
3. Copy .env.example to .env and set necessary settings

### Test config:
```
HOST=net-dev.nolus.io:26622
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=16
SUPPORTED_CURRENCIES=(binance-smart-chain,0x0eb3a705fc54725037cc9e008bdede697f62f335,atom,18),(cosmos,ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858,usdc,18)
STABLE_CURRENCY=usd
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
LP_POOLS=(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,usdc)
NATIVE_CURRENCY=atom
TREASURY_CONTRACT=nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5
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
ExecStart=/home/alexanderm/Projects/rust/etl/target/release/etl

[Install]
WantedBy=multi-user.target
```