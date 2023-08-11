# Extract Transform Load
Define Nolus data necessary for further analysis and implement an agent extracting it, transforming and loading into a relational SQL database.

## HOW TO

### PostgreSQL/Mysql

sudo -i -u postgres
psql

1. CREATE DATABASE database_name;
2. GRANT ALL PRIVILEGES ON DATABASE database_name to user_name;
3. Copy .env.example to .env and set necessary settings

### Test config:
```
HOST=pirin-cl.nolus.network:26657
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=16
SUPPORTED_CURRENCIES=(nolus,unls,NLS,6),(usd-coin,ibc/7FBDBEEEBA9C50C4BCDF7BF438EAB99E64360833D240B32655C96E319559E911,USDC,6),(osmosis,ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518,OSMO,6),(stride-staked-osmo,ibc/AF5559D128329B6C753F15481BEC26E533B847A471074703FA4903E7E6F61BA1,ST_OSMO,6),(cosmos,ibc/6CDD4663F2F09CD62285E2D45891FC149A3568E316CE3EBBE201A71A78A69388,ATOM,6),(stride-staked-atom,ibc/FCFF8B19C61677F3B78E2A5AE3B4A34A8D23858D16905F253B8438B3AFD07FF8,ST_ATOM,6),(weth,ibc/A7C4A3FB19E88ABE60416125F9189DA680800F4CDD14E3C10C874E022BEFF04C,WETH,18),(wrapped-bitcoin,ibc/84E70F4A34FB2DE135FD3A04FDDF53B7DA4206080AA785C8BAB7F8B26299A221,WBTC,8),(akash-network,ibc/ADC63C00000CA75F909D2BE3ACB5A9980BED3A73B92746E0FCE6C67414055459,AKT,6)
STABLE_CURRENCY=usd
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
LP_POOLS=(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,USDC)
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz
```

PATH: /lib/systemd/system/etl.service
sudo systemctl enable etl

### BUILD

```
cargo 1.66.0 or higher

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
