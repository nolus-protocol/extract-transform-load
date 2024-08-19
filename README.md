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

### Test config:

```
HOST=pirin-cl-arc.nolus.network:26612
GRPC_HOST=https://pirin-cl-arc.nolus.network:26615
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=32
SUPPORTED_CURRENCIES=(NLS,6),(USDC,6),(OSMO,6),(ST_OSMO,6),(ATOM,6),(ST_ATOM,6),(WETH,18),(WBTC,8),(AKT,6),(AXL,6),(JUNO,6),(EVMOS,18),(STK_ATOM,6),(SCRT,6),(CRO,8),(TIA,6),(STARS,6),(Q_ATOM,6),(NTRN,6),(USDC_AXELAR,6),(DYDX,18),(STRD,6),(INJ,18),(ST_TIA,6),(JKL,6),(MILK_TIA,6),(LVN,6),(QSR,6),(PICA,12),(DYM,18),(USDC_NOBLE,6),(CUDOS,18)
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
MAX_TASKS=128
ADMIN_CONTRACT=nolus1gurgpv8savnfw66lckwzn4zk7fp394lpe667dhu7aw48u40lj6jsqxf8nd
IGNORE_PROTOCOLS=
INITIAL_PROTOCOL=OSMOSIS-OSMOSIS-USDC_AXELAR
SOCKET_RECONNECT_INTERVAL=5
EVENTS_SUBSCRIBE=deposit,burn,open_lease,repay
```

### Testnet config:

```
HOST=rila-cl.nolus.network:26657
GRPC_HOST=https://rila-cl.nolus.network:9090
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=32
SUPPORTED_CURRENCIES=(NLS,6),(USDC,6),(USDC_AXELAR,6),(OSMO,6),(ATOM,6),(AKT,6),(JUNO,6),(NTRN,6),(USDC_NOBLE,6)
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
CACHE_INTERVAL_IN_MINUTES=60
LP_POOLS=(nolus184vpz5tng9gy236uu4hf8gqq5rk0ylk27uds72cczz05q0vrwvvsy9mfkp,USDC),(nolus1urdpfxrj7m9r70mv5tdrlnmn02eta6ksaxak8ejsc7pshu83qlzsyqf004,USDC_AXELAR),(nolus1k58ffjs4cnymw9xs8cgk66q6dxssd0epw6zzp9p5lylehmfys6tqjqfm7k,OSMO)
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus1nc5tatafv6eyq7llkr2gv50ff9e22mnf70qgjlv737ktmt4eswrqrr2r7y
SERVER_HOST=127.0.0.1
PORT=8080
ALLOWED_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://app-dev.nolus.io
TIMEOUT=300
MAX_TASKS=64
ADMIN_CONTRACT=nolus17p9rzwnnfxcjp32un9ug7yhhzgtkhvl9jfksztgw5uh69wac2pgsmc5xhq
IGNORE_PROTOCOLS=
INITIAL_PROTOCOL=OSMOSIS-OSMOSIS-USDC_AXELAR
SOCKET_RECONNECT_INTERVAL=5
EVENTS_SUBSCRIBE=deposit,burn,open_lease,repay
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
FOR counter IN 1..722850 LOOP
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
cargo 1.80.0 or higher

cargo build --release

### TESTNET

cargo build --features testnet --no-default-features --release
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

CREATE INDEX raw_message_from ON raw_message("from", "timestamp");
CREATE INDEX raw_message_to ON raw_message("to", "timestamp");
