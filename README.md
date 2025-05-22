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

### DEV

cargo install cargo-watch
cargo watch -c -w src -x run


### Test config:

```
HOST=rpc.nolus.network
GRPC_HOST=https://grpc.nolus.network
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=32
SUPPORTED_CURRENCIES=(NLS,6),(USDC,6),(OSMO,6),(ST_OSMO,6),(ATOM,6),(ST_ATOM,6),(WETH,18),(WBTC,8),(AKT,6),(AXL,6),(JUNO,6),(EVMOS,18),(STK_ATOM,6),(SCRT,6),(CRO,8),(TIA,6),(STARS,6),(Q_ATOM,6),(NTRN,6),(USDC_AXELAR,6),(DYDX,18),(STRD,6),(INJ,18),(ST_TIA,6),(JKL,6),(MILK_TIA,6),(LVN,6),(QSR,6),(PICA,12),(DYM,18),(USDC_NOBLE,6),(CUDOS,18),(D_ATOM,6),(ALL_SOL,9),(ALL_BTC,8),(OM,6),(XION,6),(NIL,6),(ALL_ETH,18),(BABY,6),(D_NTRN,6)
MP_ASSET_INTERVAL_IN_SEC=20
CACHE_INTERVAL_IN_MINUTES=60
LP_POOLS=(nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6,USDC_NOBLE,long),(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,USDC,long),(nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94,USDC_AXELAR,long),(nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf,USDC_NOBLE,long),(nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990,ST_ATOM,short),(nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3,ALL_BTC,short),(nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm,ALL_SOL,short),(nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z,AKT,short)
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz
SERVER_HOST=127.0.0.1
PORT=8080
ALLOWED_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://app-dev.nolus.io,https://app.nolus.io
TIMEOUT=300
MAX_TASKS=128
ADMIN_CONTRACT=nolus1gurgpv8savnfw66lckwzn4zk7fp394lpe667dhu7aw48u40lj6jsqxf8nd
IGNORE_PROTOCOLS=OSMOSIS-OSMOSIS-ATOM,OSMOSIS-OSMOSIS-OSMO,OSMOSIS-OSMOSIS-INJ
INITIAL_PROTOCOL=OSMOSIS-OSMOSIS-USDC_AXELAR
SOCKET_RECONNECT_INTERVAL=5
EVENTS_SUBSCRIBE=deposit,burn,open_lease,repay,claim_rewards,close_position
ENABLE_SYNC=true
TASKS_INTERVAL=3000
```

HOST=pirin-cl.nolus.network:26657
GRPC_HOST=https://pirin-cl.nolus.network:9090

### Testnet config:

```
HOST=rila-cl.nolus.network:26657
GRPC_HOST=https://rila-cl.nolus.network:9090
DATABASE_URL=postgres://user_name:password@localhost:5432/database_name
SYNC_THREADS=32
SUPPORTED_CURRENCIES=(NLS,6),(USDC,6),(USDC_AXELAR,6),(OSMO,6),(ATOM,6),(AKT,6),(JUNO,6),(NTRN,6),(USDC_NOBLE,6)
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_SEC=20
CACHE_INTERVAL_IN_MINUTES=60
LP_POOLS=(nolus184vpz5tng9gy236uu4hf8gqq5rk0ylk27uds72cczz05q0vrwvvsy9mfkp,USDC_AXELAR,long),(nolus1urdpfxrj7m9r70mv5tdrlnmn02eta6ksaxak8ejsc7pshu83qlzsyqf004,USDC_AXELAR,long),(nolus1vmmhpakm6c93f80m3c2kpy220pvxp3ltw4s4p5m6kpha4cg86s2sehz7g2,OSMO,short)
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
EVENTS_SUBSCRIBE=deposit,burn,open_lease,repay,claim_rewards,close_position
ENABLE_SYNC=true
TASKS_INTERVAL=3000
```

TESTNET CURRENCIES WHEN FEEDERS NOT RETURN CORRECT DATA

INSERT INTO "MP_Asset"
("MP_asset_symbol", "MP_asset_timestamp", "MP_price_in_stable", "Protocol")
VALUES
('OSMO', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('USDC', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('ATOM', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('NLS', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('AKT', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('JUNO', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),
('USDC_AXELAR', NOW(), 0, 'OSMOSIS-OSMOSIS-OSMO'),

('OSMO', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
('USDC_AXELAR', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
('ATOM', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
('NLS', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
('AKT', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),
('JUNO', NOW(), 0, 'OSMOSIS-OSMOSIS-USDC_AXELAR'),

('NTRN', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('NLS', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('USDC_AXELAR', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('DYDX', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('ST_TIA', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('STK_ATOM', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL'),
('ATOM', NOW(), 0, 'NEUTRON-ASTROPORT-USDC_AXL');

DO $FN$
BEGIN
FOR counter IN 1..7335364 LOOP
EXECUTE $$ INSERT INTO block(id) VALUES ($1) RETURNING id $$
USING counter;
END LOOP;
END;
$FN$;

DO $FN$
BEGIN
FOR counter IN 6871801..6871800 LOOP
EXECUTE $$ 

INSERT INTO block(id)
SELECT $1
WHERE
NOT EXISTS (
SELECT id FROM block WHERE id = $1
);

$$
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

### ENDPOINTS

https://rpc.nolus.network
https://grpc.nolus.network
https://lcd.nolus.network
https://etl.nolus.network
https://archive-rpc.nolus.network
https://archive-grpc.nolus.network
https://archive-lcd.nolus.network

20.02.2025

bock: 10961047 - add { state: {due_projection_secs: due_projection_secs} } in lease query