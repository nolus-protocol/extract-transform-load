# Extract Transform Load
Define Nolus data necessary for further analysis and implement an agent extracting it, transforming and loading into a relational SQL database.

## HOW TO

### PostgreSQL

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
SUPPORTED_CURRENCIES=(nolus,unls,NLS,6),(usd-coin,ibc/7FBDBEEEBA9C50C4BCDF7BF438EAB99E64360833D240B32655C96E319559E911,USDC,6),(osmosis,ibc/ED07A3391A112B175915CD8FAF43A2DA8E4790EDE12566649D0C2F97716B8518,OSMO,6),(stride-staked-osmo,ibc/AF5559D128329B6C753F15481BEC26E533B847A471074703FA4903E7E6F61BA1,ST_OSMO,6),(cosmos,ibc/6CDD4663F2F09CD62285E2D45891FC149A3568E316CE3EBBE201A71A78A69388,ATOM,6),(stride-staked-atom,ibc/FCFF8B19C61677F3B78E2A5AE3B4A34A8D23858D16905F253B8438B3AFD07FF8,ST_ATOM,6),(weth,ibc/A7C4A3FB19E88ABE60416125F9189DA680800F4CDD14E3C10C874E022BEFF04C,WETH,18),(wrapped-bitcoin,ibc/84E70F4A34FB2DE135FD3A04FDDF53B7DA4206080AA785C8BAB7F8B26299A221,WBTC,8),(akash-network,ibc/ADC63C00000CA75F909D2BE3ACB5A9980BED3A73B92746E0FCE6C67414055459,AKT,6),(axelar,ibc/1B03A71B8E6F6EF424411DC9326A8E0D25D096E4D2616425CFAF2AF06F0FE717,AXL,6),(juno-network,ibc/4F3E83AB35529435E4BFEA001F5D935E7250133347C4E1010A9C77149EF0394C,JUNO,6),(evmos,ibc/A59A9C955F1AB8B76671B00C1A0482C64A6590352944BB5880E5122358F7E1CE,EVMOS,18),(stkatom,ibc/DAAD372DB7DD45BBCFA4DDD40CA9793E9D265D1530083AB41A8A0C53C3EBE865,STK_ATOM,6),(secret,ibc/EA00FFF0335B07B5CD1530B7EB3D2C710620AE5B168C71AFF7B50532D690E107,SCRT,6),(crypto-com-chain,ibc/E1BCC0F7B932E654B1A930F72B76C0678D55095387E2A4D8F00E941A8F82EE48,CRO,8),(celestia,ibc/6C349F0EB135C5FA99301758F35B87DB88403D690E5E314AB080401FEE4066E5,TIA,6),(stargaze,ibc/11E3CF372E065ACB1A39C531A3C7E7E03F60B5D0653AD2139D31128ACD2772B5,STARS,6),(cosmos,ibc/317FCA2D7554F55BBCD0019AB36F7FEA18B6D161F462AF5E565068C719A29F20,Q_ATOM,6),(neutron-3,ibc/3D6BC6E049CAEB905AC97031A42800588C58FB471EBDC7A3530FFCD0C3DC9E09,NTRN,6),(usd-coin,ibc/076CF690A9912E0B7A2CCA75B719D68AF7C20E4B0B6460569B333DDEB19BBBA1,USDC_AXELAR,6),(dydx-chain,ibc/6DF8CF5C976851D152E2C7270B0AB25C4F9D64C0A46513A68D6CBB2662A98DF4,DYDX,18),(stride,ibc/04CA9067228BB51F1C39A506DA00DF07E1496D8308DD21E8EF66AD6169FA722B,STRD,6),(injective-protocol,ibc/4DE84C92C714009D07AFEA7350AB3EC383536BB0FAAD7AF9C0F1A0BEA169304E,INJ,18)
STABLE_CURRENCY=usd
AGGREGATION_INTTERVAL=1
MP_ASSET_INTERVAL_IN_MINUTES=1
CACHE_INTERVAL_IN_MINUTES=300
LP_POOLS=(nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5,USDC),(nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94,USDC_AXELAR)
NATIVE_CURRENCY=NLS
TREASURY_CONTRACT=nolus14hj2tavq8fpesdwxxcu44rty3hh90vhujrvcmstl4zr3txmfvw9s0k0puz
SERVER_HOST=127.0.0.1
PORT=8080
ALLOWED_ORIGINS=http://localhost:8080,http://127.0.0.1:8080
TIMEOUT=300
MAX_TASKS=64
ADMIN_CONTRACT=nolus1gurgpv8savnfw66lckwzn4zk7fp394lpe667dhu7aw48u40lj6jsqxf8nd
IGNORE_PROTOCOLS=
INITIAL_PROTOCOL=OSMOSIS
LPN_PRICE=1
LPNS=USDC,USDC_AXELAR
LPN_DECIMALS=6
```

### Testnet config:
```
HOST=pirin-cl.nolus.network:26657
SUPPORTED_CURRENCIES=(nolus,unls,NLS,6,OSMOSIS),(usd-coin,ibc/7DABB27AEEAFC0576967D342F21DC0944F5EA6584B45B9C635A3B3C35DCDA159,USDC_AXELAR,6,OSMOSIS),(usd-coin,ibc/88E889952D6F30CEFCE1B1EE4089DA54939DE44B0A7F11558C230209AF228937,USDC_AXELAR,6,NEUTRON),(osmosis,ibc/0A9CB406B20A767719CDA5C36D3F9939C529B96D122E7B42C09B9BA1F8E84298,OSMO,6,OSMOSIS),(cosmos,ibc/CFAC783D503ABF2BD3C9BB1D2AC6CD6136192782EE936D9BE406977F6D133926,ATOM,6,OSMOSIS),(akash-network,ibc/E3477DEE69A2AFF7A1665C2961C210132DD50954EF0AE171086189257FFC844F,AKT,6,OSMOSIS),(juno-network,ibc/BEDEB6912C720F66B74F44620EA7A5C415E5BD0E78198ACEBF667D5974761835,JUNO,6,OSMOSIS),(neutron,ibc/712F900E327780AAB33B9204DB5257FB1D6FACCF9CD7B70A0EFB31ED4C1255C4,NTRN,6,NEUTRON)
```

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
