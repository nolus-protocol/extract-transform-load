# Nolus ETL Service

Rust-based ETL (Extract-Transform-Load) service for the Nolus blockchain. Extracts blockchain data (transactions, events, smart contract states), transforms it, and loads it into PostgreSQL. Provides a REST API for querying the data.

## Features

- Real-time blockchain data synchronization via WebSocket
- Historical block backfilling
- Market price aggregation from oracles
- Push notifications for position alerts
- REST API with 70+ endpoints
- CSV export support
- Response caching with configurable TTLs

## Requirements

- Rust 1.80.0+ (Edition 2021)
- PostgreSQL 14+

## Quick Start

### 1. Database Setup

```bash
# macOS
psql -U $USER postgres

# Linux
sudo -i -u postgres && psql
```

```sql
CREATE DATABASE etl_nolus;
GRANT ALL PRIVILEGES ON DATABASE etl_nolus TO your_user;
GRANT ALL ON SCHEMA public TO your_user;
```

### 2. Configuration

```bash
cp .env.example .env
# Edit .env with your settings
```

Key configuration options:
- `DATABASE_URL` - PostgreSQL connection string
- `HOST` - RPC endpoint (e.g., `rpc.nolus.network`)
- `GRPC_HOST` - gRPC endpoint (e.g., `https://grpc.nolus.network`)
- `SERVER_HOST` / `PORT` - API server bind address

### 3. Build & Run

```bash
# Development (with hot reload)
cargo install cargo-watch
cargo watch -c -w src -x run

# Production build
cargo build --release
./target/release/etl
```

## CLI Commands

The ETL binary supports multiple modes:

```bash
# Run the server (default)
./etl
./etl serve

# Run database migrations only
./etl migrate
./etl migrate --status

# Run data backfills
./etl backfill ls-opening --all --batch-size=1000
./etl backfill raw-txs --concurrency=20

# Preview without changes
./etl backfill ls-opening --dry-run
./etl backfill raw-txs --dry-run

# Help
./etl --help
./etl backfill --help
```

## Project Structure

```
src/
├── controller/     # REST API endpoints (consolidated by domain)
│   ├── treasury.rs     # Revenue, buyback, distributed, incentives
│   ├── metrics.rs      # TVL, borrowed, supplied, open interest
│   ├── pnl.rs          # Realized/unrealized PnL
│   ├── leases.rs       # Leases, loans, liquidations
│   ├── positions.rs    # Positions, buckets, daily stats
│   ├── liquidity.rs    # Pools, lenders, utilization
│   ├── misc.rs         # Prices, blocks, subscriptions
│   └── admin.rs        # Protected admin operations
├── handler/        # Business logic & event handlers
├── provider/       # External integrations (gRPC, WebSocket, DB)
├── dao/            # Database access objects
├── model/          # Data structures
└── types/          # Event types & API structures
```

## API Endpoints

### Treasury
- `GET /api/revenue` - Total protocol revenue
- `GET /api/distributed` - Total distributed rewards
- `GET /api/buyback` - Buyback history (supports `?period=3m|6m|12m|all`)
- `GET /api/buyback-total` - Total buyback amount
- `GET /api/incentives-pool` - Incentives pool balance
- `GET /api/earnings?address=` - Earnings by address

### Metrics
- `GET /api/total-value-locked` - Platform TVL
- `GET /api/supplied-funds` - Total supplied funds
- `GET /api/borrowed` - Total borrowed (optional `?protocol=`)
- `GET /api/open-interest` - Open interest value
- `GET /api/supplied-borrowed-history` - Historical series

### Positions & Leases
- `GET /api/positions` - All open positions
- `GET /api/leases?address=` - Leases by address
- `GET /api/liquidations` - Liquidation history
- `GET /api/historically-opened` - Historical openings

### Liquidity Pools
- `GET /api/pools` - All pools with utilization & APR
- `GET /api/utilization-level?protocol=` - Pool utilization history
- `GET /api/current-lenders` - Active lenders
- `GET /api/historical-lenders` - Lender history

### Export & Filtering
Most list endpoints support:
- `?format=csv` - CSV format response
- `?period=3m|6m|12m|all` - Time window filter
- `?from=<timestamp>` - Incremental sync filter
- `?export=true` - Streaming CSV export (full data)

## Deployment

### Systemd Service

```ini
# /lib/systemd/system/etl.service
[Unit]
Description=Nolus ETL Service
After=network.target

[Service]
Type=simple
Restart=always
User=root
RestartSec=10
ExecStart=/path/to/etl

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable etl
sudo systemctl start etl
journalctl -u etl -f  # View logs
```

## Network Endpoints

| Network | RPC | gRPC | ETL |
|---------|-----|------|-----|
| Mainnet | rpc.nolus.network | grpc.nolus.network | etl.nolus.network |
| Archive | archive-rpc.nolus.network | archive-grpc.nolus.network | - |
| Testnet | rila-cl.nolus.network:26657 | rila-cl.nolus.network:9090 | - |

## Database Migrations

Migrations use [refinery](https://github.com/rust-db/refinery) and run automatically on startup.

- Versioned SQL files in `migrations/` directory
- Migration state tracked in `refinery_schema_history` table
- Run `./etl migrate --status` to check applied migrations

## Documentation

- `API_MIGRATION_GUIDE.md` - Frontend migration guide for API changes
- `entities.md` - Database schema documentation

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
