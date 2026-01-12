# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rust-based ETL (Extract-Transform-Load) service for the Nolus blockchain. Extracts blockchain data (transactions, events, smart contract states), transforms it, and loads it into PostgreSQL. Runs as a daemon that continuously syncs blockchain data and provides a REST API.

## Build and Run Commands

```bash
# Requires Rust 1.79.0+ (Edition 2021)

# Development (with hot reload)
cargo install cargo-watch
cargo watch -c -w src -x run

# Production build
cargo build --release

# Testing
cargo test                  # All tests
cargo test <test_name>      # Specific test
cargo test -- --nocapture   # Show println! output

# Check/lint/format
cargo check
cargo clippy
cargo fmt
```

## CLI Commands

The ETL binary supports multiple modes via CLI subcommands:

```bash
# Run the server (default)
./etl
./etl serve

# Run migrations only (useful in CI/CD)
./etl migrate
./etl migrate --status    # Check migration status

# Backfill operations
./etl backfill ls-opening --dry-run           # Preview changes
./etl backfill ls-opening --batch-size=1000   # Process in batches
./etl backfill ls-opening --all               # Process all records

./etl backfill raw-txs --dry-run              # Preview changes
./etl backfill raw-txs --concurrency=20       # Set concurrent tasks

# Help
./etl --help
./etl backfill --help
./etl backfill ls-opening --help
```

## Database Setup

```bash
# PostgreSQL (macOS: psql -U $USER postgres, Linux: sudo -i -u postgres && psql)
CREATE DATABASE database_name;
GRANT ALL PRIVILEGES ON DATABASE database_name to user_name;
GRANT ALL ON SCHEMA public TO user_name;
```

Copy `.env.example` to `.env` and configure settings.

**Database Migrations** (using [refinery](https://github.com/rust-db/refinery)):
- Versioned SQL migrations in `migrations/` directory (currently V001-V005)
- Migrations run automatically on startup before the connection pool is created
- Migration state tracked in `refinery_schema_history` table
- Checksum validation prevents modifying already-applied migrations

**Database pool settings** (configurable in `.env`, optimized for PgBouncer):
- `DB_MAX_CONNECTIONS` (default: 5), `DB_MIN_CONNECTIONS` (default: 1)
- `DB_ACQUIRE_TIMEOUT` (default: 30s), `DB_IDLE_TIMEOUT` (default: 300s)
- `DB_STATEMENT_TIMEOUT` (default: 60s) - kills queries exceeding this limit

**Required config files**: `.env` (main config) and `etl.conf` (WebSocket and push notification settings).

## Architecture

### Three-Layer Structure

```
src/
├── provider/       # External integrations (Layer 1)
│   ├── event.rs           # WebSocket client for NewBlock events
│   ├── grpc.rs            # gRPC client for blockchain/contract queries
│   ├── http.rs            # HTTP client for market data and push notifications
│   ├── database.rs        # PostgreSQL connection pool
│   └── synchronization.rs # Historical block sync
├── handler/        # Business logic (Layer 2)
│   ├── wasm_*.rs          # Event handlers (ls_open, lp_deposit, etc.)
│   ├── *_state.rs         # State aggregation (ls_state, lp_pool_state, etc.)
│   ├── aggregation_task.rs# Periodic aggregation scheduler
│   ├── mp_assets.rs       # Market price fetching
│   ├── cache_refresher.rs # Proactive cache refresh
│   └── send_push.rs       # Push notification delivery
├── controller/     # REST API endpoints (Layer 3) - consolidated by domain
│   ├── treasury.rs        # Revenue, buyback, distributed, incentives
│   ├── metrics.rs         # TVL, borrowed, supplied, open interest
│   ├── pnl.rs             # Realized/unrealized PnL endpoints
│   ├── leases.rs          # Leases, loans, liquidations, historical data
│   ├── positions.rs       # Positions, buckets, daily positions
│   ├── liquidity.rs       # Pools, lenders, utilization
│   ├── misc.rs            # Prices, blocks, txs, subscriptions
│   └── admin.rs           # Protected backfill/update operations
├── helpers.rs      # Event parsing, dispatch (parse_event routes to handlers), EventsType enum
├── configuration.rs# App config, State, and AppState definitions
├── dao/            # Database access objects (one file per table)
├── model/          # Data structures - consolidated into models.rs
│   ├── models.rs          # All domain models (lease, liquidity, treasury, etc.)
│   ├── raw_message.rs     # Raw blockchain message types
│   └── table.rs           # Generic table wrapper
└── types/          # Blockchain event types - consolidated into common.rs
    ├── common.rs          # All event types and API structures
    └── push.rs            # Push notification types
```

### Data Flow

1. **Real-time**: WebSocket receives NewBlock → `helpers.rs::insert_txs()` parses transactions → `parse_event()` dispatches to `wasm_*.rs` handlers → DB insert
2. **Historical Sync**: `synchronization.rs` backfills missing blocks by detecting gaps in `block` table
3. **Aggregation**: `aggregation_task.rs` runs at `AGGREGATION_INTTERVAL` hours (note: config key has typo) to compute `*_State` tables
4. **Market Data**: `mp_assets.rs` fetches prices from oracle every `MP_ASSET_INTERVAL_IN_SEC` seconds

### Event Dispatch Pattern

Events are routed through `helpers.rs::parse_event()` using the `EventsType` enum (defined at `helpers.rs:719`):
- `EventsType::from_str()` parses event type strings (e.g., "wasm-ls-open")
- `EventsType::as_str()` provides canonical string representation (single source of truth)
- Each variant maps to a handler in `src/handler/wasm_*.rs`

Current event types: `LS_Opening`, `LS_Closing`, `LS_Close_Position`, `LS_Repay`, `LS_Liquidation`, `LS_Liquidation_Warning`, `LS_Slippage_Anomaly`, `LS_Auto_Close_Position`, `Reserve_Cover_Loss`, `LP_deposit`, `LP_Withdraw`, `TR_Profit`, `TR_Rewards_Distribution`

### Key Database Tables

- **LS_*** - Lease transactions and state (Opening, Closing, Repayment, Liquidation, State)
- **LP_*** - Liquidity provider deposits/withdrawals and state
- **MP_Asset** - Market prices from oracles
- **TR_*** - Treasury transactions and state
- **PL_State** - Platform-wide aggregated statistics
- **raw_message** - All blockchain transactions (with optional rewards field)
- **block** - Synced block IDs (gaps trigger historical sync)

### Concurrency

- `GRPC_CONNECTIONS` / `GRPC_PERMITS` - gRPC connection pool with semaphore rate limiting (timeout on permit acquisition)
- `SYNC_THREADS` - Parallel historical sync workers
- `push_permits` - Semaphore limiting concurrent push notification tasks (MAX_PUSH_TASKS = 100)
- Tokio tasks: WebSocket subscription, sync, market data, aggregation, cache, HTTP server (Actix-web)

### API Response Caching

The `ApiCache` struct in `configuration.rs` provides TTL-based caching for expensive API endpoints using `TimedCache<T>` from `src/cache.rs`:
- **CACHE_TTL_CURRENT (5 min)**: Real-time data (TVL, revenue, liquidations)
- **CACHE_TTL_STANDARD (30 min)**: Historical aggregates (daily positions, loans by token)
- **CACHE_TTL_LONG (1 hour)**: Stable time series (supplied/borrowed history, monthly data)

Pattern for cached endpoints:
```rust
// Check cache first
if let Some(cached) = app_state.api_cache.endpoint_name.get("cache_key").await {
    return Ok(HttpResponse::Ok().json(cached));
}
// Fetch from DB, then cache
let data = fetch_from_database().await?;
app_state.api_cache.endpoint_name.set("cache_key", data.clone()).await;
```

## Smart Contracts

Interacts with Nolus contracts via gRPC CosmWasm queries:
- **Admin** (`ADMIN_CONTRACT`) - Central config, returns all contract addresses
- **Treasury** (`TREASURY_CONTRACT`) - Protocol fees and buyback
- **LPP** (from `LP_POOLS`) - Liquidity pools
- **Leaser/Oracle/Profit/Rewards** - Queried via Admin contract

## Key Implementation Details

### Currency Handling
- Format: `(symbol, decimals, ibc_denom)` tuples in `SUPPORTED_CURRENCIES`
- Amounts stored in smallest unit (e.g., uNLS)
- `in_stabe()` / `in_stabe_by_date()` functions convert to stable currency using market prices

### Event Subscription
- `EVENTS_SUBSCRIBE` config lists event types to capture
- Events filtered by `wasm-` prefix, dispatched by type in `helpers.rs::parse_event()`

### Error Handling
- Custom `Error` enum in `src/error.rs`
- Retry logic in `grpc.rs::retry()` for transient failures
- WebSocket auto-reconnects on disconnect

### Handler Utilities
- `handler::parse_event_timestamp()` - Converts nanosecond timestamp strings to `DateTime<Utc>`

## Common Tasks

### Adding a Database Migration
1. Create a new file in `migrations/` with format `V{NNN}__{description}.sql` (next: `V006__description.sql`)
2. Use `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for idempotency
3. Migration will auto-run on next startup
4. For existing databases, use `./etl migrate --fake` to mark migrations as applied without running them

### Adding a New Event Handler
1. Define event struct in `src/types/`
2. Create handler in `src/handler/wasm_*.rs` with `parse_and_insert()` function
3. Add migration SQL in `migrations/` directory (see "Adding a Database Migration")
4. Add variant to `EventsType` enum in `helpers.rs` (update `as_str()` and `from_str()`)
5. Add match arm in `helpers.rs::parse_event()`
6. Update `entities.md`

### Adding a New API Endpoint
1. Add the endpoint function to the appropriate domain controller in `src/controller/`:
   - `treasury.rs` - Revenue, buyback, incentives endpoints
   - `metrics.rs` - TVL, borrowed, supplied, open interest endpoints
   - `pnl.rs` - Realized/unrealized PnL endpoints
   - `leases.rs` - Lease-related endpoints (including liquidations, repayments)
   - `positions.rs` - Position-related endpoints
   - `liquidity.rs` - Pool and lender endpoints
   - `misc.rs` - Utility endpoints (prices, blocks, version, etc.)
   - `admin.rs` - Protected admin/backfill endpoints
2. Use `#[get("/endpoint")]` or `#[post("/endpoint")]` attribute on the function
3. Register the route in `src/server.rs` under the `/api` scope (e.g., `.service(treasury::new_endpoint)`)
4. Implement DB query in `src/dao/` or model
5. For expensive queries, add a `TimedCache<ResponseType>` field to `ApiCache` in `configuration.rs`

### Debugging Sync Issues
- Check `block` table for gaps: `SELECT id FROM block ORDER BY id` - missing IDs trigger sync
- Check `action_history` for last aggregation timestamp
- Review logs for gRPC/WebSocket errors
- Check `refinery_schema_history` for migration status

## Important Files

- `entities.md` - Complete data model specification with all table schemas
- `migrations/*.sql` - Versioned database migrations (V001-V005, auto-applied on startup)
- `src/cli.rs` - CLI argument parsing (serve, migrate, backfill commands)
- `src/migration.rs` - Migration runner using refinery
- `.env.example` - Required configuration template with mainnet defaults
- `etl.conf` - Additional ETL-specific configuration (WEBSOCKET_HOST, push notification settings)
- `src/cache.rs` - Generic `TimedCache<T>` implementation for API response caching
- `src/configuration.rs` - `Config`, `State`, `AppState`, and `ApiCache` definitions
- `src/helpers.rs` - Event parsing, dispatch, `EventsType` enum (single source of truth for event routing)
- `cert/` - VAPID keys for push notifications (vapid_private.pem, vapid_public.b64)

## Systemd Deployment

```bash
# Service file at /lib/systemd/system/etl.service
sudo systemctl enable etl
sudo systemctl start etl
journalctl -u etl -f  # View logs
```

## Network Endpoints

- Mainnet: `rpc.nolus.network`, `grpc.nolus.network`, `etl.nolus.network`
- Archive: `archive-rpc.nolus.network`, `archive-grpc.nolus.network`
- Testnet: `rila-cl.nolus.network:26657`, `rila-cl.nolus.network:9090`

## Main Entry Point

The application (`src/main.rs`) spawns 5 concurrent Tokio tasks via `tokio::try_join!`:
1. `event_manager.run()` - WebSocket subscription for NewBlock events
2. `mp_assets_task()` - Market price fetching every `MP_ASSET_INTERVAL_IN_SEC` seconds
3. `start_aggregation_tasks()` - Hourly state aggregation
4. `server_task()` - Actix HTTP server
5. `cache_refresher::cache_refresh_task()` - Proactive cache refresh

## Handler Pattern

Event handlers in `src/handler/wasm_*.rs` follow a consistent pattern:
```rust
pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: EventType,           // Parsed event from blockchain
    tx_hash: String,
    height: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    // 1. Parse timestamps with parse_event_timestamp()
    // 2. Fetch prices/state via tokio::try_join! for parallel queries
    // 3. Calculate derived fields
    // 4. Insert via DAO's insert_if_not_exists()
}
```

## DAO Pattern

Database access objects in `src/dao/postgre/` use SQLx with:
- `insert_if_not_exists()` - Prevents duplicates using ON CONFLICT
- Parameterized queries with `sqlx::query!` or `sqlx::query_as!`
- Transaction support for multi-step operations
- Large DAOs: `ls_opening.rs` (93KB), `ls_state.rs` (49KB), `lp_pool_state.rs` (38KB)
