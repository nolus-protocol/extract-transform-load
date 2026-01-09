# Nolus ETL API Endpoints Documentation

This document provides a comprehensive overview of all available API endpoints, their parameters, pagination options, and caching behavior.

---

## Protocol Analytics

Platform-wide metrics and aggregated statistics.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/total-value-locked` | Total value locked across all pools (USD) | None | None | 30 min |
| `/api/total-tx-value` | Cumulative transaction value (USD) | None | None | 30 min |
| `/api/revenue` | Total protocol revenue (USD) | None | None | 30 min |
| `/api/borrowed` | Total borrowed amount (USD) | None | None | 30 min |
| `/api/supplied-funds` | Total supplied by lenders (USD) | None | None | 30 min |
| `/api/blocks` | Latest synced block number | None | None | - |
| `/api/version` | API version | None | None | - |
| `/api/supplied-borrowed-history` | Time series of supplied vs borrowed | None | None | 1 hour |
| `/api/monthly-active-wallets` | Unique active wallets per month | None | None | 1 hour |
| `/api/revenue-series` | Daily and cumulative revenue over time | None | None | 1 hour |
| `/api/borrow-apr` | Historical borrow APR rates | `protocol`, `period`, `from`, `format` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/earn-apr` | Current earn APR for LPs | `protocol` | None | - |
| `/api/utilization-level` | Historical pool utilization levels | `protocol`, `period`, `from`, `format` | `period=12m` (3m/6m/12m/all) | 30 min |
| `/api/utilization-levels` | Current utilization for all pools | None | None | 30 min |
| `/api/optimal` | Optimal utilization rate threshold | None | None | - |
| `/api/deposit-suspension` | Deposit suspension threshold | None | None | - |
| `/api/buyback` | NLS token buyback transactions | `period`, `from`, `format` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/buyback-total` | Total buyback amount (USD) | None | None | 1 hour |
| `/api/distributed` | Total rewards distributed (USD) | None | None | 1 hour |
| `/api/incentives-pool` | Incentives pool balance (USD) | None | None | 1 hour |

---

## Position Analytics

Metrics related to trading positions and leases.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/open-position-value` | Total market value of open positions (USD) | None | None | 1 hour |
| `/api/open-interest` | Total outstanding debt (USD) | None | None | 1 hour |
| `/api/unrealized-pnl` | Aggregate unrealized PnL (USD) | None | None | 1 hour |
| `/api/realized-pnl-stats` | Aggregate realized PnL (USD) | None | None | 1 hour |
| `/api/leased-assets` | Leased assets grouped by token | None | None | 1 hour |
| `/api/loans-by-token` | Active loans grouped by token | None | None | 1 hour |
| `/api/open-positions-by-token` | Open positions by token with market values | None | None | 1 hour |
| `/api/position-buckets` | Position distribution by loan size buckets | None | None | 1 hour |
| `/api/positions` | All open positions with full details | `format` (json/csv) | None (returns all) | 1 hour |
| `/api/positions/export` | Streaming CSV export of all positions | None | Streaming CSV | 1 hour |
| `/api/leases-monthly` | Monthly lease count and volume | None | None | 1 hour |
| `/api/daily-positions` | Daily opened/closed position counts | None | None | 1 hour |

---

## Lending Analytics

Lending pool metrics, liquidations, and historical data.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/current-lenders` | Active lenders with deposit amounts | None | None | 1 hour |
| `/api/liquidations` | Liquidation events | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/liquidations/export` | Streaming CSV of all liquidations | None | Streaming CSV | 1 hour |
| `/api/lease-value-stats` | Min/max/avg/sum lease values per protocol | None | None | 1 hour |
| `/api/historical-lenders` | Lender deposit/withdrawal history | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/historical-lenders/export` | Streaming CSV of lender history | None | Streaming CSV | 1 hour |
| `/api/loans-granted` | Loans granted summary per protocol | None | None | 1 hour |
| `/api/historically-liquidated` | Historically liquidated positions | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/historically-liquidated/export` | Streaming CSV of liquidated positions | None | Streaming CSV | 1 hour |
| `/api/historically-repaid` | Historically repaid positions | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/historically-repaid/export` | Streaming CSV of repaid positions | None | Streaming CSV | 1 hour |
| `/api/historically-opened` | Historically opened positions | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/historically-opened/export` | Streaming CSV of opened positions | None | Streaming CSV | 1 hour |
| `/api/interest-repayments` | Interest repayment events | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/interest-repayments/export` | Streaming CSV of interest repayments | None | Streaming CSV | 1 hour |
| `/api/realized-pnl-wallet` | Realized PnL aggregated per wallet | `format`, `period`, `from` | `period=12m` (3m/6m/12m/all) | 1 hour |
| `/api/realized-pnl-wallet/export` | Streaming CSV of realized PnL by wallet | None | Streaming CSV | 1 hour |

---

## Market Data

Asset pricing information.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/prices` | Historical asset prices | `interval`, `key`, `protocol` | None | - |

---

## Wallet Analytics

User-specific data and transaction history.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/unrealized-pnl-by-address` | Unrealized PnL for a wallet | `address` **(required)** | None | - |
| `/api/pnl-over-time` | PnL progression over time | `address` **(required)**, `interval` | None | - |
| `/api/txs` | Transaction history for a wallet | `address` **(required)**, `filter`, `to` | `skip=0`, `limit=10` (max 100) | - |
| `/api/leases` | Leases for a wallet | `address` **(required)** | `skip=0`, `limit=10` (max 10) | - |
| `/api/ls-loan-closing` | Lease closing details with PnL | `address` **(required)** | `skip=0`, `limit=10` (max 10) | - |
| `/api/realized-pnl` | Total realized PnL for a wallet | `address` **(required)** | None | - |
| `/api/position-debt-value` | Position value and debt for a wallet | `address` **(required)** | None | - |
| `/api/realized-pnl-data` | Realized PnL breakdown per lease | `address` **(required)** | None | - |
| `/api/history-stats` | Trading statistics (trades, win rate, avg PnL) | `address` **(required)** | None | - |
| `/api/leases-search` | Search leases by wallet | `address` **(required)**, `search` | `skip=0`, `limit=10` (max 100) | - |
| `/api/earnings` | Lending earnings for a wallet | `address` **(required)** | None | - |

---

## Record Lookup

Single record retrieval by ID.

| Endpoint | Description | Params | Pagination | Cache |
|----------|-------------|--------|------------|-------|
| `/api/ls-opening` | Lease opening details by contract ID | `lease` **(required)** | None | - |
| `/api/ls-openings` | Multiple lease openings by IDs | `leases` (array) | None | - |
| `/api/lp-withdraw` | LP withdrawal details by tx hash | `tx` **(required)** | None | - |

---

## Push Notifications

WebPush notification subscription management.

| Endpoint | Method | Description | Params |
|----------|--------|-------------|--------|
| `/api/subscribe` | GET | Get subscription status | `address` **(required)** |
| `/api/subscribe` | POST | Create/update subscription | JSON body with subscription data |
| `/api/test-push` | POST | Send test notification | JSON body |

---

## Admin Endpoints

Authenticated administrative operations.

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/update/raw-txs` | POST | Bearer token | Update raw transactions |
| `/api/update/backfill-ls-opening` | POST | Bearer token | Backfill LS opening data |

---

## Reference

### Period Filter Options

Used by historical endpoints:

| Value | Description |
|-------|-------------|
| `3m` | Last 3 months |
| `6m` | Last 6 months |
| `12m` | Last 12 months **(default)** |
| `all` | All time (no limit) |

### From Parameter (Incremental Syncing)

The `from` parameter enables incremental data syncing by returning only records created after a specified timestamp. This is useful for clients that need to periodically fetch new data without re-downloading the entire dataset.

| Parameter | Type | Description |
|-----------|------|-------------|
| `from` | DateTime (ISO 8601) | Only return records after this timestamp (exclusive) |

**How it works:**
- The `from` parameter works in conjunction with `period` using AND logic
- Records are filtered where `timestamp > from` (exclusive)
- Pass the latest timestamp from your previous sync as the `from` value

**Example:**
```
GET /api/liquidations?period=12m&from=2025-01-15T14:30:00Z
```
This returns liquidations from the last 12 months that occurred after January 15, 2025 at 14:30 UTC.

**Endpoints using period filter:**
- `/api/buyback`
- `/api/borrow-apr`
- `/api/utilization-level`
- `/api/liquidations`
- `/api/historical-lenders`
- `/api/historically-liquidated`
- `/api/historically-repaid`
- `/api/historically-opened`
- `/api/interest-repayments`
- `/api/realized-pnl-wallet`

### Format Options

Used by endpoints supporting CSV export:

| Value | Description |
|-------|-------------|
| `json` | JSON response **(default)** |
| `csv` | CSV file download |

### Export Endpoints

All `/export` endpoints return streaming CSV with the complete dataset (no pagination). Use these for bulk data extraction:

- `/api/positions/export`
- `/api/liquidations/export`
- `/api/historical-lenders/export`
- `/api/historically-liquidated/export`
- `/api/historically-repaid/export`
- `/api/historically-opened/export`
- `/api/interest-repayments/export`
- `/api/realized-pnl-wallet/export`

### Pagination Types

| Type | Description | Endpoints |
|------|-------------|-----------|
| **period filter** | Time window filtering (3m/6m/12m/all) | All historical endpoints listed above |
| **skip/limit** | Traditional offset pagination | `txs`, `leases`, `ls-loan-closing`, `leases-search` |
| **None** | Returns full dataset | All other endpoints |

### Cache TTL Summary

| TTL | Use Case | Examples |
|-----|----------|----------|
| **30 min** | Real-time state, utilization | `borrowed`, `supplied-funds`, `utilization-levels`, `utilization-level` |
| **1 hour** | Historical data, positions, lending | `historically-opened`, `positions`, `borrow-apr`, `buyback`, `current-lenders`, `liquidations`, `historical-lenders`, `loans-granted`, `historically-repaid`, `realized-pnl-wallet`, `lease-value-stats` |

### Pagination Limits Summary (Wallet Endpoints)

| Endpoint | Default | Maximum |
|----------|---------|---------|
| `txs` | 10 | 100 |
| `leases` | 10 | 10 |
| `ls-loan-closing` | 10 | 10 |
| `leases-search` | 10 | 100 |

---

## Usage Examples

### Get buyback data for last 6 months
```
GET /api/buyback?period=6m
```

### Get borrow APR for a specific protocol (last 3 months)
```
GET /api/borrow-apr?protocol=OSMOSIS-OSMOSIS-USDC_NOBLE&period=3m
```

### Export all liquidations as CSV
```
GET /api/liquidations/export
```

### Get liquidations for last 12 months as CSV
```
GET /api/liquidations?period=12m&format=csv
```

### Get utilization level history for a protocol
```
GET /api/utilization-level?protocol=OSMOSIS-OSMOSIS-USDC_NOBLE&period=all
```

### Get interest repayments (all time)
```
GET /api/interest-repayments?period=all
```

---

*Generated: January 2025*
*ETL Version: 3.14.22*
