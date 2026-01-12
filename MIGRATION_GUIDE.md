# ETL Database Migration Guide

This guide covers the last three major database updates and their associated backfill endpoints.

---

## Table of Contents

1. [Update 1: Transaction Code Field](#update-1-transaction-code-field-01012025)
2. [Update 2: Pool Config Table](#update-2-pool-config-table)
3. [Update 3: Protocol Column & Endpoint Consolidation](#update-3-protocol-column--endpoint-consolidation-12012026)
4. [Backfill Endpoints](#backfill-endpoints)
5. [Complete Migration Script](#complete-migration-script)
6. [API Changes Summary](#api-changes-summary)

---

## Update 1: Transaction Code Field (01.01.2025)

### Purpose
Add a `code` field to the `raw_message` table to track transaction success/failure status.

### Database Changes

```sql
ALTER TABLE "raw_message" ADD COLUMN "code" INT;
```

### Backfill Required
Yes - use the `/update/raw-txs` endpoint to populate the `code` field for existing transactions.

### Backfill Endpoint

```
GET /update/raw-txs?auth=<AUTH_TOKEN>
```

**Description:** Fetches transaction codes from the blockchain via gRPC and updates the `code` column for all existing `raw_message` records.

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `auth` | String | Yes | Authentication token (from `.env` AUTH variable) |

**Response:**
```json
{
  "result": true
}
```

**Notes:**
- This endpoint processes all transactions without a code, so it may take a while
- Progress is controlled by `MAX_TASKS` in `.env`
- Safe to run multiple times - will only update records missing the code

---

## Update 2: Pool Config Table

### Purpose
Replace hardcoded pool CTEs in SQL queries with a reference table for better maintainability and query performance.

### Database Changes

```sql
-- Create the pool_config table
CREATE TABLE IF NOT EXISTS "pool_config" (
    "pool_id" VARCHAR(128) PRIMARY KEY,
    "position_type" VARCHAR(10) NOT NULL,
    "lpn_symbol" VARCHAR(20) NOT NULL,
    "lpn_decimals" BIGINT NOT NULL,
    "label" VARCHAR(50) NOT NULL
);

-- Insert pool configurations
INSERT INTO "pool_config" ("pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label") VALUES
    ('nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE'),
    ('nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5', 'Long', 'USDC', 1000000, 'USDC'),
    ('nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94', 'Long', 'USDC_AXELAR', 1000000, 'USDC_AXELAR'),
    ('nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE (Neutron)'),
    ('nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'Short', 'ST_ATOM', 1000000, 'ST_ATOM (Short)'),
    ('nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short', 'ALL_BTC', 100000000, 'ALL_BTC (Short)'),
    ('nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short', 'ALL_SOL', 1000000000, 'ALL_SOL (Short)'),
    ('nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short', 'AKT', 1000000, 'AKT (Short)'),
    ('nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short', 'ATOM', 1000000, 'ATOM (Short)'),
    ('nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short', 'OSMO', 1000000, 'OSMO (Short)')
ON CONFLICT ("pool_id") DO UPDATE SET
    "position_type" = EXCLUDED."position_type",
    "lpn_symbol" = EXCLUDED."lpn_symbol",
    "lpn_decimals" = EXCLUDED."lpn_decimals",
    "label" = EXCLUDED."label";
```

### Backfill Required
Yes - use the `/update/backfill-ls-opening` endpoint to populate pre-computed columns in `LS_Opening`.

### Backfill Endpoint

```
GET /update/backfill-ls-opening?auth=<AUTH_TOKEN>&batch_size=500
```

**Description:** Populates pre-computed columns in `LS_Opening` table for historical data:
- `LS_position_type` (from pool_config)
- `LS_lpn_symbol` (from pool_config)
- `LS_lpn_decimals` (from pool_config)
- `LS_opening_price` (from MP_Asset historical prices)
- `LS_liquidation_price_at_open` (calculated from position type and prices)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `auth` | String | Yes | - | Authentication token (from `.env` AUTH variable) |
| `batch_size` | Integer | No | 500 | Number of records to process per call |

**Response:**
```json
{
  "success": true,
  "step1_updated": 100,
  "step2_updated": 500,
  "step3_updated": 450,
  "remaining": 2500,
  "message": "Backfill in progress. 2500 rows remaining. Call again to continue."
}
```

**Backfill Process:**
1. **Step 1:** Populate pool_config data (position_type, lpn_symbol, lpn_decimals) - fast
2. **Step 2:** Populate opening prices in batches - slower, controlled by `batch_size`
3. **Step 3:** Calculate liquidation prices - fast, runs after step 2

**Usage:**
```bash
# Run repeatedly until "remaining" is 0
while true; do
  response=$(curl -s "https://etl.nolus.network/update/backfill-ls-opening?auth=YOUR_AUTH_TOKEN&batch_size=1000")
  remaining=$(echo $response | jq '.remaining')
  echo "Remaining: $remaining"
  if [ "$remaining" -eq 0 ]; then
    echo "Backfill complete!"
    break
  fi
  sleep 1
done
```

---

## Update 3: Protocol Column & Endpoint Consolidation (12.01.2026)

### Purpose
- Add `protocol` column to `pool_config` for the new `/api/pools` endpoint
- Merge `/api/borrow-apr` data into the pools endpoint
- Rename `/api/utilization-levels` to `/api/pools`

### Database Changes

```sql
-- Add protocol column to pool_config
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "protocol" VARCHAR(50);

-- Update pool_config with protocol names
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-USDC_NOBLE' WHERE "pool_id" = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-USDC_AXELAR' WHERE "pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5';
UPDATE "pool_config" SET "protocol" = 'NEUTRON-ASTROPORT-USDC_AXELAR' WHERE "pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94';
UPDATE "pool_config" SET "protocol" = 'NEUTRON-ASTROPORT-USDC_NOBLE' WHERE "pool_id" = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ST_ATOM' WHERE "pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ALL_BTC' WHERE "pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ALL_SOL' WHERE "pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-AKT' WHERE "pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ATOM' WHERE "pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-OSMO' WHERE "pool_id" = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t';
```

### Backfill Required
No - the UPDATE statements populate all existing data.

---

## Backfill Endpoints

### Summary Table

| Endpoint | Purpose | Auth Required | Batch Support |
|----------|---------|---------------|---------------|
| `/update/raw-txs` | Populate transaction `code` field | Yes | No (uses MAX_TASKS) |
| `/update/backfill-ls-opening` | Populate LS_Opening pre-computed columns | Yes | Yes (`batch_size`) |

### Authentication
Both endpoints require the `auth` parameter matching the `AUTH` variable in `.env`.

---

## Complete Migration Script

Run these commands on the production database in order:

```bash
# Connect to PostgreSQL
psql -U <username> -d <database>
```

```sql
-- ============================================
-- UPDATE 1: Transaction Code Field (01.01.2025)
-- ============================================
ALTER TABLE "raw_message" ADD COLUMN "code" INT;


-- ============================================
-- UPDATE 2: Pool Config Table
-- ============================================
CREATE TABLE IF NOT EXISTS "pool_config" (
    "pool_id" VARCHAR(128) PRIMARY KEY,
    "position_type" VARCHAR(10) NOT NULL,
    "lpn_symbol" VARCHAR(20) NOT NULL,
    "lpn_decimals" BIGINT NOT NULL,
    "label" VARCHAR(50) NOT NULL
);

INSERT INTO "pool_config" ("pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label") VALUES
    ('nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE'),
    ('nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5', 'Long', 'USDC', 1000000, 'USDC'),
    ('nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94', 'Long', 'USDC_AXELAR', 1000000, 'USDC_AXELAR'),
    ('nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf', 'Long', 'USDC_NOBLE', 1000000, 'USDC_NOBLE (Neutron)'),
    ('nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'Short', 'ST_ATOM', 1000000, 'ST_ATOM (Short)'),
    ('nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short', 'ALL_BTC', 100000000, 'ALL_BTC (Short)'),
    ('nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short', 'ALL_SOL', 1000000000, 'ALL_SOL (Short)'),
    ('nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short', 'AKT', 1000000, 'AKT (Short)'),
    ('nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short', 'ATOM', 1000000, 'ATOM (Short)'),
    ('nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short', 'OSMO', 1000000, 'OSMO (Short)')
ON CONFLICT ("pool_id") DO UPDATE SET
    "position_type" = EXCLUDED."position_type",
    "lpn_symbol" = EXCLUDED."lpn_symbol",
    "lpn_decimals" = EXCLUDED."lpn_decimals",
    "label" = EXCLUDED."label";


-- ============================================
-- UPDATE 3: Protocol Column (12.01.2026)
-- ============================================
ALTER TABLE "pool_config" ADD COLUMN IF NOT EXISTS "protocol" VARCHAR(50);

UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-USDC_NOBLE' WHERE "pool_id" = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-USDC_AXELAR' WHERE "pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5';
UPDATE "pool_config" SET "protocol" = 'NEUTRON-ASTROPORT-USDC_AXELAR' WHERE "pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94';
UPDATE "pool_config" SET "protocol" = 'NEUTRON-ASTROPORT-USDC_NOBLE' WHERE "pool_id" = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ST_ATOM' WHERE "pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ALL_BTC' WHERE "pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ALL_SOL' WHERE "pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-AKT' WHERE "pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-ATOM' WHERE "pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6';
UPDATE "pool_config" SET "protocol" = 'OSMOSIS-OSMOSIS-OSMO' WHERE "pool_id" = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t';
```

### Post-Migration Steps

```bash
# 1. Restart the ETL service
sudo systemctl restart etl

# 2. Check the logs
journalctl -u etl -f

# 3. Run backfill for transaction codes (optional, if not done before)
curl "https://etl.nolus.network/update/raw-txs?auth=YOUR_AUTH_TOKEN"

# 4. Run backfill for LS_Opening pre-computed columns (run until remaining=0)
while true; do
  response=$(curl -s "https://etl.nolus.network/update/backfill-ls-opening?auth=YOUR_AUTH_TOKEN&batch_size=1000")
  remaining=$(echo $response | jq '.remaining')
  echo "Remaining: $remaining"
  if [ "$remaining" -eq 0 ]; then
    echo "Backfill complete!"
    break
  fi
  sleep 1
done
```

---

## API Changes Summary

### Removed Endpoints
| Endpoint | Status | Replacement |
|----------|--------|-------------|
| `GET /api/borrow-apr` | **REMOVED** | Use `borrow_apr` field from `/api/pools` |

### Renamed Endpoints
| Old Endpoint | New Endpoint |
|--------------|--------------|
| `GET /api/utilization-levels` | `GET /api/pools` |

### New `/api/pools` Response Format

```json
{
  "protocols": [
    {
      "protocol": "OSMOSIS-OSMOSIS-USDC_NOBLE",
      "utilization": "68.5",
      "supplied": "5432109.87",
      "borrowed": "3721543.21",
      "borrow_apr": "12.5"
    }
  ]
}
```

### Field Mapping (Old to New)

| Old Field | New Field | Notes |
|-----------|-----------|-------|
| `total_value_locked_in_stable` | `supplied` | Renamed for clarity |
| `date` | *removed* | No longer included |
| *n/a* | `borrowed` | **NEW** - Amount borrowed from pool |
| *n/a* | `borrow_apr` | **NEW** - From deprecated `/api/borrow-apr` |

### Protocol Name Format
Protocol names now use the canonical format: `NETWORK-DEX-ASSET`

| Protocol Name |
|---------------|
| `OSMOSIS-OSMOSIS-USDC_NOBLE` |
| `OSMOSIS-OSMOSIS-USDC_AXELAR` |
| `NEUTRON-ASTROPORT-USDC_AXELAR` |
| `NEUTRON-ASTROPORT-USDC_NOBLE` |
| `OSMOSIS-OSMOSIS-ST_ATOM` |
| `OSMOSIS-OSMOSIS-ALL_BTC` |
| `OSMOSIS-OSMOSIS-ALL_SOL` |
| `OSMOSIS-OSMOSIS-AKT` |
| `OSMOSIS-OSMOSIS-ATOM` |
| `OSMOSIS-OSMOSIS-OSMO` |

### UI Migration Steps

1. Replace all calls to `/api/borrow-apr` with the `borrow_apr` field from `/api/pools`
2. Update endpoint URL from `/api/utilization-levels` to `/api/pools`
3. Update field mapping:
   - `total_value_locked_in_stable` → `supplied`
   - Remove any usage of `date` field
   - Add handling for new `borrowed` field if needed
4. Update protocol name mappings to use the canonical format

---

## Verification

After migration, verify the changes:

```sql
-- Check pool_config table
SELECT * FROM "pool_config";

-- Check raw_message code column exists
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'raw_message' AND column_name = 'code';

-- Check LS_Opening backfill progress
SELECT 
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE "LS_opening_price" IS NOT NULL) as with_opening_price,
  COUNT(*) FILTER (WHERE "LS_liquidation_price_at_open" IS NOT NULL) as with_liq_price
FROM "LS_Opening";
```

Test the new endpoint:
```bash
curl "https://etl.nolus.network/api/pools"
```
