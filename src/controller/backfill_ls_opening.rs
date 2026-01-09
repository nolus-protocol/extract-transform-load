use actix_web::{get, web, Responder};
use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

/// Backfill endpoint for LS_Opening pre-computed columns.
/// 
/// This endpoint populates the following columns for historical data:
/// - LS_position_type (from pool_config)
/// - LS_lpn_symbol (from pool_config)
/// - LS_lpn_decimals (from pool_config)
/// - LS_opening_price (from MP_Asset historical prices)
/// - LS_liquidation_price_at_open (calculated from position type and prices)
///
/// Usage: GET /update/backfill-ls-opening?auth=<AUTH_TOKEN>&batch_size=500
///
/// The backfill runs in 3 steps:
/// 1. Populate pool_config data (fast)
/// 2. Populate opening prices in batches (slower, uses batch_size)
/// 3. Calculate liquidation prices (fast, after step 2)
///
/// Call this endpoint repeatedly until "remaining" is 0.
#[get("/update/backfill-ls-opening")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = query.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response {
            success: false,
            step1_updated: 0,
            step2_updated: 0,
            step3_updated: 0,
            remaining: 0,
            message: "Unauthorized".to_string(),
        }));
    }

    let batch_size = query.batch_size.unwrap_or(500);

    // Step 1: Backfill from pool_config (position_type, lpn_symbol, lpn_decimals)
    let step1_updated = sqlx::query_scalar::<_, i64>(
        r#"
        WITH updated AS (
            UPDATE "LS_Opening" o SET
                "LS_position_type" = pc."position_type",
                "LS_lpn_symbol" = pc."lpn_symbol",
                "LS_lpn_decimals" = pc."lpn_decimals"
            FROM "pool_config" pc 
            WHERE o."LS_loan_pool_id" = pc."pool_id"
              AND o."LS_position_type" IS NULL
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM updated
        "#,
    )
    .fetch_one(&state.database.pool)
    .await
    .unwrap_or(0);

    // Step 2: Backfill opening_price in batches
    let step2_updated = sqlx::query_scalar::<_, i64>(
        r#"
        WITH batch AS (
            SELECT "LS_contract_id" 
            FROM "LS_Opening" 
            WHERE "LS_opening_price" IS NULL 
            ORDER BY "LS_timestamp" DESC
            LIMIT $1
        ),
        updated AS (
            UPDATE "LS_Opening" o SET "LS_opening_price" = (
                SELECT m."MP_price_in_stable" 
                FROM "MP_Asset" m
                WHERE m."MP_asset_symbol" = o."LS_asset_symbol"
                  AND m."MP_asset_timestamp" <= o."LS_timestamp"
                ORDER BY m."MP_asset_timestamp" DESC 
                LIMIT 1
            )
            WHERE o."LS_contract_id" IN (SELECT "LS_contract_id" FROM batch)
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM updated
        "#,
    )
    .bind(batch_size as i64)
    .fetch_one(&state.database.pool)
    .await
    .unwrap_or(0);

    // Step 3: Backfill liquidation_price_at_open (only for rows that have opening_price)
    let step3_updated = sqlx::query_scalar::<_, i64>(
        r#"
        WITH updated AS (
            UPDATE "LS_Opening" o SET "LS_liquidation_price_at_open" = 
                CASE 
                    WHEN o."LS_position_type" = 'Long' THEN 
                        (o."LS_loan_amnt_stable" / 1000000.0 / 0.9) / 
                        NULLIF((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / 1000000.0, 0) * 
                        o."LS_opening_price"
                    WHEN o."LS_position_type" = 'Short' THEN 
                        ((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / 1000000.0) / 
                        NULLIF(o."LS_lpn_loan_amnt" / 1000000.0 / 0.9, 0)
                END
            WHERE o."LS_liquidation_price_at_open" IS NULL
              AND o."LS_opening_price" IS NOT NULL
              AND o."LS_position_type" IS NOT NULL
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM updated
        "#,
    )
    .fetch_one(&state.database.pool)
    .await
    .unwrap_or(0);

    // Count remaining rows needing backfill
    let remaining = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM "LS_Opening"
        WHERE "LS_opening_price" IS NULL
           OR "LS_liquidation_price_at_open" IS NULL
        "#,
    )
    .fetch_one(&state.database.pool)
    .await
    .unwrap_or(0);

    let message = if remaining == 0 {
        "Backfill complete!".to_string()
    } else {
        format!("Backfill in progress. {} rows remaining. Call again to continue.", remaining)
    };

    Ok(web::Json(Response {
        success: true,
        step1_updated,
        step2_updated,
        step3_updated,
        remaining,
        message,
    }))
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub success: bool,
    pub step1_updated: i64,
    pub step2_updated: i64,
    pub step3_updated: i64,
    pub remaining: i64,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    auth: Option<String>,
    /// Batch size for opening_price backfill (default: 500)
    batch_size: Option<i32>,
}
