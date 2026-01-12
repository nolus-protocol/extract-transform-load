//! CLI module for ETL service
//!
//! Provides command-line interface for running migrations, backfills,
//! and other maintenance tasks without starting the HTTP server.

use clap::{Parser, Subcommand};

use crate::{
    configuration::{get_configuration, set_configuration, Config},
    error::Error,
    migration,
    provider::DatabasePool,
};

/// Nolus ETL Service
#[derive(Parser)]
#[command(name = "etl")]
#[command(about = "Nolus blockchain ETL service", long_about = None)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Run the ETL server (default if no command specified)
    Serve,

    /// Run database migrations
    Migrate {
        /// Show migration status without running migrations
        #[arg(long)]
        status: bool,

        /// Mark migrations as applied without running them (for existing databases).
        /// Use alone to fake all migrations, or with a version number to fake up to that version.
        /// Example: --fake 4 marks V001-V004 as applied, then runs V005+ normally.
        #[arg(long)]
        fake: Option<Option<u32>>,
    },

    /// Run data backfill operations
    Backfill {
        #[command(subcommand)]
        command: BackfillCommands,
    },
}

#[derive(Subcommand)]
pub enum BackfillCommands {
    /// Backfill LS_Opening pre-computed columns (position_type, opening_price, liquidation_price)
    LsOpening {
        /// Number of records to process per batch
        #[arg(long, default_value = "500")]
        batch_size: i32,

        /// Preview changes without applying them
        #[arg(long)]
        dry_run: bool,

        /// Run until all records are processed
        #[arg(long)]
        all: bool,
    },

    /// Backfill raw_message transaction codes
    RawTxs {
        /// Number of concurrent tasks
        #[arg(long, default_value = "10")]
        concurrency: usize,

        /// Preview changes without applying them
        #[arg(long)]
        dry_run: bool,
    },
}

/// Initialize configuration and return Config
pub fn init_config() -> Result<Config, Error> {
    set_configuration()?;
    get_configuration()
}

/// Run migrations and show status
pub async fn run_migrate(status_only: bool, fake: Option<Option<u32>>) -> Result<(), Error> {
    let config = init_config()?;

    match fake {
        // --fake (no version) - fake all migrations
        Some(None) => {
            tracing::info!("Marking all migrations as applied without running them...");
            migration::run_migrations_fake(&config.database_url, None).await?;
            tracing::info!("All migrations marked as applied");
        }
        // --fake <version> - fake up to version, then run remaining
        Some(Some(version)) => {
            tracing::info!("Marking migrations up to V{:03} as applied...", version);
            migration::run_migrations_fake(&config.database_url, Some(version)).await?;
            tracing::info!("Running remaining migrations...");
            migration::run_migrations(&config.database_url).await?;
            tracing::info!("Migrations complete");
        }
        // No --fake flag
        None => {
            if status_only {
                tracing::info!("Checking migration status...");
                migration::run_migrations(&config.database_url).await?;
                tracing::info!("Migration status check complete");
            } else {
                tracing::info!("Running database migrations...");
                migration::run_migrations(&config.database_url).await?;
                tracing::info!("Migrations complete");
            }
        }
    }

    Ok(())
}

/// Run LS_Opening backfill
pub async fn run_backfill_ls_opening(
    batch_size: i32,
    dry_run: bool,
    run_all: bool,
) -> Result<(), Error> {
    let config = init_config()?;

    // Run migrations first to ensure schema is up to date
    migration::run_migrations(&config.database_url).await?;

    let database = DatabasePool::new(&config).await?;

    tracing::info!("Starting LS_Opening backfill...");
    if dry_run {
        tracing::info!("DRY RUN MODE - no changes will be made");
    }

    loop {
        // Step 1: Backfill from pool_config
        let step1_query = if dry_run {
            r#"
            SELECT COUNT(*)::BIGINT
            FROM "LS_Opening" o
            JOIN "pool_config" pc ON o."LS_loan_pool_id" = pc."pool_id"
            WHERE o."LS_position_type" IS NULL
            "#
        } else {
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
            "#
        };

        let step1_count = sqlx::query_scalar::<_, i64>(step1_query)
            .fetch_one(&database.pool)
            .await
            .unwrap_or(0);

        if step1_count > 0 {
            tracing::info!(
                "Step 1: {} position_type, lpn_symbol, lpn_decimals from pool_config",
                if dry_run { "Would update" } else { "Updated" },
            );
            tracing::info!("  Records: {}", step1_count);
        }

        // Step 2: Backfill opening_price
        let step2_count = if dry_run {
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM "LS_Opening"
                WHERE "LS_opening_price" IS NULL
                "#,
            )
            .fetch_one(&database.pool)
            .await
            .unwrap_or(0)
            .min(batch_size as i64)
        } else {
            sqlx::query_scalar::<_, i64>(
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
            .fetch_one(&database.pool)
            .await
            .unwrap_or(0)
        };

        if step2_count > 0 {
            tracing::info!(
                "Step 2: {} opening_price from MP_Asset",
                if dry_run { "Would update" } else { "Updated" },
            );
            tracing::info!("  Records: {}", step2_count);
        }

        // Step 3: Backfill liquidation_price_at_open
        let step3_count = if dry_run {
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM "LS_Opening"
                WHERE "LS_liquidation_price_at_open" IS NULL
                  AND "LS_opening_price" IS NOT NULL
                  AND "LS_position_type" IS NOT NULL
                "#,
            )
            .fetch_one(&database.pool)
            .await
            .unwrap_or(0)
        } else {
            sqlx::query_scalar::<_, i64>(
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
            .fetch_one(&database.pool)
            .await
            .unwrap_or(0)
        };

        if step3_count > 0 {
            tracing::info!(
                "Step 3: {} liquidation_price_at_open",
                if dry_run { "Would update" } else { "Updated" },
            );
            tracing::info!("  Records: {}", step3_count);
        }

        // Count remaining
        let remaining = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM "LS_Opening"
            WHERE "LS_opening_price" IS NULL
               OR "LS_liquidation_price_at_open" IS NULL
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .unwrap_or(0);

        tracing::info!("Remaining records to process: {}", remaining);

        if remaining == 0 {
            tracing::info!("Backfill complete!");
            break;
        }

        if !run_all || dry_run {
            if remaining > 0 {
                tracing::info!(
                    "Run with --all flag to process all {} remaining records",
                    remaining
                );
            }
            break;
        }
    }

    Ok(())
}

/// Run raw_message backfill
pub async fn run_backfill_raw_txs(
    concurrency: usize,
    dry_run: bool,
) -> Result<(), Error> {
    use tokio::task::JoinSet;
    use crate::configuration::{AppState, State};
    use crate::provider::{Grpc, HTTP};
    use anyhow::Context;

    let config = init_config()?;

    // Run migrations first
    migration::run_migrations(&config.database_url).await?;

    let database = DatabasePool::new(&config).await?;

    // Count records needing update
    let count = sqlx::query_scalar::<_, i64>(
        r#"SELECT COUNT(*)::BIGINT FROM "raw_message" WHERE "code" IS NULL"#,
    )
    .fetch_one(&database.pool)
    .await
    .unwrap_or(0);

    tracing::info!("Found {} raw_message records needing code backfill", count);

    if dry_run {
        tracing::info!("DRY RUN MODE - no changes will be made");
        return Ok(());
    }

    if count == 0 {
        tracing::info!("No records to process");
        return Ok(());
    }

    // Initialize full state for gRPC access
    let grpc = Grpc::new(config.clone()).await?;
    let http = HTTP::new(config.clone())?;
    let state = State::new(config.clone(), database, grpc, http).await?;
    let app_state = AppState::new(state);

    let data = app_state.database.raw_message.get_all().await?;
    let total = data.len();
    let mut processed = 0;

    tracing::info!("Processing {} records with concurrency {}", total, concurrency);

    let mut tasks: Vec<_> = data.into_iter().collect();

    while !tasks.is_empty() {
        let mut set = JoinSet::new();
        let batch_size = tasks.len().min(concurrency);

        for _ in 0..batch_size {
            if let Some(raw_message) = tasks.pop() {
                let s = app_state.clone();
                set.spawn(async move {
                    let tx = s
                        .grpc
                        .get_tx(raw_message.tx_hash.to_owned(), raw_message.block)
                        .await?
                        .context(format!("missing transaction {}", &raw_message.tx_hash))?;

                    let mut msg = raw_message;
                    msg.code = Some(tx.code.try_into()?);
                    s.database.raw_message.update(msg).await?;
                    Ok::<_, Error>(())
                });
            }
        }

        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(())) => processed += 1,
                Ok(Err(e)) => tracing::warn!("Failed to process record: {}", e),
                Err(e) => tracing::warn!("Task panicked: {}", e),
            }
        }

        if processed % 100 == 0 || tasks.is_empty() {
            tracing::info!("Progress: {}/{} ({:.1}%)", processed, total, (processed as f64 / total as f64) * 100.0);
        }
    }

    tracing::info!("Backfill complete! Processed {} records", processed);

    Ok(())
}
