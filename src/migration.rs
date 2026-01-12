//! Database migration module using refinery
//!
//! Provides atomic, versioned database migrations that run on startup.
//! Migrations are tracked in a `refinery_schema_history` table.

use refinery::{embed_migrations, Target};
use tokio_postgres::NoTls;

use crate::error::Error;

// Embed migrations from the migrations/ directory at compile time
embed_migrations!("migrations");

/// Run all pending database migrations atomically.
///
/// This function:
/// 1. Connects to the database using the provided URL
/// 2. Checks which migrations have already been applied
/// 3. Runs any pending migrations in order
/// 4. Records each successful migration in `refinery_schema_history`
///
/// Migrations are atomic - if any migration fails, the transaction is rolled back.
pub async fn run_migrations(database_url: &str) -> Result<(), Error> {
    tracing::info!("Running database migrations...");

    // Parse the database URL for tokio-postgres
    let config: tokio_postgres::Config = database_url
        .parse()
        .map_err(|e| Error::ConfigurationError(format!("Invalid database URL: {}", e)))?;

    // Connect to the database
    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|e| Error::ConfigurationError(format!("Failed to connect for migrations: {}", e)))?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Migration connection error: {}", e);
        }
    });

    // Run migrations
    let report = migrations::runner()
        .run_async(&mut client)
        .await
        .map_err(|e| Error::ConfigurationError(format!("Migration failed: {}", e)))?;

    // Log results
    let applied = report.applied_migrations();
    if applied.is_empty() {
        tracing::info!("No new migrations to apply");
    } else {
        for migration in applied {
            tracing::info!(
                "Applied migration: V{:03}__{} (checksum: {})",
                migration.version(),
                migration.name(),
                migration.checksum()
            );
        }
        tracing::info!("Successfully applied {} migration(s)", applied.len());
    }

    Ok(())
}

/// Mark all migrations as applied without running them.
///
/// Uses refinery's Target::Fake to update the schema history table
/// without executing the actual migration SQL. Useful for databases
/// where the schema already exists from manual migration.
pub async fn run_migrations_fake(database_url: &str) -> Result<(), Error> {
    tracing::info!("Faking database migrations (marking as applied without running)...");

    let config: tokio_postgres::Config = database_url
        .parse()
        .map_err(|e| Error::ConfigurationError(format!("Invalid database URL: {}", e)))?;

    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|e| Error::ConfigurationError(format!("Failed to connect for migrations: {}", e)))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Migration connection error: {}", e);
        }
    });

    // Use Target::Fake to mark migrations as applied without running them
    let report = migrations::runner()
        .set_target(Target::Fake)
        .run_async(&mut client)
        .await
        .map_err(|e| Error::ConfigurationError(format!("Migration failed: {}", e)))?;

    let applied = report.applied_migrations();
    if applied.is_empty() {
        tracing::info!("No migrations to mark as applied");
    } else {
        for migration in applied {
            tracing::info!(
                "Marked as applied: V{:03}__{} (checksum: {})",
                migration.version(),
                migration.name(),
                migration.checksum()
            );
        }
        tracing::info!("Marked {} migration(s) as applied", applied.len());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrations_are_embedded() {
        // Verify migrations are properly embedded at compile time
        let runner = migrations::runner();
        let migrations = runner.get_migrations();
        assert!(!migrations.is_empty(), "No migrations found");
        
        // Verify migrations are in order
        let mut prev_version = 0;
        for m in migrations {
            assert!(
                m.version() > prev_version,
                "Migrations must be in ascending version order"
            );
            prev_version = m.version();
        }
    }
}
