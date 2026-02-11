use sqlx::Error;

use crate::model::{ProtocolRegistry, Table};

impl Table<ProtocolRegistry> {
    /// Upsert an active protocol from admin contract
    /// Sets is_active = true and clears deprecated_at
    pub async fn upsert_active(
        &self,
        protocol: &ProtocolRegistry,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO "protocol_registry" 
                ("protocol_name", "network", "dex", "leaser_contract", "lpp_contract", 
                 "oracle_contract", "profit_contract", "reserve_contract", 
                 "lpn_symbol", "position_type", "is_active", "first_seen_at")
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true, NOW())
            ON CONFLICT ("protocol_name") DO UPDATE SET
                "network" = COALESCE(EXCLUDED."network", "protocol_registry"."network"),
                "dex" = COALESCE(EXCLUDED."dex", "protocol_registry"."dex"),
                "leaser_contract" = COALESCE(EXCLUDED."leaser_contract", "protocol_registry"."leaser_contract"),
                "lpp_contract" = COALESCE(EXCLUDED."lpp_contract", "protocol_registry"."lpp_contract"),
                "oracle_contract" = COALESCE(EXCLUDED."oracle_contract", "protocol_registry"."oracle_contract"),
                "profit_contract" = COALESCE(EXCLUDED."profit_contract", "protocol_registry"."profit_contract"),
                "reserve_contract" = COALESCE(EXCLUDED."reserve_contract", "protocol_registry"."reserve_contract"),
                "lpn_symbol" = EXCLUDED."lpn_symbol",
                "position_type" = EXCLUDED."position_type",
                "is_active" = true,
                "deprecated_at" = NULL
            "#,
        )
        .bind(&protocol.protocol_name)
        .bind(&protocol.network)
        .bind(&protocol.dex)
        .bind(&protocol.leaser_contract)
        .bind(&protocol.lpp_contract)
        .bind(&protocol.oracle_contract)
        .bind(&protocol.profit_contract)
        .bind(&protocol.reserve_contract)
        .bind(&protocol.lpn_symbol)
        .bind(&protocol.position_type)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark all protocols NOT in the provided list as deprecated
    pub async fn mark_deprecated_except(
        &self,
        active_names: &[String],
    ) -> Result<u64, Error> {
        let result = sqlx::query(
            r#"
            UPDATE "protocol_registry"
            SET "is_active" = false, "deprecated_at" = NOW()
            WHERE "protocol_name" != ALL($1) AND "is_active" = true
            "#,
        )
        .bind(active_names)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get all protocols (active and deprecated)
    pub async fn get_all(&self) -> Result<Vec<ProtocolRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "protocol_name", "network", "dex", "leaser_contract", "lpp_contract",
                   "oracle_contract", "profit_contract", "reserve_contract",
                   "lpn_symbol", "position_type", "is_active", "first_seen_at", "deprecated_at"
            FROM "protocol_registry"
            ORDER BY "is_active" DESC, "protocol_name"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get only active protocols
    pub async fn get_active(&self) -> Result<Vec<ProtocolRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "protocol_name", "network", "dex", "leaser_contract", "lpp_contract",
                   "oracle_contract", "profit_contract", "reserve_contract",
                   "lpn_symbol", "position_type", "is_active", "first_seen_at", "deprecated_at"
            FROM "protocol_registry"
            WHERE "is_active" = true
            ORDER BY "protocol_name"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get a protocol by name
    pub async fn get_by_name(
        &self,
        name: &str,
    ) -> Result<Option<ProtocolRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "protocol_name", "network", "dex", "leaser_contract", "lpp_contract",
                   "oracle_contract", "profit_contract", "reserve_contract",
                   "lpn_symbol", "position_type", "is_active", "first_seen_at", "deprecated_at"
            FROM "protocol_registry"
            WHERE "protocol_name" = $1
            "#,
        )
        .bind(name)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    /// Get a protocol by LPP contract (pool_id)
    pub async fn get_by_lpp_contract(
        &self,
        lpp_contract: &str,
    ) -> Result<Option<ProtocolRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "protocol_name", "network", "dex", "leaser_contract", "lpp_contract",
                   "oracle_contract", "profit_contract", "reserve_contract",
                   "lpn_symbol", "position_type", "is_active", "first_seen_at", "deprecated_at"
            FROM "protocol_registry"
            WHERE "lpp_contract" = $1
            "#,
        )
        .bind(lpp_contract)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    /// Count protocols by status
    pub async fn count_by_status(&self) -> Result<(i64, i64), Error> {
        let active: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "protocol_registry" WHERE "is_active" = true"#,
        )
        .fetch_one(&self.pool)
        .await?;

        let deprecated: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "protocol_registry" WHERE "is_active" = false"#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok((active.0, deprecated.0))
    }
}
