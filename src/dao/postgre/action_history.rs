use chrono::{DateTime, Utc};
use sqlx::Error;

use crate::model::{Action_History, Table};

impl Table<Action_History> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(&self, data: Action_History) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "action_history" (
            "action_type",
            "created_at"
        )
        VALUES ($1, $2)
        "#;

        sqlx::query(SQL)
            .bind(data.action_type)
            .bind(data.created_at)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    // FIXME Pass `Actions` instead.
    pub async fn get_last_by_type(
        &self,
        action_type: String,
    ) -> Result<Option<Action_History>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "action_history"
        WHERE "action_type" = $1
        ORDER BY "created_at" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL)
            .bind(action_type)
            .fetch_optional(&self.pool)
            .await
    }

    // FIXME Pass `Actions` instead.
    pub async fn get_last_by_type_before(
        &self,
        action_type: String,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<Action_History>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "action_history"
        WHERE
            "action_type" = $1 AND
            "created_at" < $2
        ORDER BY "created_at" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL)
            .bind(action_type)
            .bind(timestamp)
            .fetch_optional(&self.pool)
            .await
    }
}
