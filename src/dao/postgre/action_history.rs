use chrono::{DateTime, Utc};
use sqlx::error::Error;

use crate::model::{Action_History, Actions, Table};

impl Table<Action_History> {
    pub async fn insert(
        &self,
        Action_History {
            action_type,
            created_at,
        }: Action_History,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "action_history" (
            "action_type",
            "created_at"
        )
        VALUES($1, $2)
        "#;

        sqlx::query(SQL)
            .bind(action_type.as_str())
            .bind(created_at)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_last_by_type(
        &self,
        action_type: Actions,
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

    pub async fn get_last_by_type_before(
        &self,
        action_type: Actions,
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
