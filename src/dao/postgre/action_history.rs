use chrono::{DateTime, Utc};
use sqlx::Error;

use crate::model::{Action_History, Table};

use super::QueryResult;

impl Table<Action_History> {
    pub async fn insert(
        &self,
        data: Action_History,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO action_history (action_type, created_at)
            VALUES($1, $2)
            "#,
        )
        .bind(&data.action_type)
        .bind(data.created_at)
        .persistent(false)
        .execute(&self.pool)
        .await
    }

    pub async fn get_last_by_type(
        &self,
        action_type: String,
    ) -> Result<Option<Action_History>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "action_history" WHERE "action_type" = $1 ORDER BY "created_at" DESC LIMIT 1
            "#,
        )
        .bind(action_type)
        .persistent(false)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_last_by_type_before(
        &self,
        action_type: String,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<Action_History>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "action_history" WHERE "action_type" = $1 AND "created_at" < $2 ORDER BY "created_at" DESC LIMIT 1
            "#,
        )
        .bind(action_type)
        .bind(timestamp)
        .persistent(false)
        .fetch_optional(&self.pool)
        .await
    }
}
