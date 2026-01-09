use super::QueryResult;
use crate::model::{Subscription, Table};
use sqlx::error::Error;

impl Table<Subscription> {
    pub async fn insert(
        &self,
        subscription: Subscription,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO subscription (address, p256dh, auth, endpoint, expiration, ip, user_agent)
            VALUES($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&subscription.address)
        .bind(&subscription.p256dh)
        .bind(&subscription.auth)
        .bind(&subscription.endpoint)
        .bind(subscription.expiration)
        .bind(&subscription.ip)
        .bind(&subscription.user_agent)
        .execute(&self.pool)
        .await
    }

    pub async fn get_by_address(
        &self,
        address: String,
    ) -> Result<Vec<Subscription>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM subscription WHERE active = true and address=$1
            "#,
        )
        .bind(address)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn deactivate(
        &self,
        endpoint: String,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE subscription SET active = false WHERE endpoint=$1
            "#,
        )
        .bind(endpoint)
        .execute(&self.pool)
        .await
    }

    pub async fn deactivate_by_auth_and_ne_address(
        &self,
        address: String,
        auth: String,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE subscription SET active = false WHERE address!=$1 AND auth=$2
            "#,
        )
        .bind(address)
        .bind(auth)
        .execute(&self.pool)
        .await
    }

    pub async fn update(
        &self,
        active: bool,
        address: String,
        auth: String,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE subscription SET active=$1 WHERE address=$2 AND auth=$3
            "#,
        )
        .bind(active)
        .bind(address)
        .bind(auth)
        .execute(&self.pool)
        .await
    }

    pub async fn get_one(
        &self,
        address: String,
        auth: String,
    ) -> Result<Option<Subscription>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                *
            FROM "subscription"
            WHERE
                address=$1
            AND
                auth=$2
            "#,
        )
        .bind(address)
        .bind(auth)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn isExists(
        &self,
        address: String,
        auth: String,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "subscription"
            WHERE
                address=$1
            AND
                auth=$2
            "#,
        )
        .bind(address)
        .bind(auth)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }
}
