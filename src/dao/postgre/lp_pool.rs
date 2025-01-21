use std::iter;

use sqlx::{Error, QueryBuilder};

use crate::model::{LP_Pool, Table};

impl<Str> Table<LP_Pool<Str>>
where
    Str: AsRef<str>,
{
    pub async fn insert(
        &self,
        LP_Pool {
            LP_Pool_id,
            LP_symbol,
        }: &LP_Pool<Str>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LP_Pool" (
            "LP_Pool_id",
            "LP_symbol"
        )
        SELECT *
        FROM (
            SELECT
                $1 as "LP_Pool_id",
                $2 as "LP_symbol"
        ) AS "tmp"
        WHERE NOT EXISTS (
            SELECT "LP_Pool_id"
            FROM "LP_Pool"
            WHERE "LP_Pool_id" = $1
        )
        LIMIT 1
        "#;

        sqlx::query(SQL)
            .bind(LP_Pool_id.as_ref())
            .bind(LP_symbol.as_ref())
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LP_Pool<Str>>,
    {
        const SQL_PRE_VALUES: &str = r#"
        INSERT INTO "LP_Pool" (
            "LP_Pool_id",
            "LP_symbol"
        )
        "#;

        const SQL_POST_VALUES: &str = r#"
        ON CONFLICT DO NOTHING
        "#;

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL_PRE_VALUES)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 LP_Pool {
                     LP_Pool_id,
                     LP_symbol,
                 }| {
                    b.push_bind(LP_Pool_id.as_ref())
                        .push_bind(LP_symbol.as_ref());
                },
            )
            .push(SQL_POST_VALUES)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Pool<Str>>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LP_Pool"
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }
}
