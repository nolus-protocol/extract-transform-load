use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

use crate::model::{LP_Deposit, Table};

use super::{DataBase, QueryResult};

#[derive(Debug, Clone, FromRow)]
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}

impl Table<LP_Deposit> {
    pub async fn isExists(
        &self,
        ls_deposit: &LP_Deposit,
    ) -> Result<bool, Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LP_Deposit"
            WHERE
                "LP_deposit_height" = $1 AND
                "LP_address_id" = $2 AND
                "LP_timestamp" = $3 AND
                "LP_Pool_id" = $4
            "#,
        )
        .bind(ls_deposit.LP_deposit_height)
        .bind(&ls_deposit.LP_address_id)
        .bind(ls_deposit.LP_timestamp)
        .bind(&ls_deposit.LP_Pool_id)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LP_Deposit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Deposit" (
                "LP_deposit_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        )
        .bind(data.LP_deposit_height)
        .bind(&data.LP_address_id)
        .bind(data.LP_timestamp)
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_amnt_stable)
        .bind(&data.LP_amnt_asset)
        .bind(&data.LP_amnt_receipts)
        .bind(&data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Deposit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Deposit" (
                "LP_deposit_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, lp| {
            b.push_bind(lp.LP_deposit_height)
                .push_bind(&lp.LP_address_id)
                .push_bind(lp.LP_timestamp)
                .push_bind(&lp.LP_Pool_id)
                .push_bind(&lp.LP_amnt_stable)
                .push_bind(&lp.LP_amnt_asset)
                .push_bind(&lp.LP_amnt_receipts)
                .push_bind(&lp.Tx_Hash);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;

        Ok(())
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LP_Deposit" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("LP_amnt_stable")
            FROM "LP_Deposit" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_historical_lenders_with_window(
        &self,
        months: Option<i32>,
    ) -> Result<Vec<HistoricalLender>, crate::error::Error> {
        let time_condition = match months {
            Some(m) => format!("WHERE timestamp > NOW() - INTERVAL '{} months'", m),
            None => String::new(),
        };

        let query = format!(
            r#"
            SELECT * FROM (
                SELECT 
                    'Deposit' AS transaction_type,
                    "LP_timestamp" AS timestamp,
                    "LP_address_id" AS user,
                    CASE 
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000 
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000
                        ELSE "LP_amnt_stable" / 1000000
                    END AS amount, 
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5' THEN 'Osmosis axlUSDC'
                        WHEN "LP_Pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' THEN 'Neutron axlUSDC'
                        WHEN "LP_Pool_id" = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf' THEN 'Osmosis USDC'
                        WHEN "LP_Pool_id" = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6' THEN 'Neutron USDC'
                        WHEN "LP_Pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'Osmosis stATOM'
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'Osmosis allBTC'
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'Osmosis allSOL'
                        WHEN "LP_Pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'Osmosis ATOM'
                        WHEN "LP_Pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'Osmosis AKT'
                        ELSE "LP_Pool_id"
                    END AS pool
                FROM 
                    "LP_Deposit"

                UNION ALL

                SELECT 
                    'Withdraw' AS transaction_type,
                    "LP_timestamp" AS timestamp,
                    "LP_address_id" AS user,
                    CASE 
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000 
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000
                        ELSE "LP_amnt_stable" / 1000000
                    END AS amount, 
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5' THEN 'Osmosis axlUSDC'
                        WHEN "LP_Pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' THEN 'Neutron axlUSDC'
                        WHEN "LP_Pool_id" = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf' THEN 'Osmosis USDC'
                        WHEN "LP_Pool_id" = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6' THEN 'Neutron USDC'
                        WHEN "LP_Pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'Osmosis stATOM'
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'Osmosis allBTC'
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'Osmosis allSOL'
                        WHEN "LP_Pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'Osmosis ATOM'
                        WHEN "LP_Pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'Osmosis AKT'
                        ELSE "LP_Pool_id"
                    END AS pool
                FROM 
                    "LP_Withdraw"
            ) combined
            {}
            ORDER BY timestamp DESC
            "#,
            time_condition
        );

        let data = sqlx::query_as(&query)
            .persistent(false)
            .fetch_all(&self.pool)
            .await?;

        Ok(data)
    }

    pub async fn get_all_historical_lenders(
        &self,
    ) -> Result<Vec<HistoricalLender>, crate::error::Error> {
        self.get_historical_lenders_with_window(None).await
    }
}
