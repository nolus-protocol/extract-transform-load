use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use std::str::FromStr;
use tokio::task::JoinHandle;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TR_State,
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let mut data = Vec::new();
    let balances = app_state
        .query_api
        .query_balance(app_state.config.treasury_contract.to_string())
        .await?;

    if let Some(balance) = balances {
        if let Some(page) = balance.pagination {
            if page.total > 1 {
                return Err(Error::CoinLengthError());
            }
        }

        let (stable_price,) = app_state
            .database
            .mp_asset
            .get_price(&app_state.config.native_currency)
            .await?;

        for coin in balance.balances {
            let item = TR_State {
                TR_timestamp: timestsamp,
                TR_amnt_stable: app_state.in_stabe_calc(
                    &stable_price,
                    &coin.amount,
                )?,
                TR_amnt_nls: BigDecimal::from_str(&coin.amount)?,
            };

            data.push(item);
        }
    }

    app_state.database.tr_state.insert_many(&data).await?;

    Ok(())
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
