use std::{str::FromStr, sync::Arc};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;

use crate::{
    configuration::State, error::Error, model::LP_Lender_State,
    try_join_with_capacity,
};

pub async fn parse_and_insert(
    app_state: Arc<State>,
    timestamp: DateTime<Utc>,
) -> Result<(), Error> {
    app_state
        .database
        .lp_lender_state
        .insert_many(
            try_join_with_capacity::<_, Vec<_>, _, _, _>(
                app_state
                    .database
                    .lp_lender_state
                    .get_active_states()
                    .await?
                    .into_iter()
                    .map(|item| proceed(app_state.clone(), item, timestamp)),
                app_state.config.max_tasks,
            )
            .await?,
        )
        .await
        .map_err(From::from)
}

async fn proceed(
    state: Arc<State>,
    item: (String, String),
    timestsamp: DateTime<Utc>,
) -> Result<LP_Lender_State, Error> {
    let (LP_address_id, LP_Pool_id) = item;
    let (balance, price) = tokio::try_join!(
        state
            .grpc
            .get_balance_state(LP_Pool_id.to_owned(), &LP_address_id),
        state.grpc.get_lpp_price(LP_Pool_id.to_owned())
    )?;

    let lpp_price = {
        let amount = BigDecimal::from_str(&price.amount.amount)?;

        let quote_amount = BigDecimal::from_str(&price.amount_quote.amount)?;

        amount / quote_amount
    };

    let lpp_balance = BigDecimal::from_str(&balance.balance)?;

    let value = lpp_price * &lpp_balance;

    let amount_stable = state.in_stabe_by_pool_id(&LP_Pool_id, &value).await?;

    let lp_lender_state = LP_Lender_State {
        LP_Lender_id: LP_address_id.to_owned(),
        LP_Pool_id,
        LP_timestamp: timestsamp,
        LP_Lender_stable: amount_stable,
        LP_Lender_asset: value,
        LP_Lender_receipts: lpp_balance,
    };

    Ok(lp_lender_state)
}

pub fn start_task(
    app_state: Arc<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
