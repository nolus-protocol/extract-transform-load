use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use tokio::try_join;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{LS_Opening, LS_State},
    try_join_with_capacity,
    types::AmountTicker,
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestamp: DateTime<Utc>,
) -> Result<(), Error> {
    app_state
        .database
        .ls_state
        .insert_many(
            try_join_with_capacity::<_, Vec<_>, _, _, _>(
                app_state
                    .database
                    .ls_state
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
    state: AppState<State>,
    item: LS_Opening,
    timestamp: DateTime<Utc>,
) -> Result<Option<LS_State>, Error> {
    let data = state.grpc.get_lease_state(item.LS_contract_id).await?;

    if let Some(status) = data.opened {
        let pool_currency =
            state.get_currency_by_pool_id(&item.LS_loan_pool_id)?;
        let protocol = state.get_protocol_by_pool_id(&item.LS_loan_pool_id);

        let (price, pool_currency_price) = try_join!(
            state
                .database
                .mp_asset
                .get_price(&status.amount.ticker, protocol.to_owned()),
            state
                .database
                .mp_asset
                .get_price(&pool_currency.denominator, protocol.to_owned()),
        )?;

        let previous_margin_due =
            status.previous_margin_due.unwrap_or(AmountTicker {
                amount: "0".to_owned(),
                ticker: pool_currency.denominator.to_owned(),
            });
        let overdue_margin = status.overdue_margin.unwrap_or(AmountTicker {
            amount: "0".to_owned(),
            ticker: pool_currency.denominator.to_owned(),
        });

        let previous_interest_due =
            status.previous_interest_due.unwrap_or(AmountTicker {
                amount: "0".to_owned(),
                ticker: pool_currency.denominator.to_owned(),
            });
        let overdue_interest =
            status.overdue_interest.unwrap_or(AmountTicker {
                amount: "0".to_owned(),
                ticker: pool_currency.denominator.to_owned(),
            });

        let current_margin_due =
            status.current_margin_due.unwrap_or(AmountTicker {
                amount: "0".to_owned(),
                ticker: pool_currency.denominator.to_owned(),
            });
        let due_margin = status.due_margin.unwrap_or(AmountTicker {
            amount: "0".to_owned(),
            ticker: pool_currency.denominator.to_owned(),
        });

        let current_interest_due =
            status.current_interest_due.unwrap_or(AmountTicker {
                amount: "0".to_owned(),
                ticker: pool_currency.denominator.to_owned(),
            });
        let due_interest = status.due_interest.unwrap_or(AmountTicker {
            amount: "0".to_owned(),
            ticker: pool_currency.denominator.to_owned(),
        });

        let value = &previous_margin_due.amount;
        let previous_margin_due_stable = (value * &pool_currency_price)?;
        let value = &overdue_margin.amount;
        let overdue_margin_stable = (value * &pool_currency_price)?;

        let value = &previous_interest_due.amount;
        let previous_interest_due_stable = (value * &pool_currency_price)?;
        let value = &overdue_interest.amount;
        let overdue_interest_stable = (value * &pool_currency_price)?;

        let value = &current_margin_due.amount;
        let current_margin_due_stable = (value * &pool_currency_price)?;
        let value = &due_margin.amount;
        let due_margin_stable = (value * &pool_currency_price)?;

        let value = &current_interest_due.amount;
        let current_interest_due_stable = (value * &pool_currency_price)?;
        let value = &due_interest.amount;
        let due_interest_stable = (value * &pool_currency_price)?;

        let (
            LS_prev_margin_asset,
            LS_prev_interest_asset,
            LS_current_margin_asset,
            LS_current_interest_asset,
            LS_principal_asset,
        ) = (
            BigDecimal::from_str(&overdue_margin.amount)?,
            BigDecimal::from_str(&overdue_interest.amount)?,
            BigDecimal::from_str(&due_margin.amount)?,
            BigDecimal::from_str(&due_interest.amount)?,
            BigDecimal::from_str(&status.principal_due.amount)?,
        );

        let LS_lpn_loan_amnt = BigDecimal::from_str(&status.amount.amount)?
            * &price
            / &pool_currency_price;

        let value = &status.principal_due.amount;
        let value1 = &status.amount.amount;
        let ls_state = LS_State {
            LS_contract_id: item.LS_contract_id,
            LS_timestamp: timestamp,
            LS_amnt_stable: (value1 * &price)?,
            LS_amnt: BigDecimal::from_str(&status.amount.amount.to_string())?,
            LS_prev_margin_stable: previous_margin_due_stable
                + overdue_margin_stable,
            LS_prev_interest_stable: previous_interest_due_stable
                + overdue_interest_stable,
            LS_current_margin_stable: current_margin_due_stable
                + due_margin_stable,
            LS_current_interest_stable: current_interest_due_stable
                + due_interest_stable,
            LS_principal_stable: (value * &pool_currency_price)?,
            LS_lpn_loan_amnt,
            LS_prev_margin_asset,
            LS_prev_interest_asset,
            LS_current_margin_asset,
            LS_current_interest_asset,
            LS_principal_asset,
        };

        return Ok(Some(ls_state));
    }

    Ok(None)
}
