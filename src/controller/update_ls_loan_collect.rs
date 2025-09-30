use std::{collections::HashMap, str::FromStr};

use actix_web::{get, web, Responder};
use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::Loan_Closing_Status,
    model::{LS_Loan_Closing, LS_Loan_Collect},
};

#[get("/update/loan-collect")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response { result: false }));
    };

    let data = state.database.ls_loan_closing.get_all().await?;
    let mut tasks = vec![];
    let max_tasks = state.config.max_tasks;

    for lease in data {
        let s = state.get_ref().clone();
        tasks.push(proceed(s, lease));
    }

    while !tasks.is_empty() {
        let mut st = JoinSet::new();
        let range = if tasks.len() > max_tasks {
            max_tasks
        } else {
            tasks.len()
        };

        for _t in 0..range {
            if let Some(item) = tasks.pop() {
                st.spawn(item);
            }
        }

        while let Some(item) = st.join_next().await {
            item??;
        }
    }

    return Ok(web::Json(Response { result: true }));
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    auth: Option<String>,
}

async fn proceed(
    state: AppState<State>,
    ls_loan_closing: LS_Loan_Closing,
) -> Result<(), Error> {
    let ignore = vec![4179035, 4179039];
    if ignore.contains(&ls_loan_closing.Block) {
        return Ok(());
    }
    match Loan_Closing_Status::from_str(&ls_loan_closing.Type)? {
        Loan_Closing_Status::Reypay => {
            proceed_repayment(state, ls_loan_closing).await?;
        },
        Loan_Closing_Status::MarketClose => {
            proceed_market_close(state, ls_loan_closing).await?;
        },
        _ => {},
    }

    Ok(())
}

async fn proceed_repayment(
    state: AppState<State>,
    ls_loan_closing: LS_Loan_Closing,
) -> Result<(), Error> {
    let (balances, lease) = tokio::try_join!(
        state.grpc.get_balances_by_block(
            ls_loan_closing.LS_contract_id.to_owned(),
            ls_loan_closing.Block - 1
        ),
        state.grpc.get_lease_raw_state_by_block(
            ls_loan_closing.LS_contract_id.to_owned(),
            ls_loan_closing.Block - 1
        )
    )?;

    let mut data: HashMap<String, LS_Loan_Collect> = HashMap::new();

    if let Some(lease) = lease.OpenedActive {
        let v1 = lease.lease.lease.position;
        let v2 = lease.lease.lease.amount;

        let value = if let Some(v) = v2 {
            v
        } else if let Some(v) = v1 {
            v.amount
        } else {
            return Err(Error::MissingParams(String::from("missing params")));
        };

        let amount_stable = get_stable_amount(
            state.clone(),
            ls_loan_closing.LS_contract_id.to_owned(),
            value.amount.to_owned(),
            value.ticker.to_owned(),
            ls_loan_closing.LS_timestamp.to_owned(),
        )
        .await?;
        data.insert(
            value.ticker.to_owned(),
            LS_Loan_Collect {
                LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                LS_symbol: value.ticker.to_owned(),
                LS_amount: BigDecimal::from_str(&value.amount)?,
                LS_amount_stable: amount_stable,
            },
        );
    }

    for b in balances.balances {
        let item = state.config.supported_currencies.iter().find(|item| {
            return item.2 == b.denom.to_uppercase();
        });

        if let Some(c) = item {
            let amount_stable = get_stable_amount(
                state.clone(),
                ls_loan_closing.LS_contract_id.to_owned(),
                b.amount.to_owned(),
                c.0.to_owned(),
                ls_loan_closing.LS_timestamp.to_owned(),
            )
            .await?;

            data.insert(
                c.0.to_owned(),
                LS_Loan_Collect {
                    LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                    LS_symbol: c.0.to_owned(),
                    LS_amount: BigDecimal::from_str(&b.amount)?,
                    LS_amount_stable: amount_stable,
                },
            );
        }
    }

    let contract_balances: Vec<LS_Loan_Collect> = data
        .into_values()
        .filter(|item| item.LS_symbol != state.config.native_currency)
        .collect();

    state
        .database
        .ls_loan_collect
        .insert_many(&contract_balances)
        .await?;

    Ok(())
}

async fn proceed_market_close(
    state: AppState<State>,
    ls_loan_closing: LS_Loan_Closing,
) -> Result<(), Error> {
    let (lease,) = tokio::try_join!(state.grpc.get_lease_raw_state_by_block(
        ls_loan_closing.LS_contract_id.to_owned(),
        ls_loan_closing.Block - 1
    ),)?;

    if let Some(_) = lease.FullClose {
        let data = state
            .database
            .ls_close_position
            .get_closed_by_contract(ls_loan_closing.LS_contract_id.to_owned())
            .await;
        if let Ok(l) = data {
            let amount_stable = get_stable_amount(
                state.clone(),
                ls_loan_closing.LS_contract_id.to_owned(),
                l.LS_change.to_string(),
                l.LS_payment_symbol.to_owned(),
                ls_loan_closing.LS_timestamp.to_owned(),
            )
            .await?;

            let data = vec![LS_Loan_Collect {
                LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                LS_symbol: l.LS_payment_symbol,
                LS_amount: l.LS_change,
                LS_amount_stable: amount_stable,
            }];

            state.database.ls_loan_collect.insert_many(&data).await?;
        }

        return Ok(());
    }

    if let Some(_) = lease.PartialClose {
        let mut data: HashMap<String, LS_Loan_Collect> = HashMap::new();

        let (balances, lease_state) = tokio::try_join!(
            state.grpc.get_balances_by_block(
                ls_loan_closing.LS_contract_id.to_owned(),
                ls_loan_closing.Block
            ),
            state.grpc.get_lease_raw_state_by_block(
                ls_loan_closing.LS_contract_id.to_owned(),
                ls_loan_closing.Block
            )
        )?;

        if let Some(paid) = &lease_state.ClosingTransferIn {
            let c = paid.clone();
            let paid_amount =
                c.TransferInInit.context("Missing paid.TransferInInit")?;

            let amount_stable = get_stable_amount(
                state.clone(),
                ls_loan_closing.LS_contract_id.to_owned(),
                paid_amount.amount_in.amount.to_owned(),
                paid_amount.amount_in.ticker.to_owned(),
                ls_loan_closing.LS_timestamp.to_owned(),
            )
            .await?;

            data.insert(
                paid_amount.amount_in.ticker.to_owned(),
                LS_Loan_Collect {
                    LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                    LS_symbol: paid_amount.amount_in.ticker.to_owned(),
                    LS_amount: BigDecimal::from_str(
                        &paid_amount.amount_in.amount,
                    )?,
                    LS_amount_stable: amount_stable,
                },
            );
        };

        for b in balances.balances {
            let item = state.config.supported_currencies.iter().find(|item| {
                return item.2 == b.denom.to_uppercase();
            });

            if let Some(c) = item {
                let amount_stable = get_stable_amount(
                    state.clone(),
                    ls_loan_closing.LS_contract_id.to_owned(),
                    b.amount.to_owned(),
                    c.0.to_owned(),
                    ls_loan_closing.LS_timestamp.to_owned(),
                )
                .await?;

                data.insert(
                    c.0.to_owned(),
                    LS_Loan_Collect {
                        LS_contract_id: ls_loan_closing
                            .LS_contract_id
                            .to_owned(),
                        LS_symbol: c.0.to_owned(),
                        LS_amount: BigDecimal::from_str(&b.amount)?,
                        LS_amount_stable: amount_stable,
                    },
                );
            }
        }

        let contract_balances: Vec<LS_Loan_Collect> = data
            .into_values()
            .filter(|item| item.LS_symbol != state.config.native_currency)
            .collect();

        state
            .database
            .ls_loan_collect
            .insert_many(&contract_balances)
            .await?;
    }

    Ok(())
}

async fn get_stable_amount(
    state: AppState<State>,
    contract: String,
    amount: String,
    ticker: String,
    date: DateTime<Utc>,
) -> Result<BigDecimal, Error> {
    let ls_opening = state
        .database
        .ls_opening
        .get(contract.to_owned())
        .await?
        .context(format!("lease contract not found {}", &contract))?;

    let protocol = match state
        .get_protocol_by_pool_id(&ls_opening.LS_loan_pool_id)
        .context(format!(
            "protocol not found {}",
            &ls_opening.LS_loan_pool_id
        )) {
        Ok(p) => Some(p),
        Err(_) => None,
    };

    let amount_stable = state
        .in_stabe_by_date(&ticker, &amount, protocol, &date)
        .await?;

    Ok(amount_stable)
}
