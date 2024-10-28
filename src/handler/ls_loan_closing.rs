use std::str::FromStr;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use sqlx::Transaction;
use tokio::task::JoinSet;
use tracing::info;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Loan_Closing_Status,
    model::{LS_Close_Position, LS_Liquidation, LS_Loan_Closing, LS_Opening},
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    contract: String,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    change_amount: BigDecimal,
    taxes: BigDecimal,
    block: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let isExists = app_state
        .database
        .ls_loan_closing
        .isExists(contract.to_owned())
        .await?;

    if !isExists {
        let ls_loan_closing = get_loan(
            app_state.clone(),
            contract,
            r#type,
            at,
            block,
            change_amount,
            taxes,
        )
        .await?;
        app_state
            .database
            .ls_loan_closing
            .insert(ls_loan_closing, transaction)
            .await?;
    }

    Ok(())
}

pub async fn proceed_leases(app_state: AppState<State>) -> Result<(), Error> {
    let items = app_state
        .database
        .ls_loan_closing
        .get_leases_to_proceed()
        .await?;
    let mut tasks = vec![];
    let max_tasks = app_state.config.max_tasks;

    for item in items {
        tasks.push(proceed(app_state.clone(), item));
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
    info!("Loans Synchronization completed");

    Ok(())
}

async fn proceed(
    app_state: AppState<State>,
    item: LS_Loan_Closing,
) -> Result<(), Error> {
    let ls_loan_closing = get_loan(
        app_state.clone(),
        item.LS_contract_id,
        Loan_Closing_Status::from_str(&item.Type)?,
        item.LS_timestamp,
        item.Block,
        BigDecimal::from(0),
        BigDecimal::from(0),
    )
    .await?;

    if ls_loan_closing.Active {
        app_state
            .database
            .ls_loan_closing
            .update(ls_loan_closing)
            .await?;
    }

    Ok(())
}

async fn get_loan(
    app_state: AppState<State>,
    contract: String,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    block: i64,
    change_amount: BigDecimal,
    taxes: BigDecimal,
) -> Result<LS_Loan_Closing, Error> {
    let active = app_state
        .database
        .block
        .is_synced_to_block(block - 1)
        .await?;

    if !active {
        return Ok(LS_Loan_Closing {
            LS_contract_id: contract.to_owned(),
            LS_amnt_stable: BigDecimal::from(0),
            LS_timestamp: at,
            Type: String::from(r#type),
            LS_amnt: BigDecimal::from(0),
            LS_pnl: BigDecimal::from(0),
            Block: block,
            Active: false,
        });
    }

    let lease = app_state
        .database
        .ls_opening
        .get(contract.to_owned())
        .await?;

    match lease {
        Some(lease) => {
            let loan = app_state
                .database
                .ls_loan_closing
                .get_lease_amount(contract.to_owned())
                .await?
                - change_amount;

            let protocol =
                app_state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);

            let symbol = lease.LS_asset_symbol.to_owned();
            let loan_str = &loan.to_string();

            let f1 = app_state.in_stabe_by_date(
                &symbol,
                loan_str,
                protocol.to_owned(),
                &lease.LS_timestamp,
            );

            let f2 = app_state.in_stabe_by_date(
                &symbol,
                loan_str,
                protocol.to_owned(),
                &at,
            );

            let (open_amount, close_amount, fee) = tokio::try_join!(
                f1,
                f2,
                get_fees(&app_state, &lease, protocol.to_owned())
            )?;
            dbg!(&close_amount.to_string());
            dbg!(&open_amount.to_string());
            dbg!(&fee.to_string());
            dbg!(&taxes.to_string());
            let pnl = &close_amount - &open_amount - fee - &taxes;

            return Ok(LS_Loan_Closing {
                LS_contract_id: contract.to_owned(),
                LS_amnt_stable: close_amount,
                LS_timestamp: at,
                Type: String::from(r#type),
                LS_amnt: loan,
                LS_pnl: pnl,
                Block: block,
                Active: true,
            });
        },
        None => {
            return Ok(LS_Loan_Closing {
                LS_contract_id: contract.to_owned(),
                LS_amnt_stable: BigDecimal::from(0),
                LS_timestamp: at,
                Type: String::from(r#type),
                LS_amnt: BigDecimal::from(0),
                LS_pnl: BigDecimal::from(0),
                Block: block,
                Active: false,
            });
        },
    };
}

async fn get_fees(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    protocol: Option<String>,
) -> Result<BigDecimal, Error> {
    let symbol = &lease.LS_asset_symbol.to_owned();
    let ctrl_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!(
            "ctrl_currencyt not found {}",
            &lease.LS_cltr_symbol
        ))?;

    let loan_currency = app_state
        .config
        .hash_map_currencies
        .get(&symbol.to_owned())
        .context(format!("LS_asset_symbol not found {}", &symbol))?;

    let market_closings_fn = app_state
        .database
        .ls_close_position
        .get_by_contract(lease.LS_contract_id.to_owned())
        .map_err(Error::from);

    let liquidations_fn = app_state
        .database
        .ls_liquidation
        .get_by_contract(lease.LS_contract_id.to_owned())
        .map_err(Error::from);

    let ctrl_amount_stable = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, ctrl_currency.1.try_into()?));

    let loan_amount_symbol_decimals =
        BigDecimal::from(u64::pow(10, loan_currency.1.try_into()?));

    let loan_amnt =
        (&lease.LS_loan_amnt / &loan_amount_symbol_decimals).to_string();

    let f1 = app_state
        .in_stabe_by_date(&symbol, &loan_amnt, protocol, &lease.LS_timestamp)
        .map_err(Error::from);

    let (loan_amount, market_closings, liquidations) =
        tokio::try_join!(f1, market_closings_fn, liquidations_fn)?;

    let market_close_fee = get_market_close_fee(app_state, market_closings)?;
    let liquidation_fee = get_liquidation_fee(app_state, liquidations)?;

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let loan_amount = (loan_amount * &loan_amount_symbol_decimals).round(0);
    let loan_amount_stable = &lease.LS_loan_amnt_stable
        / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));

    let total_loan_stable = ((loan_amount_stable + ctrl_amount_stable)
        * &loan_amount_symbol_decimals)
        .round(0);

    let fee =
        total_loan_stable - loan_amount + market_close_fee + liquidation_fee;

    Ok(fee)
}

pub fn get_market_close_fee(
    app_state: &AppState<State>,
    market_closings: Vec<LS_Close_Position>,
) -> Result<BigDecimal, Error> {
    let mut fee = BigDecimal::from(0);
    for market_close in market_closings {
        let c1 = app_state
            .config
            .hash_map_currencies
            .get(&market_close.LS_amnt_symbol)
            .context(format!(
                "market_close.LS_amount_symbol not found {}",
                &market_close.LS_amnt_symbol
            ))?;

        let c2 = app_state
            .config
            .hash_map_currencies
            .get(&market_close.LS_payment_symbol)
            .context(format!(
                "LS_payment_symbol not found {}",
                &market_close.LS_payment_symbol
            ))?;
        let decimals = BigDecimal::from(u64::pow(10, c2.1.try_into()?));
        let payment_amount = &market_close.LS_payment_amnt_stable / &decimals;

        let amount_amount = &market_close.LS_amnt_stable
            / BigDecimal::from(u64::pow(10, c1.1.try_into()?));
        let amount = ((amount_amount - payment_amount) * &decimals).round(0);
        fee += amount;
    }

    Ok(fee)
}

pub fn get_liquidation_fee(
    app_state: &AppState<State>,
    liquidations: Vec<LS_Liquidation>,
) -> Result<BigDecimal, Error> {
    let mut fee = BigDecimal::from(0);
    for liquidation in liquidations {
        let c1 = app_state
            .config
            .hash_map_currencies
            .get(&liquidation.LS_amnt_symbol)
            .context(format!(
                "liquidation.LS_amnt_symbol not found {}",
                &liquidation.LS_amnt_symbol
            ))?;

        let c2 = app_state
            .config
            .hash_map_currencies
            .get(&liquidation.LS_payment_symbol)
            .context(format!(
                "liquidation.LS_payment_symbol not found {}",
                &liquidation.LS_payment_symbol
            ))?;
        let decimals = BigDecimal::from(u64::pow(10, c2.1.try_into()?));
        let payment_amount = &liquidation.LS_payment_amnt_stable / &decimals;

        let amount_amount = &liquidation.LS_amnt_stable
            / BigDecimal::from(u64::pow(10, c1.1.try_into()?));
        let amount = ((amount_amount - payment_amount) * &decimals).round(0);
        fee += amount;
    }

    Ok(fee)
}
