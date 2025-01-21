use std::{
    borrow::{Borrow, Cow},
    sync::Arc,
};

use anyhow::Context as _;
use bigdecimal::{num_bigint::BigInt, BigDecimal, One as _, Zero as _};
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use sqlx::Transaction;
use tokio::task::JoinSet;
use tracing::info;

use crate::{
    configuration::State,
    custom_uint::UInt63,
    dao::DataBase,
    error::Error,
    helpers::{Loan_Closing_Status, Protocol_Types},
    model::{LS_Loan, LS_Loan_Closing, LS_Opening},
    provider::is_sync_runing,
    try_join_with_capacity,
};

pub async fn parse_and_insert(
    app_state: &State,
    contract: &str,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    block: UInt63,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let isExists = app_state
        .database
        .ls_loan_closing
        .isExists(contract)
        .await?;

    if !isExists {
        let ls_loan_closing =
            get_loan(app_state, contract.to_string(), r#type, at, block)
                .await?;

        app_state
            .database
            .ls_loan_closing
            .insert(&ls_loan_closing, transaction)
            .await?;
    }

    Ok(())
}

pub async fn proceed_leases(app_state: Arc<State>) -> Result<(), Error> {
    try_join_with_capacity(
        app_state
            .database
            .ls_loan_closing
            .get_leases_to_proceed()
            .await?
            .into_iter()
            .map(|item| proceed(&app_state.clone(), item)),
        app_state.config.max_tasks,
    )
    .await?;

    info!("Loans Synchronization completed");

    Ok(())
}

async fn proceed<Str, Decimal>(
    app_state: &State,
    item: LS_Loan_Closing<Str, Decimal>,
) -> Result<(), Error>
where
    Str: Borrow<str>,
    Decimal: Borrow<BigDecimal>,
{
    let ls_loan_closing = get_loan(
        &app_state,
        item.LS_contract_id,
        item.Type.borrow().parse()?,
        item.LS_timestamp,
        item.Block,
    )
    .await?;

    if ls_loan_closing.Active {
        app_state
            .database
            .ls_loan_closing
            .update(&ls_loan_closing)
            .await?;
    }

    Ok(())
}

async fn get_loan(
    app_state: &State,
    contract: String,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    block: UInt63,
) -> Result<LS_Loan_Closing<String, BigDecimal>, Error> {
    if is_sync_runing() {
        return Ok(LS_Loan_Closing {
            LS_contract_id: contract,
            LS_amnt_stable: BigDecimal::from(0),
            LS_timestamp: at,
            Type: r#type.to_string(),
            LS_amnt: BigDecimal::from(0),
            LS_pnl: BigDecimal::from(0),
            Block: block,
            Active: false,
        });
    }

    let lease = app_state.database.ls_opening.get(&contract).await?;

    match lease {
        Some(lease) => {
            if r#type == Loan_Closing_Status::Liquidation {
                let l = get_pnl_liquidated(
                    &app_state,
                    &lease,
                    contract,
                    at.to_owned(),
                )
                .await?;

                return Ok(LS_Loan_Closing {
                    Block: block,
                    LS_contract_id: l.LS_contract_id,
                    LS_amnt_stable: l.LS_amnt_stable,
                    LS_timestamp: l.LS_timestamp,
                    Type: String::from(r#type),
                    LS_amnt: l.LS_amnt,
                    Active: l.Active,
                    LS_pnl: l.LS_pnl,
                });
            }

            let protocol_data = app_state
                .config
                .hash_map_lp_pools
                .get(&lease.LS_loan_pool_id)
                .context(format!(
                    "could not get protocol {}",
                    &lease.LS_loan_pool_id
                ))?;

            let loan = match protocol_data.2 {
                Protocol_Types::Long => {
                    get_pnl_long(
                        &app_state,
                        &lease,
                        contract.to_owned(),
                        block.to_owned(),
                        at.to_owned(),
                    )
                    .await?
                },
                Protocol_Types::Short => {
                    get_pnl_short(
                        &app_state,
                        &lease,
                        contract.to_owned(),
                        block,
                        at.to_owned(),
                    )
                    .await?
                },
            };

            Ok(LS_Loan_Closing {
                Block: block,
                LS_contract_id: loan.LS_contract_id,
                LS_amnt_stable: loan.LS_amnt_stable,
                LS_timestamp: loan.LS_timestamp,
                Type: String::from(r#type),
                LS_amnt: loan.LS_amnt,
                Active: loan.Active,
                LS_pnl: loan.LS_pnl,
            })
        },
        None => Ok(LS_Loan_Closing {
            LS_contract_id: contract.to_owned(),
            LS_amnt_stable: BigDecimal::from(0),
            LS_timestamp: at,
            Type: String::from(r#type),
            LS_amnt: BigDecimal::from(0),
            LS_pnl: BigDecimal::from(0),
            Block: block,
            Active: false,
        }),
    }
}

pub async fn get_pnl_long(
    app_state: &State,
    lease: &LS_Opening,
    contract: String,
    block: UInt63,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let lease_status = app_state
        .grpc
        .get_lease_state_by_block(contract.to_owned(), block - 1)
        .await?
        .opened
        .context("Loan not opened")?;

    let lease_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease_status.amount.ticker)
        .context(format!(
            "LS_asset_symbol not found {}",
            &lease_status.amount.ticker
        ))?;

    let downpayment_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!(
            "lease.LS_cltr_symbol not found {}",
            &lease.LS_cltr_symbol
        ))?;

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let lease_amount_data = lease_status.amount.amount.parse()?;
    let lease_amount = &lease_amount_data
        * BigDecimal::new(BigInt::one(), lease_currency.exponent.into());

    let lease_debt = lease_status.principal_due.amount.parse()?
        + lease_status
            .overdue_margin
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .overdue_interest
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .due_margin
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .due_interest
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero);

    let lease_debt = lease_debt
        * BigDecimal::new(BigInt::one(), lpn_currency.exponent.into());

    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let amount_fn = app_state.in_stabe_by_date(
        &lease_status.amount.ticker,
        lease_amount,
        Some(protocol),
        at,
    );

    let lease_downpayment = &lease.LS_cltr_amnt_stable
        * BigDecimal::new(BigInt::one(), downpayment_currency.exponent.into());

    let fee_fn = get_fees(&app_state, &lease, protocol).map_err(From::from);

    let repayments_fn = app_state
        .database
        .ls_repayment
        .get_by_contract(&lease.LS_contract_id)
        .map_err(From::from);

    let (fee, repayments, amount) =
        tokio::try_join!(fee_fn, repayments_fn, amount_fn)?;

    let mut repayment_value = BigDecimal::from(0);

    for repayment in repayments {
        if !repayment.LS_loan_close {
            let currency = app_state
                .config
                .hash_map_currencies
                .get(&repayment.LS_payment_symbol)
                .context(format!(
                    "currency not found  {}",
                    &repayment.LS_payment_symbol
                ))?;

            repayment_value += repayment.LS_payment_amnt_stable
                * BigDecimal::new(BigInt::one(), currency.exponent.into());
        }
    }

    let fee =
        fee * BigDecimal::new(BigInt::one(), lease_currency.exponent.into());

    let pnl = &amount - lease_debt - repayment_value - lease_downpayment + fee;

    Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable: &amount
            / BigDecimal::new(BigInt::one(), lease_currency.exponent.into()),
        LS_timestamp: at,
        LS_amnt: lease_amount_data,
        LS_pnl: pnl
            / BigDecimal::new(BigInt::one(), lease_currency.exponent.into()),
        Active: true,
    })
}

pub async fn get_pnl_short(
    app_state: &State,
    lease: &LS_Opening,
    contract: String,
    block: UInt63,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let lease_status = app_state
        .grpc
        .get_lease_state_by_block(contract.to_owned(), block - 1)
        .await?
        .opened
        .context("Loan not opened")?;

    let lease_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease_status.amount.ticker)
        .context(format!(
            "LS_asset_symbol not found {}",
            &lease_status.amount.ticker
        ))?;

    let downpayment_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!(
            "lease.LS_cltr_symbol not found {}",
            &lease.LS_cltr_symbol
        ))?;

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let lease_amount_data = lease_status.amount.amount.parse()?;
    let lease_amount = &lease_amount_data
        * BigDecimal::new(BigInt::one(), lease_currency.exponent.into());

    let lease_debt = lease_status.principal_due.amount.parse()?
        + lease_status
            .overdue_margin
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .overdue_interest
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .due_margin
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero)
        + lease_status
            .due_interest
            .map(|amount| amount.amount.parse())
            .transpose()?
            .unwrap_or_else(BigDecimal::zero);

    let lease_debt = lease_debt
        / BigDecimal::from(u64::pow(10, lpn_currency.exponent.try_into()?));

    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", lease.LS_loan_pool_id))?;

    let amount_fn = app_state.in_stabe_by_date(
        &lease_status.amount.ticker,
        lease_amount,
        Some(protocol),
        at,
    );

    let lease_downpayment = &lease.LS_cltr_amnt_stable
        * BigDecimal::new(BigInt::one(), downpayment_currency.exponent.into());

    let fee_fn = get_fees(&app_state, &lease, protocol).map_err(From::from);

    let repayments_fn = app_state
        .database
        .ls_repayment
        .get_by_contract(&lease.LS_contract_id)
        .map_err(From::from);

    let lpn_price_fn = app_state
        .database
        .mp_asset
        .get_price_by_date(&lpn_currency.denominator, Some(protocol), at)
        .map_err(From::from);

    let (fee, repayments, amount, lpn_price) =
        tokio::try_join!(fee_fn, repayments_fn, amount_fn, lpn_price_fn)?;

    let mut repayment_value = BigDecimal::zero();

    for repayment in repayments {
        if !repayment.LS_loan_close {
            let currency = app_state
                .config
                .hash_map_currencies
                .get(&repayment.LS_payment_symbol)
                .context(format!(
                    "currency not found  {}",
                    &repayment.LS_payment_symbol
                ))?;

            repayment_value += repayment.LS_payment_amnt_stable
                * BigDecimal::new(BigInt::one(), currency.exponent.into());
        }
    }

    let fee =
        fee * BigDecimal::new(BigInt::one(), lease_currency.exponent.into());

    let pnl =
        &amount - lease_debt * lpn_price - repayment_value - lease_downpayment
            + fee;

    Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable: &amount
            / BigDecimal::new(BigInt::one(), lease_currency.exponent.into()),
        LS_timestamp: at,
        LS_amnt: lease_amount_data,
        LS_pnl: pnl
            / BigDecimal::new(BigInt::one(), lease_currency.exponent.into()),
        Active: true,
    })
}

pub async fn get_change_long(
    app_state: &State,
    symbol: &str,
    amnt: BigDecimal,
    protocol: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<BigDecimal, Error> {
    let f1 = app_state.in_stabe_by_date(
        symbol,
        amnt.clone(),
        Some(protocol),
        start_date,
    );

    let f2 = app_state.in_stabe_by_date(symbol, amnt, Some(protocol), end_date);

    let (open, close) = tokio::try_join!(f1, f2)?;

    Ok(close - open)
}

pub async fn get_fees(
    app_state: &State,
    lease: &LS_Opening,
    protocol: &str,
) -> Result<BigDecimal, Error> {
    let symbol = &lease.LS_asset_symbol;
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
        .get(symbol)
        .context(format!("LS_asset_symbol not found {symbol}"))?;

    let ctrl_amount_stable = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, ctrl_currency.exponent.try_into()?));

    let loan_amount_symbol_decimals =
        BigDecimal::from(u64::pow(10, loan_currency.exponent.try_into()?));

    let loan_amnt = &lease.LS_loan_amnt / &loan_amount_symbol_decimals;

    let loan_amount_fn = app_state
        .in_stabe_by_date(symbol, loan_amnt, Some(protocol), lease.LS_timestamp)
        .map_err(Error::from);

    let loan_amount = loan_amount_fn.await?;

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let loan_amount = (loan_amount * &loan_amount_symbol_decimals).round(0);
    let loan_amount_stable = &lease.LS_loan_amnt_stable
        / BigDecimal::from(u64::pow(10, lpn_currency.exponent.try_into()?));

    let total_loan_stable = ((loan_amount_stable + ctrl_amount_stable)
        * &loan_amount_symbol_decimals)
        .round(0);

    let fee = total_loan_stable - loan_amount;

    Ok(fee.round(0))
}

pub async fn get_pnl_liquidated(
    app_state: &State,
    lease: &LS_Opening,
    contract: String,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let lease_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol)
        .context(format!(
            "LS_asset_symbol not found {}",
            &lease.LS_asset_symbol
        ))?;

    let downpayment_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!(
            "lease.LS_cltr_symbol not found {}",
            &lease.LS_cltr_symbol
        ))?;

    let lease_downpayment = &lease.LS_cltr_amnt_stable
        * BigDecimal::new(BigInt::one(), downpayment_currency.exponent.into());

    let repayments = app_state
        .database
        .ls_repayment
        .get_by_contract(&lease.LS_contract_id)
        .await?;

    let mut repayment_value = BigDecimal::from(0);

    for repayment in repayments {
        let currency = app_state
            .config
            .hash_map_currencies
            .get(&repayment.LS_payment_symbol)
            .context(format!(
                "currency not found  {}",
                &repayment.LS_payment_symbol
            ))?;

        repayment_value += repayment.LS_payment_amnt_stable
            * BigDecimal::new(BigInt::one(), currency.exponent.into());
    }

    let pnl = -(repayment_value + lease_downpayment);

    Ok(LS_Loan {
        LS_contract_id: contract,
        LS_amnt_stable: BigDecimal::from(0),
        LS_timestamp: at,
        LS_amnt: BigDecimal::from(0),
        LS_pnl: pnl
            / BigDecimal::new(BigInt::one(), lease_currency.exponent.into()),
        Active: true,
    })
}
