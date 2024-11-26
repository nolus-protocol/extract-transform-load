use std::str::FromStr;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::{future::try_join_all, TryFutureExt};
use sqlx::Transaction;
use tokio::task::JoinSet;
use tracing::info;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::{Loan_Closing_Status, Protocol_Types},
    model::{
        LS_Close_Position, LS_Liquidation, LS_Loan, LS_Loan_Closing, LS_Opening,
    },
    provider::is_sync_runing,
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
    if is_sync_runing() {
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
                        (change_amount, r#type.to_owned()),
                        taxes.to_owned(),
                        at.to_owned(),
                    )
                    .await?
                },
                Protocol_Types::Short => {
                    get_pnl_short(
                        &app_state,
                        &lease,
                        contract.to_owned(),
                        (change_amount, r#type.to_owned()),
                        taxes.to_owned(),
                        at.to_owned(),
                    )
                    .await?
                },
            };

            return Ok(LS_Loan_Closing {
                Block: block,
                LS_contract_id: loan.LS_contract_id,
                LS_amnt_stable: loan.LS_amnt_stable,
                LS_timestamp: loan.LS_timestamp,
                Type: String::from(r#type),
                LS_amnt: loan.LS_amnt,
                Active: loan.Active,
                LS_pnl: loan.LS_pnl,
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

pub async fn get_pnl_long(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    contract: String,
    change_amount: (BigDecimal, Loan_Closing_Status),
    taxes: BigDecimal,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let f1 = app_state
        .database
        .ls_liquidation
        .get_by_contract(contract.to_owned())
        .map_err(Error::from);

    let f2 = app_state
        .database
        .ls_close_position
        .get_by_contract(contract.to_owned())
        .map_err(Error::from);

    let f3 =
        get_fees(&app_state, &lease, protocol.to_owned()).map_err(Error::from);

    let (liqidations, closings, fee) = tokio::try_join!(f1, f2, f3)?;

    let mut liqidated_amount = BigDecimal::from(0);
    let mut closed_amount = BigDecimal::from(0);
    let mut pnl = BigDecimal::from(0);
    let mut tasks = vec![];
    let mut LS_amnt_stable = BigDecimal::from(0);

    for l in liqidations {
        liqidated_amount += l.LS_amnt;
    }

    for c in closings {
        let f = get_change_long(
            app_state,
            lease.LS_asset_symbol.to_owned(),
            c.LS_amnt.to_string(),
            protocol.to_owned(),
            lease.LS_timestamp,
            c.LS_timestamp,
        )
        .map_ok(|r| (r, c.LS_amnt));
        tasks.push(f);
    }

    let res = try_join_all(tasks).await?;

    for (change, amount) in res {
        pnl += change;
        closed_amount += amount
    }

    let mut loan = &lease.LS_loan_amnt - &liqidated_amount - &closed_amount;

    if change_amount.0 > BigDecimal::from(0) {
        loan -= &change_amount.0;
        match change_amount.1 {
            Loan_Closing_Status::MarketClose => {
                let change = get_change_long(
                    &app_state,
                    lease.LS_asset_symbol.to_owned(),
                    change_amount.1.to_string(),
                    protocol.to_owned(),
                    lease.LS_timestamp.to_owned(),
                    at.to_owned(),
                )
                .await?;
                pnl += change;
            },
            _ => {},
        }
    }

    let loan_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol.to_owned())
        .context(format!(
            "LS_asset_symbol not found {}",
            &lease.LS_asset_symbol
        ))?;

    let loan_amount_symbol_decimals =
        BigDecimal::from(u64::pow(10, loan_currency.1.try_into()?));

    pnl -= &fee;
    pnl -= (&taxes * loan_amount_symbol_decimals).round(0);

    if loan > BigDecimal::from(0) {
        let symbol = lease.LS_asset_symbol.to_owned();
        let l = loan.to_owned().to_string();

        let f1 = app_state.in_stabe_by_date(
            &symbol,
            &l,
            Some(protocol.to_owned()),
            &at,
        );

        let f2 = get_change_long(
            &app_state,
            lease.LS_asset_symbol.to_owned(),
            loan.to_string(),
            protocol.to_owned(),
            lease.LS_timestamp,
            at.to_owned(),
        );

        let (amount, p) = tokio::try_join!(f1, f2)?;
        LS_amnt_stable += amount;
        pnl += p;
    }

    return Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable,
        LS_timestamp: at,
        LS_amnt: loan,
        LS_pnl: pnl,
        Active: true,
    });
}

pub async fn get_pnl_short(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    contract: String,
    change_amount: (BigDecimal, Loan_Closing_Status),
    taxes: BigDecimal,
    at: DateTime<Utc>,
) -> Result<LS_Loan, Error> {
    let loan_fn = app_state
        .database
        .ls_loan_closing
        .get_lease_amount(contract.to_owned())
        .map_err(Error::from);

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;
    let lpn_decimals =
        BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));
    let loan_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_asset_symbol.to_owned())
        .context(format!(
            "LS_asset_symbol not found {}",
            &lease.LS_asset_symbol
        ))?;

    let ctrl_currency = app_state
        .config
        .hash_map_currencies
        .get(&lease.LS_cltr_symbol)
        .context(format!(
            "ctrl_currencyt not found {}",
            &lease.LS_cltr_symbol
        ))?;

    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let f1 = app_state
        .database
        .mp_asset
        .get_price_by_date(&lpn_currency.0, Some(protocol.to_owned()), &at)
        .map_err(Error::from);

    let repayments_fn = app_state
        .database
        .ls_repayment
        .get_by_contract(lease.LS_contract_id.to_owned())
        .map_err(Error::from);

    let ((close_price,), mut loan, repayments) =
        tokio::try_join!(f1, loan_fn, repayments_fn)?;

    let ctrl_amount_stable = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, ctrl_currency.1.try_into()?));

    let loan_amnt = &lease.LS_loan_amnt
        / &close_price
        / BigDecimal::from(u64::pow(10, loan_currency.1.try_into()?));

    let ls_loan_amnt = &lease.LS_loan_amnt_asset / &lpn_decimals;

    let mut pnl =
        (&loan_amnt - &ls_loan_amnt) * &close_price - &ctrl_amount_stable;
    // let mut pnl = (&lease.LS_loan_amnt / &close_price
    //     - &lease.LS_loan_amnt_asset)
    //     * &close_price
    //     - ctrl_amount_stable;

    let mut amount = BigDecimal::from(0);

    for repayment in repayments {
        amount += repayment.LS_payment_amnt;
    }

    amount =
        amount / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));
    pnl -= amount * &close_price;
    pnl -= (&taxes * &lpn_decimals).round(0);

    if change_amount.0 > BigDecimal::from(0) {
        loan -= &change_amount.0;
    }

    let LS_amnt_stable = &loan * &close_price;

    return Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable,
        LS_timestamp: at,
        LS_amnt: loan,
        LS_pnl: pnl,
        Active: true,
    });
}

pub async fn get_change_long(
    app_state: &AppState<State>,
    symbol: String,
    amnt: String,
    protocol: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<BigDecimal, Error> {
    let f1 = app_state.in_stabe_by_date(
        &symbol,
        &amnt,
        Some(protocol.to_owned()),
        &start_date,
    );

    let f2 = app_state.in_stabe_by_date(
        &symbol,
        &amnt,
        Some(protocol.to_owned()),
        &end_date,
    );

    let (open, close) = tokio::try_join!(f1, f2)?;

    Ok(close - open)
}

pub async fn get_fees(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    protocol: String,
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
        .in_stabe_by_date(
            &symbol,
            &loan_amnt,
            Some(protocol),
            &lease.LS_timestamp,
        )
        .map_err(Error::from);

    let (loan_amount, market_closings, liquidations) =
        tokio::try_join!(f1, market_closings_fn, liquidations_fn)?;

    let market_close_fee = get_market_close_fee(app_state, market_closings)?
        * &loan_amount_symbol_decimals;
    let liquidation_fee = get_liquidation_fee(app_state, liquidations)?
        * &loan_amount_symbol_decimals;

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

    Ok(fee.round(0))
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

        let payment_amount = &market_close.LS_payment_amnt_stable
            / BigDecimal::from(u64::pow(10, c2.1.try_into()?));

        let amount_amount = &market_close.LS_amnt_stable
            / BigDecimal::from(u64::pow(10, c1.1.try_into()?));

        let amount = amount_amount - payment_amount;
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

        let payment_amount = &liquidation.LS_payment_amnt_stable
            / BigDecimal::from(u64::pow(10, c2.1.try_into()?));

        let amount_amount = &liquidation.LS_amnt_stable
            / BigDecimal::from(u64::pow(10, c1.1.try_into()?));
        let amount = amount_amount - payment_amount;
        fee += amount;
    }

    Ok(fee)
}
