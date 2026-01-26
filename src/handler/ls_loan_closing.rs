use std::{collections::HashMap, str::FromStr as _};

use anyhow::Context as _;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::TryFutureExt as _;
use sqlx::Transaction;
use tokio::task::JoinSet;
use tracing::info;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Loan_Closing_Status,
    model::{LS_Loan, LS_Loan_Closing, LS_Loan_Collect, LS_Opening},
    provider::is_sync_running,
    types::AmountTicker,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    contract: String,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    block: i64,
    change_amount: Option<AmountTicker>,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let ls_loan_closing =
        get_loan(app_state.clone(), contract.to_owned(), r#type, at, block)
            .await?;

    let result = app_state
        .database
        .ls_loan_closing
        .insert_if_not_exists(ls_loan_closing.clone(), transaction)
        .await?;

    // Only proceed with loan collect if we actually inserted a new record
    if result.rows_affected() > 0 {
        proceed_loan_collect(app_state.clone(), ls_loan_closing, change_amount)
            .await?;
    }

    Ok(())
}

async fn proceed_loan_collect(
    state: AppState<State>,
    ls_loan_closing: LS_Loan_Closing,
    change_amount: Option<AmountTicker>,
) -> Result<(), Error> {
    let ls_opening = state
        .database
        .ls_opening
        .get(ls_loan_closing.LS_contract_id.to_owned())
        .await?;
    
    // Skip if lease opening not found (happens during partial sync)
    let Some(ls_opening) = ls_opening else {
        tracing::debug!(
            "Skipping loan collect for {} - lease opening not found (partial sync)",
            ls_loan_closing.LS_contract_id
        );
        return Ok(());
    };
    
    match Loan_Closing_Status::from_str(&ls_loan_closing.Type)? {
        Loan_Closing_Status::Repay => {
            proceed_repayment(
                state,
                ls_loan_closing,
                ls_opening,
            )
            .await?;
        },
        Loan_Closing_Status::MarketClose => {
            proceed_market_close(
                state,
                ls_loan_closing,
                ls_opening,
                change_amount,
            )
            .await?;
        },
        _ => {},
    }

    Ok(())
}

async fn proceed_repayment(
    state: AppState<State>,
    ls_loan_closing: LS_Loan_Closing,
    ls_opening: LS_Opening,
) -> Result<(), Error> {
    let (balances, lease) = tokio::try_join!(
        state.grpc.get_balances_by_block(
            ls_loan_closing.LS_contract_id.to_owned(),
            ls_loan_closing.Block - 1
        ),
        state.grpc.get_lease_state_by_block(
            ls_loan_closing.LS_contract_id.to_owned(),
            ls_loan_closing.Block - 1
        )
    )?;

    let mut data: HashMap<String, LS_Loan_Collect> = HashMap::new();
    let protocol = state
        .get_protocol_by_pool_id(&ls_opening.LS_loan_pool_id)
        .context(format!(
        "protocol not found {}",
        &ls_opening.LS_loan_pool_id
    ))?;

    if let Some(lease) = lease.opened {
        data.insert(
            lease.amount.ticker.to_owned(),
            LS_Loan_Collect {
                LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                LS_symbol: lease.amount.ticker.to_owned(),
                LS_amount: BigDecimal::from_str(&lease.amount.amount)?,
                LS_amount_stable: state
                    .in_stable_by_date(
                        &lease.amount.ticker,
                        &lease.amount.amount,
                        Some(protocol.to_owned()),
                        &ls_loan_closing.LS_timestamp,
                    )
                    .await?,
            },
        );
    }

    for b in balances.balances {
        // Look up currency by bank_symbol (IBC denom)
        let item = state.config.hash_map_currencies.values().find(|item| {
            item.2 == b.denom.to_uppercase()
        });

        if let Some(c) = item {
            let c = c.clone();
            data.insert(
                c.0.to_owned(),
                LS_Loan_Collect {
                    LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                    LS_symbol: c.0.to_owned(),
                    LS_amount: BigDecimal::from_str(&b.amount)?,
                    LS_amount_stable: state
                        .in_stable_by_date(
                            &c.0,
                            &b.amount,
                            Some(protocol.to_owned()),
                            &ls_loan_closing.LS_timestamp,
                        )
                        .await?,
                },
            );
        }
    }

    let native_currency = &state.config.native_currency;
    let contract_balances: Vec<LS_Loan_Collect> = data
        .into_values()
        .filter(|item| item.LS_symbol != *native_currency)
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
    ls_opening: LS_Opening,
    change_amount: Option<AmountTicker>,
) -> Result<(), Error> {
    let (lease,) = tokio::try_join!(state.grpc.get_lease_raw_state_by_block(
        ls_loan_closing.LS_contract_id.to_owned(),
        ls_loan_closing.Block - 1
    ),)?;
    let protocol = state
        .get_protocol_by_pool_id(&ls_opening.LS_loan_pool_id)
        .context(format!(
        "protocol not found {}",
        &ls_opening.LS_loan_pool_id
    ))?;

    if lease.FullClose.is_some() {
        let change_amount =
            change_amount.context("LS_Change not set in market-close")?;
        let data = vec![LS_Loan_Collect {
            LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
            LS_symbol: change_amount.ticker.to_owned(),
            LS_amount: BigDecimal::from_str(&change_amount.amount)?,
            LS_amount_stable: state
                .in_stable_by_date(
                    &change_amount.ticker,
                    &change_amount.amount,
                    Some(protocol.to_owned()),
                    &ls_loan_closing.LS_timestamp,
                )
                .await?,
        }];

        state.database.ls_loan_collect.insert_many(&data).await?;

        return Ok(());
    }

    if lease.PartialClose.is_some() {
        let mut data: HashMap<String, LS_Loan_Collect> = HashMap::new();

        let (balances, lease_state) = tokio::try_join!(
            state.grpc.get_balances_by_block(
                ls_loan_closing.LS_contract_id.to_owned(),
                ls_loan_closing.Block
            ),
            state.grpc.get_lease_state_by_block(
                ls_loan_closing.LS_contract_id.to_owned(),
                ls_loan_closing.Block
            )
        )?;

        if let Some(paid) = &lease_state.paid {
            data.insert(
                paid.amount.ticker.to_owned(),
                LS_Loan_Collect {
                    LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                    LS_symbol: paid.amount.ticker.to_owned(),
                    LS_amount: BigDecimal::from_str(&paid.amount.amount)?,
                    LS_amount_stable: state
                        .in_stable_by_date(
                            &paid.amount.ticker,
                            &paid.amount.amount,
                            Some(protocol.to_owned()),
                            &ls_loan_closing.LS_timestamp,
                        )
                        .await?,
                },
            );
        };

        if let Some(paid) = &lease_state.closing {
            data.insert(
                paid.amount.ticker.to_owned(),
                LS_Loan_Collect {
                    LS_contract_id: ls_loan_closing.LS_contract_id.to_owned(),
                    LS_symbol: paid.amount.ticker.to_owned(),
                    LS_amount: BigDecimal::from_str(&paid.amount.amount)?,
                    LS_amount_stable: state
                        .in_stable_by_date(
                            &paid.amount.ticker,
                            &paid.amount.amount,
                            Some(protocol.to_owned()),
                            &ls_loan_closing.LS_timestamp,
                        )
                        .await?,
                },
            );
        };

        for b in balances.balances {
            // Look up currency by bank_symbol (IBC denom)
            let item = state.config.hash_map_currencies.values().find(|item| {
                item.2 == b.denom.to_uppercase()
            });

            if let Some(c) = item {
                let c = c.clone();
                data.insert(
                    c.0.to_owned(),
                    LS_Loan_Collect {
                        LS_contract_id: ls_loan_closing
                            .LS_contract_id
                            .to_owned(),
                        LS_symbol: c.0.to_owned(),
                        LS_amount: BigDecimal::from_str(&b.amount)?,
                        LS_amount_stable: state
                            .in_stable_by_date(
                                &c.0,
                                &b.amount,
                                Some(protocol.to_owned()),
                                &ls_loan_closing.LS_timestamp,
                            )
                            .await?,
                    },
                );
            }
        }

        let native_currency = &state.config.native_currency;
        let contract_balances: Vec<LS_Loan_Collect> = data
            .into_values()
            .filter(|item| item.LS_symbol != *native_currency)
            .collect();

        state
            .database
            .ls_loan_collect
            .insert_many(&contract_balances)
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
) -> Result<LS_Loan_Closing, Error> {
    if is_sync_running() {
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
            if r#type == Loan_Closing_Status::Liquidation {
                let l = get_pnl_liquidated(
                    &app_state,
                    &lease,
                    contract.to_owned(),
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

            // Get position type from protocol registry
            let position_type = app_state
                .get_position_type_by_pool_id(&lease.LS_loan_pool_id)
                .await?;

            let loan = match position_type.as_str() {
                "Long" => {
                    get_pnl_long(
                        &app_state,
                        &lease,
                        contract.to_owned(),
                        block.to_owned(),
                        at.to_owned(),
                    )
                    .await?
                },
                _ => {
                    // Default to Short for any non-Long position type
                    get_pnl_short(
                        &app_state,
                        &lease,
                        contract.to_owned(),
                        block.to_owned(),
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
        None => {
            Ok(LS_Loan_Closing {
                LS_contract_id: contract.to_owned(),
                LS_amnt_stable: BigDecimal::from(0),
                LS_timestamp: at,
                Type: String::from(r#type),
                LS_amnt: BigDecimal::from(0),
                LS_pnl: BigDecimal::from(0),
                Block: block,
                Active: false,
            })
        },
    }
}

pub async fn get_pnl_long(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    contract: String,
    block: i64,
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

    let lease_amount_data = BigDecimal::from_str(&lease_status.amount.amount)?;
    let lease_amount = &lease_amount_data
        / BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?));

    let lease_debt = BigDecimal::from_str(&lease_status.principal_due.amount)?
        + BigDecimal::from_str(
            &lease_status.overdue_margin.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.overdue_interest.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.due_margin.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.due_interest.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0));

    let lease_debt =
        lease_debt / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));

    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let amnt_str = lease_amount.to_string();
    let amount_fn = app_state.in_stable_by_date(
        &lease_status.amount.ticker,
        &amnt_str,
        Some(protocol.to_owned()),
        &at,
    );

    let lease_downpayment = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, downpayment_currency.1.try_into()?));

    let fee_fn =
        get_fees(app_state, lease, protocol.to_owned()).map_err(Error::from);

    let repayments_fn = app_state
        .database
        .ls_repayment
        .get_by_contract(lease.LS_contract_id.to_owned())
        .map_err(Error::from);

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
                / BigDecimal::from(u64::pow(10, currency.1.try_into()?));
        }
    }

    let fee =
        fee / BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?));

    let pnl = &amount - lease_debt - repayment_value - lease_downpayment + fee;

    Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable: &amount
            * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)),
        LS_timestamp: at,
        LS_amnt: lease_amount_data,
        LS_pnl: pnl
            * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)),
        Active: true,
    })
}

pub async fn get_pnl_short(
    app_state: &AppState<State>,
    lease: &LS_Opening,
    contract: String,
    block: i64,
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

    let lease_amount_data = BigDecimal::from_str(&lease_status.amount.amount)?;
    let lease_amount = &lease_amount_data
        / BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?));

    let lease_debt = BigDecimal::from_str(&lease_status.principal_due.amount)?
        + BigDecimal::from_str(
            &lease_status.overdue_margin.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.overdue_interest.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.due_margin.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0))
        + BigDecimal::from_str(
            &lease_status.due_interest.unwrap_or_default().amount,
        )
        .unwrap_or(BigDecimal::from(0));

    let lease_debt =
        lease_debt / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));

    let protocol = app_state
        .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        .context(format!("protocol not found {}", &lease.LS_loan_pool_id))?;

    let amnt_str = lease_amount.to_string();
    let amount_fn = app_state.in_stable_by_date(
        &lease_status.amount.ticker,
        &amnt_str,
        Some(protocol.to_owned()),
        &at,
    );

    let lease_downpayment = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, downpayment_currency.1.try_into()?));

    let fee_fn =
        get_fees(app_state, lease, protocol.to_owned()).map_err(Error::from);

    let repayments_fn = app_state
        .database
        .ls_repayment
        .get_by_contract(lease.LS_contract_id.to_owned())
        .map_err(Error::from);

    let lpn_price_fn = app_state
        .database
        .mp_asset
        .get_price_by_date(&lpn_currency.0, Some(protocol.to_owned()), &at)
        .map_err(Error::from);

    let (fee, repayments, amount, (lpn_price,)) =
        tokio::try_join!(fee_fn, repayments_fn, amount_fn, lpn_price_fn)?;

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
                / BigDecimal::from(u64::pow(10, currency.1.try_into()?));
        }
    }

    let fee =
        fee / BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?));

    let pnl =
        &amount - lease_debt * lpn_price - repayment_value - lease_downpayment
            + fee;

    Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable: &amount
            * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)),
        LS_timestamp: at,
        LS_amnt: lease_amount_data,
        LS_pnl: pnl
            * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)),
        Active: true,
    })
}

pub async fn get_change_long(
    app_state: &AppState<State>,
    symbol: String,
    amnt: String,
    protocol: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<BigDecimal, Error> {
    let f1 = app_state.in_stable_by_date(
        &symbol,
        &amnt,
        Some(protocol.to_owned()),
        &start_date,
    );

    let f2 = app_state.in_stable_by_date(
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

    let ctrl_amount_stable = &lease.LS_cltr_amnt_stable
        / BigDecimal::from(u64::pow(10, ctrl_currency.1.try_into()?));

    let loan_amount_symbol_decimals =
        BigDecimal::from(u64::pow(10, loan_currency.1.try_into()?));

    let loan_amnt =
        (&lease.LS_loan_amnt / &loan_amount_symbol_decimals).to_string();

    let loan_amount_fn = app_state
        .in_stable_by_date(
            symbol,
            &loan_amnt,
            Some(protocol),
            &lease.LS_timestamp,
        )
        .map_err(Error::from);

    let loan_amount = loan_amount_fn.await?;

    let lpn_currency =
        app_state.get_currency_by_pool_id(&lease.LS_loan_pool_id)?;

    let loan_amount = (loan_amount * &loan_amount_symbol_decimals).round(0);
    let loan_amount_stable = &lease.LS_loan_amnt_stable
        / BigDecimal::from(u64::pow(10, lpn_currency.1.try_into()?));

    let total_loan_stable = ((loan_amount_stable + ctrl_amount_stable)
        * &loan_amount_symbol_decimals)
        .round(0);

    let fee = total_loan_stable - loan_amount;

    Ok(fee.round(0))
}

pub async fn get_pnl_liquidated(
    app_state: &AppState<State>,
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
        / BigDecimal::from(u64::pow(10, downpayment_currency.1.try_into()?));

    let repayments = app_state
        .database
        .ls_repayment
        .get_by_contract(lease.LS_contract_id.to_owned())
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
            / BigDecimal::from(u64::pow(10, currency.1.try_into()?));
    }

    let pnl = -(repayment_value + lease_downpayment);

    Ok(LS_Loan {
        LS_contract_id: contract.to_owned(),
        LS_amnt_stable: BigDecimal::from(0),
        LS_timestamp: at,
        LS_amnt: BigDecimal::from(0),
        LS_pnl: pnl
            * BigDecimal::from(u64::pow(10, lease_currency.1.try_into()?)),
        Active: true,
    })
}
