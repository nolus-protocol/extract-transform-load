use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Loan_Closing_Status,
    model::LS_Loan_Closing,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    contract: String,
    r#type: Loan_Closing_Status,
    at: DateTime<Utc>,
    substract: BigDecimal,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let isExists = app_state
        .database
        .ls_loan_closing
        .isExists(contract.to_owned())
        .await?;

    if !isExists {
        let lease = app_state
            .database
            .ls_opening
            .get(contract.to_owned())
            .await?;

        if let Some(lease) = lease {
            let loan = app_state
                .database
                .ls_loan_closing
                .get_lease_amount(contract.to_owned())
                .await?
                - substract;

            let protocol =
                app_state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);
            let symbol = lease.LS_asset_symbol.to_owned();
            let loan_str = &loan.to_string();
            let loan_currency = app_state
                .config
                .hash_map_currencies
                .get(&lease.LS_asset_symbol)
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

            let loan_amount_symbol_decimals =
                BigDecimal::from(u64::pow(10, loan_currency.1.try_into()?));

            let loan_amnt = (&lease.LS_loan_amnt
                / &loan_amount_symbol_decimals)
                .to_string();

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

            let f3 = app_state.in_stabe_by_date(
                &symbol,
                &loan_amnt,
                protocol.to_owned(),
                &lease.LS_timestamp,
            );

            let ctrl_amount_stable = lease.LS_cltr_amnt_stable
                / BigDecimal::from(u64::pow(10, ctrl_currency.1.try_into()?));

            let loan_amount_stable =
                lease.LS_loan_amnt_stable / &loan_amount_symbol_decimals;

            let total_loan_stable = ((loan_amount_stable + ctrl_amount_stable)
                * &loan_amount_symbol_decimals)
                .round(0);

            let (open_amount, close_amount, loan_amount) =
                tokio::try_join!(f1, f2, f3)?;
            let loan_amount: BigDecimal =
                (loan_amount * &loan_amount_symbol_decimals).round(0);

            let fee = total_loan_stable - loan_amount;
            let pnl = &close_amount - &open_amount - fee;
            let ls_loan_closing = LS_Loan_Closing {
                LS_contract_id: contract.to_owned(),
                LS_symbol: lease.LS_asset_symbol.to_owned(),
                LS_amnt_stable: close_amount,
                LS_timestamp: at,
                Type: String::from(r#type),
                LS_amnt: loan,
                LS_pnl: pnl,
            };

            app_state
                .database
                .ls_loan_closing
                .insert(ls_loan_closing, transaction)
                .await?;
        }
    }

    Ok(())
}
