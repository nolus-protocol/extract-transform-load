use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Liquidation,
    types::LS_Liquidation_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_LS_Liquidation date parse {}",
            at_sec
        ))
    })?;
    let lease = app_state
        .database
        .ls_opening
        .get(item.to.to_owned())
        .await?;

    let protocol = match lease {
        Some(lease) => {
            app_state.get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        },
        None => None,
    };

    let ls_liquidation = LS_Liquidation {
        LS_liquidation_height: item.height.parse()?,
        LS_liquidation_idx: None,
        LS_contract_id: item.to,
        LS_symbol: item.liquidation_symbol.to_owned(),
        LS_amnt_stable: app_state
            .in_stabe_by_date(
                &item.liquidation_symbol,
                &item.liquidation_amount,
                protocol,
                &at,
            )
            .await?,
        LS_timestamp: at,
        LS_transaction_type: item.r#type,
        LS_prev_margin_stable: BigDecimal::from_str(
            &item.prev_margin_interest,
        )?,
        LS_prev_interest_stable: BigDecimal::from_str(
            &item.prev_loan_interest,
        )?,
        LS_current_margin_stable: BigDecimal::from_str(
            &item.curr_margin_interest,
        )?,
        LS_current_interest_stable: BigDecimal::from_str(
            &item.curr_loan_interest,
        )?,
        LS_principal_stable: BigDecimal::from_str(&item.principal)?,
    };

    let isExists = app_state
        .database
        .ls_liquidation
        .isExists(&ls_liquidation)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_liquidation
            .insert(ls_liquidation, transaction)
            .await?;
    }

    Ok(())
}
