use chrono::DateTime;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Closing,
    types::LS_Closing_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Closing_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!("Wasm_LS_close date parse {}", at_sec))
    })?;

    let ls_closing = LS_Closing {
        LS_contract_id: item.id,
        LS_timestamp: at,
    };
    let isExists = app_state.database.ls_closing.isExists(&ls_closing).await?;

    if !isExists {
        app_state
            .database
            .ls_closing
            .insert(ls_closing, transaction)
            .await?;
    }

    Ok(())
}
