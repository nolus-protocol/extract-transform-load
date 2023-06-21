use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Closing,
    types::LS_Closing_Type, dao::DataBase,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Closing_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {

    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(at_sec, 0), Utc);

    let ls_closing = LS_Closing {
        LS_contract_id: item.id,
        LS_timestamp: at,
    };

    app_state
        .database
        .ls_closing
        .insert(ls_closing, transaction)
        .await?;

    Ok(())
}
