use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    model::LS_Closing,
    types::LS_Closing_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Closing_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let at = parse_event_timestamp(&item.at)?;

    let ls_closing = LS_Closing {
        Tx_Hash: tx_hash,
        LS_contract_id: item.id,
        LS_timestamp: at,
    };

    app_state
        .database
        .ls_closing
        .insert_if_not_exists(ls_closing, transaction)
        .await?;

    Ok(())
}
