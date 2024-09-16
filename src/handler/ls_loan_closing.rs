use anyhow::Result;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Loan_Closing,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Loan_Closing,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let isExists = app_state.database.ls_loan_closing.isExists(&item).await?;

    if !isExists {
        app_state
            .database
            .ls_loan_closing
            .insert(item, transaction)
            .await?;
    }

    Ok(())
}
