use anyhow::Context as _;
use chrono::DateTime;
use cosmrs::proto::Timestamp;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::send_push::send,
    model::LS_Liquidation_Warning,
    types::{LS_Liquidation_Warning_Type, PushData, PUSH_TYPES},
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Warning_Type,
    time_stamp: Timestamp,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let seconds = time_stamp.seconds.try_into()?;
    let nanos = time_stamp.nanos.try_into()?;
    let time_stamp = DateTime::from_timestamp(seconds, nanos)
        .context("Could not parse time stamp")?;
    let level = item.level.parse()?;
    let ltv = item.ltv.parse()?;
    let contract = item.lease.to_owned();

    let ls_liquidation_warning = LS_Liquidation_Warning {
        Tx_Hash: Some(tx_hash),
        LS_contract_id: item.lease.to_owned(),
        LS_address_id: item.customer,
        LS_asset_symbol: item.lease_asset,
        LS_level: level,
        LS_ltv: ltv,
        LS_timestamp: time_stamp,
    };

    app_state
        .database
        .ls_liquidation_warning
        .insert_if_not_exists(ls_liquidation_warning, transaction)
        .await?;

    let push_data = match PUSH_TYPES::from(level) {
        PUSH_TYPES::Funding => PushData {
            r#type: PUSH_TYPES::Funding.to_string(),
            body: format!(
                r#"{{"level": {}, "ltv": {}, "position": "{}"}}"#,
                item.level, item.ltv, item.lease
            ),
        },
        PUSH_TYPES::FundingRecommended => PushData {
            r#type: PUSH_TYPES::FundingRecommended.to_string(),
            body: format!(
                r#"{{"level": {}, "ltv": {}, "position": "{}"}}"#,
                item.level, item.ltv, item.lease
            ),
        },
        PUSH_TYPES::FundNow => PushData {
            r#type: PUSH_TYPES::FundNow.to_string(),
            body: format!(
                r#"{{"level": {}, "ltv": {}, "position": "{}"}}"#,
                item.level, item.ltv, item.lease
            ),
        },
        _ => PushData {
            r#type: PUSH_TYPES::Unsupported.to_string(),
            body: format!(r#"{{}}"#),
        },
    };

    send(app_state.clone(), contract, push_data).await?;

    Ok(())
}
