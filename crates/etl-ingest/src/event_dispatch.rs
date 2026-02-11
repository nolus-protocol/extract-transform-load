use anyhow::Context as _;
use cosmrs::{
    proto::{
        cosmos::base::abci::v1beta1::TxResponse, tendermint::abci::Event,
        Timestamp,
    },
    Tx,
};
use sqlx::Transaction;
use std::str::FromStr;

use etl_core::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::EventsType,
    model::{Block, RawMsgParams, RawTxParams, Raw_Message},
};

use crate::{
    event_parsing::*,
    handler::{
        wasm_lp_deposit, wasm_lp_withdraw, wasm_ls_auto_close_position,
        wasm_ls_close, wasm_ls_close_position, wasm_ls_liquidation,
        wasm_ls_liquidation_warning, wasm_ls_open, wasm_ls_repay,
        wasm_ls_slippage_anomaly, wasm_reserve_cover_loss, wasm_tr_profit,
        wasm_tr_rewards,
    },
};

pub async fn parse_event(
    app_state: AppState<State>,
    event: &Event,
    index: usize,
    time_stamp: Timestamp,
    tx_hash: String,
    height: i64,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    if let Ok(t) = EventsType::from_str(&event.r#type) {
        match t {
            EventsType::LS_Opening => {
                let wasm_ls_opening = parse_wasm_ls_open(&event.attributes)?;
                wasm_ls_open::parse_and_insert(
                    &app_state,
                    wasm_ls_opening,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Closing => {
                let wasm_ls_closing = parse_wasm_ls_close(&event.attributes)?;
                wasm_ls_close::parse_and_insert(
                    &app_state,
                    wasm_ls_closing,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Close_Position => {
                let wasm_ls_close_position =
                    parse_wasm_ls_close_position(&event.attributes)?;
                if let Some(item) = wasm_ls_close_position {
                    wasm_ls_close_position::parse_and_insert(
                        &app_state, item, tx_hash, height, tx,
                    )
                    .await?;
                }
            },
            EventsType::LS_Repay => {
                let wasm_ls_repay = parse_wasm_ls_repayment(&event.attributes)?;
                wasm_ls_repay::parse_and_insert(
                    &app_state,
                    wasm_ls_repay,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Liquidation => {
                let wasm_ls_liquidation =
                    parse_wasm_ls_liquidation(&event.attributes)?;
                wasm_ls_liquidation::parse_and_insert(
                    &app_state,
                    wasm_ls_liquidation,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Liquidation_Warning => {
                let ls_liquidation_warning =
                    parse_wasm_ls_liquidation_warning(&event.attributes)?;
                wasm_ls_liquidation_warning::parse_and_insert(
                    &app_state,
                    ls_liquidation_warning,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Slippage_Anomaly => {
                let ls_slippage_anomaly =
                    parse_wasm_ls_slippage_anomaly(&event.attributes)?;
                wasm_ls_slippage_anomaly::parse_and_insert(
                    &app_state,
                    ls_slippage_anomaly,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Auto_Close_Position => {
                let ls_auto_close_position =
                    parse_wasm_ls_auto_close_position(&event.attributes)?;
                wasm_ls_auto_close_position::parse_and_insert(
                    &app_state,
                    ls_auto_close_position,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::Reserve_Cover_Loss => {
                let reserve_cover_loss =
                    parse_wasm_reserve_cover_loss(&event.attributes)?;
                wasm_reserve_cover_loss::parse_and_insert(
                    &app_state,
                    reserve_cover_loss,
                    index,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LP_deposit => {
                let wasm_lp_deposit = parse_wasm_lp_deposit(&event.attributes)?;
                wasm_lp_deposit::parse_and_insert(
                    &app_state,
                    wasm_lp_deposit,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LP_Withdraw => {
                let wasm_lp_withdraw =
                    parse_wasm_lp_withdraw(&event.attributes)?;
                wasm_lp_withdraw::parse_and_insert(
                    &app_state,
                    wasm_lp_withdraw,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::TR_Profit => {
                let wasm_tr_profit = parse_wasm_tr_profit(&event.attributes)?;
                wasm_tr_profit::parse_and_insert(
                    &app_state,
                    wasm_tr_profit,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::TR_Rewards_Distribution => {
                let wasm_tr_rewards_distribution =
                    parse_wasm_tr_rewards_distribution(&event.attributes)?;
                wasm_tr_rewards::parse_and_insert(
                    &app_state,
                    wasm_tr_rewards_distribution,
                    index,
                    tx_hash,
                    tx,
                )
                .await?;
            },
        }
    }
    Ok(())
}

pub async fn insert_txs(
    app_state: AppState<State>,
    txs: Vec<Option<TxResponse>>,
    height: i64,
    time_stamp: Timestamp,
) -> Result<bool, Error> {
    let block = app_state.database.block.get_one(height).await?;

    if block.is_none() {
        let mut tx = app_state.database.pool.begin().await?;
        for tx_results in txs.into_iter().flatten() {
            let hash = tx_results.txhash.to_owned();
            let tx_data =
                tx_results.tx.context("could not find Any message")?;
            parse_raw_tx(
                app_state.clone(),
                RawTxParams {
                    tx_hash: tx_results.txhash,
                    tx_data,
                    height,
                    code: tx_results.code,
                    time_stamp,
                    tx_events: &tx_results.events,
                },
                &mut tx,
            )
            .await?;
            for (index, event) in tx_results.events.iter().enumerate() {
                parse_event(
                    app_state.clone(),
                    event,
                    index,
                    time_stamp,
                    hash.to_owned(),
                    height,
                    &mut tx,
                )
                .await?;
            }
        }

        app_state
            .database
            .block
            .insert(Block { id: height }, &mut tx)
            .await?;

        tx.commit().await?;
    }

    Ok(true)
}

pub async fn parse_raw_tx(
    app_state: AppState<State>,
    params: RawTxParams<'_>,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let c = Tx::from_bytes(&params.tx_data.value)?;
    for (index, msg) in c.body.messages.iter().enumerate() {
        let fee = c.auth_info.fee.clone();
        let memo = c.body.memo.to_owned();
        let msg: Result<Raw_Message, anyhow::Error> =
            Raw_Message::from_any(RawMsgParams {
                index: index.try_into()?,
                value: msg.clone(),
                tx_hash: params.tx_hash.clone(),
                block: params.height,
                time_stamp: params.time_stamp,
                fee,
                memo,
                events: app_state.config.events_subscribe.clone(),
                tx_events: params.tx_events,
                code: params.code,
            });

        if let Ok(msg) = msg {
            app_state
                .database
                .raw_message
                .insert_if_not_exists(msg, tx)
                .await?;
        }
    }

    Ok(())
}
