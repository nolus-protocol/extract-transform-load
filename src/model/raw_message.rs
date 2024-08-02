use std::{fmt, io, str::FromStr};

use anyhow::{anyhow, Context, Result};
use base64::prelude::*;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use cosmos_sdk_proto::{
    cosmos::{
        bank::v1beta1::MsgSend,
        distribution::v1beta1::MsgWithdrawDelegatorReward,
        gov::{v1::MsgVote, v1beta1::MsgVote as MsgVoteLegacy},
        staking::v1beta1::{MsgBeginRedelegate, MsgDelegate, MsgUndelegate},
    },
    cosmwasm::wasm::v1::MsgExecuteContract,
    ibc::{
        applications::transfer::v1::MsgTransfer,
        core::channel::v1::MsgRecvPacket,
    },
    Timestamp,
};
use cosmrs::{tx::Fee, Any};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

use crate::types::MsgReceivePacket;

#[derive(Debug, FromRow, Default, Serialize, Deserialize)]
pub struct Raw_Message {
    pub index: i32,
    pub from: String,
    pub to: String,
    pub r#type: String,
    pub value: String,
    pub tx_hash: String,
    pub block: i64,
    pub fee_amount: BigDecimal,
    pub fee_denom: Option<String>,
    pub memo: String,
    pub timestamp: DateTime<Utc>,
}

impl Raw_Message {
    pub fn from_any(
        index: i32,
        value: Any,
        tx_hash: String,
        block: i64,
        time_stamp: Timestamp,
        fee: Fee,
        memo: String,
        events: Vec<String>,
    ) -> Result<Raw_Message, anyhow::Error> {
        let k = CosmosTypes::from_str(&value.type_url)?;
        let seconds = time_stamp.seconds.try_into()?;
        let nanos = time_stamp.nanos.try_into()?;
        let coin: Option<&cosmrs::Coin> = fee.amount.first();
        let (fee_amount, fee_denom) = match coin {
            Some(f) => (f.amount, Some(f.denom.to_string())),
            None => (0, None),
        };

        match k {
            CosmosTypes::MsgSend => {
                let m = value.to_msg::<MsgSend>()?;
                Ok(Raw_Message {
                    index,
                    from: m.from_address,
                    to: m.to_address,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgTransfer => {
                let m = value.to_msg::<MsgTransfer>()?;
                Ok(Raw_Message {
                    index,
                    from: m.sender,
                    to: m.receiver,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgVoteLegacy => {
                let m = value.to_msg::<MsgVoteLegacy>()?;
                Ok(Raw_Message {
                    index,
                    from: m.voter,
                    to: m.proposal_id.to_string(),
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgVote => {
                let m = value.to_msg::<MsgVote>()?;
                Ok(Raw_Message {
                    index,
                    from: m.voter,
                    to: m.proposal_id.to_string(),
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgRecvPacket => {
                let m = value.to_msg::<MsgRecvPacket>()?;
                let packet = m.packet.context("unable to get packets")?;
                let data =
                    serde_json::from_slice::<MsgReceivePacket>(&packet.data)?;

                Ok(Raw_Message {
                    index,
                    from: data.sender,
                    to: data.receiver,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgWithdrawDelegatorReward => {
                let m = value.to_msg::<MsgWithdrawDelegatorReward>()?;
                Ok(Raw_Message {
                    index,
                    from: m.delegator_address,
                    to: m.validator_address,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgDelegate => {
                let m = value.to_msg::<MsgDelegate>()?;
                Ok(Raw_Message {
                    index,
                    from: m.delegator_address,
                    to: m.validator_address,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgBeginRedelegate => {
                let m = value.to_msg::<MsgBeginRedelegate>()?;
                Ok(Raw_Message {
                    index,
                    from: m.delegator_address,
                    to: m.validator_dst_address,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgUndelegate => {
                let m = value.to_msg::<MsgUndelegate>()?;
                Ok(Raw_Message {
                    index,
                    from: m.delegator_address,
                    to: m.validator_address,
                    r#type: value.type_url,
                    tx_hash,
                    block,
                    fee_amount: BigDecimal::from(fee_amount),
                    fee_denom,
                    timestamp: DateTime::from_timestamp(seconds, nanos)
                        .context("Could not parse time stamp")?,
                    value: BASE64_STANDARD.encode(value.value),
                    memo,
                })
            },
            CosmosTypes::MsgExecuteContract => {
                let m = value.to_msg::<MsgExecuteContract>()?;
                let msg: Value = serde_json::from_slice(&m.msg)?;
                for event in events {
                    if let Some(_) = msg.get(event) {
                        return Ok(Raw_Message {
                            index,
                            from: m.sender,
                            to: m.contract,
                            r#type: value.type_url,
                            tx_hash,
                            block,
                            fee_amount: BigDecimal::from(fee_amount),
                            fee_denom,
                            timestamp: DateTime::from_timestamp(seconds, nanos)
                                .context("Could not parse time stamp")?,
                            value: BASE64_STANDARD.encode(value.value),
                            memo,
                        });
                    }
                }
                Err(anyhow!("Missing event for subscribe in CosmosTypes::MsgExecuteContract"))
            },
        }
    }
}

#[derive(Debug)]
pub enum CosmosTypes {
    MsgSend,
    MsgTransfer,
    MsgVote,
    MsgVoteLegacy,
    MsgRecvPacket,
    MsgWithdrawDelegatorReward,
    MsgDelegate,
    MsgBeginRedelegate,
    MsgUndelegate,
    MsgExecuteContract,
}

impl fmt::Display for CosmosTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CosmosTypes::MsgSend => {
                write!(f, "/cosmos.bank.v1beta1.MsgSend")
            },
            CosmosTypes::MsgTransfer => {
                write!(f, "/ibc.applications.transfer.v1.MsgTransfer")
            },
            CosmosTypes::MsgVoteLegacy => {
                write!(f, "/cosmos.gov.v1beta1.MsgVote")
            },
            CosmosTypes::MsgVote => {
                write!(f, "/cosmos.gov.v1.MsgVote")
            },
            CosmosTypes::MsgRecvPacket => {
                write!(f, "/ibc.core.channel.v1.MsgRecvPacket")
            },
            CosmosTypes::MsgWithdrawDelegatorReward => {
                write!(
                    f,
                    "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward"
                )
            },
            CosmosTypes::MsgDelegate => {
                write!(f, "/cosmos.staking.v1beta1.MsgDelegate")
            },
            CosmosTypes::MsgBeginRedelegate => {
                write!(f, "/cosmos.staking.v1beta1.MsgBeginRedelegate")
            },
            CosmosTypes::MsgUndelegate => {
                write!(f, "/cosmos.staking.v1beta1.MsgUndelegate")
            },
            CosmosTypes::MsgExecuteContract => {
                write!(f, "/cosmwasm.wasm.v1.MsgExecuteContract")
            },
        }
    }
}

impl From<CosmosTypes> for String {
    fn from(value: CosmosTypes) -> Self {
        match value {
            CosmosTypes::MsgSend => {
                String::from("/cosmos.bank.v1beta1.MsgSend")
            },
            CosmosTypes::MsgTransfer => {
                String::from("/ibc.applications.transfer.v1.MsgTransfer")
            },
            CosmosTypes::MsgVoteLegacy => {
                String::from("/cosmos.gov.v1beta1.MsgVote")
            },
            CosmosTypes::MsgVote => String::from("/cosmos.gov.v1.MsgVote"),
            CosmosTypes::MsgRecvPacket => {
                String::from("/ibc.core.channel.v1.MsgRecvPacket")
            },
            CosmosTypes::MsgWithdrawDelegatorReward => String::from(
                "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
            ),
            CosmosTypes::MsgDelegate => {
                String::from("/cosmos.staking.v1beta1.MsgDelegate")
            },
            CosmosTypes::MsgBeginRedelegate => {
                String::from("/cosmos.staking.v1beta1.MsgBeginRedelegate")
            },
            CosmosTypes::MsgUndelegate => {
                String::from("/cosmos.staking.v1beta1.MsgUndelegate")
            },
            CosmosTypes::MsgExecuteContract => {
                String::from("/cosmwasm.wasm.v1.MsgExecuteContract")
            },
        }
    }
}

impl FromStr for CosmosTypes {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<CosmosTypes, Self::Err> {
        match value {
            "/cosmos.bank.v1beta1.MsgSend" => Ok(CosmosTypes::MsgSend),
            "/ibc.applications.transfer.v1.MsgTransfer" => {
                Ok(CosmosTypes::MsgTransfer)
            },
            "/cosmos.gov.v1beta1.MsgVote" => Ok(CosmosTypes::MsgVoteLegacy),
            "/cosmos.gov.v1.MsgVote" => Ok(CosmosTypes::MsgVote),
            "/ibc.core.channel.v1.MsgRecvPacket" => {
                Ok(CosmosTypes::MsgRecvPacket)
            },
            "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward" => {
                Ok(CosmosTypes::MsgWithdrawDelegatorReward)
            },
            "/cosmos.staking.v1beta1.MsgDelegate" => {
                Ok(CosmosTypes::MsgDelegate)
            },
            "/cosmos.staking.v1beta1.MsgBeginRedelegate" => {
                Ok(CosmosTypes::MsgBeginRedelegate)
            },
            "/cosmos.staking.v1beta1.MsgUndelegate" => {
                Ok(CosmosTypes::MsgUndelegate)
            },
            "/cosmwasm.wasm.v1.MsgExecuteContract" => {
                Ok(CosmosTypes::MsgExecuteContract)
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("CosmosTypes message not supported: {}", &value),
            )),
        }
    }
}
