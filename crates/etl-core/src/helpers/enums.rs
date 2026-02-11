use std::{fmt, io, str::FromStr};

use crate::model::CosmosTypes;

pub enum EventsType {
    LS_Opening,
    LS_Closing,
    LS_Close_Position,
    LS_Repay,
    LS_Liquidation,
    LS_Liquidation_Warning,
    LS_Slippage_Anomaly,
    LS_Auto_Close_Position,
    Reserve_Cover_Loss,

    LP_deposit,
    LP_Withdraw,
    TR_Profit,
    TR_Rewards_Distribution,
}

impl EventsType {
    /// Returns the canonical string representation of this event type.
    /// Single source of truth for event type string mappings.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventsType::LS_Opening => "wasm-ls-open",
            EventsType::LS_Closing => "wasm-ls-close",
            EventsType::LS_Close_Position => "wasm-ls-close-position",
            EventsType::LS_Repay => "wasm-ls-repay",
            EventsType::LS_Liquidation => "wasm-ls-liquidation",
            EventsType::LS_Liquidation_Warning => "wasm-ls-liquidation-warning",
            EventsType::LS_Slippage_Anomaly => "wasm-ls-slippage-anomaly",
            EventsType::LS_Auto_Close_Position => "wasm-ls-auto-close-position",
            EventsType::Reserve_Cover_Loss => "wasm-reserve-cover-loss",
            EventsType::LP_deposit => "wasm-lp-deposit",
            EventsType::LP_Withdraw => "wasm-lp-withdraw",
            EventsType::TR_Profit => "wasm-tr-profit",
            EventsType::TR_Rewards_Distribution => "wasm-tr-rewards",
        }
    }
}

impl fmt::Display for EventsType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<EventsType> for String {
    fn from(value: EventsType) -> Self {
        value.as_str().to_string()
    }
}

impl FromStr for EventsType {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<EventsType, Self::Err> {
        match value {
            "wasm-ls-open" => Ok(EventsType::LS_Opening),
            "wasm-ls-close" => Ok(EventsType::LS_Closing),
            "wasm-ls-close-position" => Ok(EventsType::LS_Close_Position),
            "wasm-ls-repay" => Ok(EventsType::LS_Repay),
            "wasm-ls-liquidation" => Ok(EventsType::LS_Liquidation),
            "wasm-ls-liquidation-warning" => {
                Ok(EventsType::LS_Liquidation_Warning)
            },
            "wasm-ls-slippage-anomaly" => Ok(EventsType::LS_Slippage_Anomaly),
            "wasm-ls-auto-close-position" => {
                Ok(EventsType::LS_Auto_Close_Position)
            },
            "wasm-reserve-cover-loss" => Ok(EventsType::Reserve_Cover_Loss),
            "wasm-lp-deposit" => Ok(EventsType::LP_deposit),
            "wasm-lp-withdraw" => Ok(EventsType::LP_Withdraw),
            "wasm-tr-profit" => Ok(EventsType::TR_Profit),
            "wasm-tr-rewards" => Ok(EventsType::TR_Rewards_Distribution),
            _ => Err(io::Error::other("Message Type not supported")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Loan_Closing_Status {
    Repay,
    Liquidation,
    MarketClose,
    None,
}

impl fmt::Display for Loan_Closing_Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Loan_Closing_Status::Repay => write!(f, "repay"),
            Loan_Closing_Status::Liquidation => write!(f, "liquidation"),
            Loan_Closing_Status::MarketClose => {
                write!(f, "market-close")
            },
            Loan_Closing_Status::None => {
                write!(f, "none")
            },
        }
    }
}

impl From<Loan_Closing_Status> for String {
    fn from(value: Loan_Closing_Status) -> Self {
        match value {
            Loan_Closing_Status::Repay => String::from("repay"),
            Loan_Closing_Status::Liquidation => String::from("liquidation"),
            Loan_Closing_Status::MarketClose => String::from("market-close"),
            Loan_Closing_Status::None => String::from("none"),
        }
    }
}

impl FromStr for Loan_Closing_Status {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Loan_Closing_Status, Self::Err> {
        match value {
            "repay" => Ok(Loan_Closing_Status::Repay),
            "liquidation" => Ok(Loan_Closing_Status::Liquidation),
            "market-close" => Ok(Loan_Closing_Status::MarketClose),
            "none" => Ok(Loan_Closing_Status::None),
            _ => Err(io::Error::other("Loan_Closing_Status not supported")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Auto_Close_Strategies {
    TakeProfit,
    StopLoss,
}

impl fmt::Display for Auto_Close_Strategies {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Auto_Close_Strategies::TakeProfit => write!(f, "take-profit"),
            Auto_Close_Strategies::StopLoss => write!(f, "stop-loss"),
        }
    }
}

impl From<Auto_Close_Strategies> for String {
    fn from(value: Auto_Close_Strategies) -> Self {
        match value {
            Auto_Close_Strategies::TakeProfit => String::from("take-profit"),
            Auto_Close_Strategies::StopLoss => String::from("stop-loss"),
        }
    }
}

impl FromStr for Auto_Close_Strategies {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Auto_Close_Strategies, Self::Err> {
        match value {
            "take-profit" => Ok(Auto_Close_Strategies::TakeProfit),
            "stop-loss" => Ok(Auto_Close_Strategies::StopLoss),
            _ => Err(io::Error::other("Auto_Close_Strategies not supported")),
        }
    }
}

#[derive(Debug)]
pub enum Status {
    Subscribed,
    Unsubscribed,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Status::Subscribed => write!(f, "subscribed"),
            Status::Unsubscribed => write!(f, "unsubscribed"),
        }
    }
}

impl From<Status> for String {
    fn from(value: Status) -> Self {
        match value {
            Status::Subscribed => String::from("subscribed"),
            Status::Unsubscribed => String::from("unsubscribed"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Filter_Types {
    Transfers,
    Earn,
    Staking,
    Positions,
    PositionsIds,
}

impl fmt::Display for Filter_Types {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter_Types::Transfers => write!(f, "transfers"),
            Filter_Types::Earn => write!(f, "earn"),
            Filter_Types::Staking => write!(f, "staking"),
            Filter_Types::Positions => write!(f, "positions"),
            Filter_Types::PositionsIds => write!(f, "positions_ids"),
        }
    }
}

impl From<Filter_Types> for String {
    fn from(value: Filter_Types) -> Self {
        match value {
            Filter_Types::Transfers => String::from("transfers"),
            Filter_Types::Earn => String::from("earn"),
            Filter_Types::Staking => String::from("staking"),
            Filter_Types::Positions => String::from("positions"),
            Filter_Types::PositionsIds => String::from("positions_ids"),
        }
    }
}

impl FromStr for Filter_Types {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Filter_Types, Self::Err> {
        match value {
            "transfers" => Ok(Filter_Types::Transfers),
            "earn" => Ok(Filter_Types::Earn),
            "staking" => Ok(Filter_Types::Staking),
            "positions" => Ok(Filter_Types::Positions),
            "positions_ids" => Ok(Filter_Types::PositionsIds),
            _ => Err(io::Error::other("Filter_Types not supported")),
        }
    }
}

impl From<Filter_Types> for Vec<String> {
    fn from(value: Filter_Types) -> Self {
        match value {
            Filter_Types::Transfers => {
                vec![
                    CosmosTypes::MsgSend.to_string(),
                    CosmosTypes::MsgTransfer.to_string(),
                    CosmosTypes::MsgRecvPacket.to_string(),
                ]
            },
            Filter_Types::Earn => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
            Filter_Types::Staking => {
                vec![
                    CosmosTypes::MsgDelegate.to_string(),
                    CosmosTypes::MsgUndelegate.to_string(),
                    CosmosTypes::MsgBeginRedelegate.to_string(),
                    CosmosTypes::MsgWithdrawDelegatorReward.to_string(),
                ]
            },
            Filter_Types::Positions => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
            Filter_Types::PositionsIds => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
        }
    }
}
