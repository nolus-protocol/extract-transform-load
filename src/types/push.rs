//! Push notification types
//!
//! Types for push notification handling, subscription, and message formatting.

use serde::{Deserialize, Serialize};
use std::{fmt, io, str::FromStr};

// =============================================================================
// Push Message Types
// =============================================================================

#[derive(Debug, Clone)]
pub struct PushHeader {
    pub ttl: i64,
    pub urgency: Urgency,
}

#[derive(Debug, Clone)]
pub struct PushData {
    pub r#type: String,
    pub body: String,
}

impl fmt::Display for PushData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r#"{{"type": "{}", "data": {}}}"#, self.r#type, self.body)
    }
}

// =============================================================================
// Urgency Enum
// =============================================================================

#[derive(Debug, Clone)]
pub enum Urgency {
    VeryLow,
    Low,
    Normal,
    High,
}

impl fmt::Display for Urgency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Urgency::VeryLow => write!(f, "very-low"),
            Urgency::Low => write!(f, "low"),
            Urgency::Normal => write!(f, "normal"),
            Urgency::High => write!(f, "high"),
        }
    }
}

impl From<Urgency> for String {
    fn from(value: Urgency) -> Self {
        match value {
            Urgency::VeryLow => String::from("very-low"),
            Urgency::Low => String::from("low"),
            Urgency::Normal => String::from("normal"),
            Urgency::High => String::from("high"),
        }
    }
}

impl FromStr for Urgency {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Urgency, Self::Err> {
        match value {
            "very-low" => Ok(Urgency::VeryLow),
            "low" => Ok(Urgency::Low),
            "normal" => Ok(Urgency::Normal),
            "high" => Ok(Urgency::High),
            _ => Err(io::Error::other("Message Type not supported")),
        }
    }
}

// =============================================================================
// Push Types Enum
// =============================================================================

#[derive(Debug, Clone)]
pub enum PUSH_TYPES {
    Funding,
    FundingRecommended,
    FundNow,
    PartiallyLiquidated,
    FullyLiquidated,
    Unsupported,
}

impl fmt::Display for PUSH_TYPES {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PUSH_TYPES::Funding => write!(f, "Funding"),
            PUSH_TYPES::FundingRecommended => write!(f, "FundingRecommended"),
            PUSH_TYPES::FundNow => write!(f, "FundNow"),
            PUSH_TYPES::PartiallyLiquidated => write!(f, "PartiallyLiquidated"),
            PUSH_TYPES::FullyLiquidated => write!(f, "FullyLiquidated"),
            PUSH_TYPES::Unsupported => write!(f, "Unsupported"),
        }
    }
}

impl From<PUSH_TYPES> for String {
    fn from(value: PUSH_TYPES) -> Self {
        match value {
            PUSH_TYPES::Funding => String::from("Funding"),
            PUSH_TYPES::FundingRecommended => String::from("FundingRecommended"),
            PUSH_TYPES::FundNow => String::from("FundNow"),
            PUSH_TYPES::PartiallyLiquidated => String::from("PartiallyLiquidated"),
            PUSH_TYPES::FullyLiquidated => String::from("FullyLiquidated"),
            PUSH_TYPES::Unsupported => String::from("Unsupported"),
        }
    }
}

impl FromStr for PUSH_TYPES {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<PUSH_TYPES, Self::Err> {
        match value {
            "Funding" => Ok(PUSH_TYPES::Funding),
            "FundingRecommended" => Ok(PUSH_TYPES::FundingRecommended),
            "FundNow" => Ok(PUSH_TYPES::FundNow),
            "PartiallyLiquidated" => Ok(PUSH_TYPES::PartiallyLiquidated),
            "FullyLiquidated" => Ok(PUSH_TYPES::FullyLiquidated),
            "Unsupported" => Ok(PUSH_TYPES::Unsupported),
            _ => Err(io::Error::other("PUSH_TYPES not supported")),
        }
    }
}

impl std::convert::From<i16> for PUSH_TYPES {
    fn from(v: i16) -> Self {
        match v {
            1 => PUSH_TYPES::Funding,
            2 => PUSH_TYPES::FundingRecommended,
            3 => PUSH_TYPES::FundNow,
            _ => PUSH_TYPES::Unsupported,
        }
    }
}

// =============================================================================
// Subscription Types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct Subscription {
    pub address: String,
    pub data: SubscriptionData,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionData {
    pub endpoint: String,
    #[serde(alias = "expirationTime")]
    pub expiration_time: Option<i64>,
    pub keys: SubscriptionKeys,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionKeys {
    pub p256dh: String,
    pub auth: String,
}

// =============================================================================
// JWT Claims
// =============================================================================

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub aud: String,
    pub sub: String,
    pub exp: i64,
}
