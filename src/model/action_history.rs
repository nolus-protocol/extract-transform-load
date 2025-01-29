use std::{fmt, io, str::FromStr};

use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct Action_History {
    pub action_type: String,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::Type, Debug)]
pub enum Actions {
    MpAssetAction,
    AggregationAction,
}

impl fmt::Display for Actions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Actions::MpAssetAction => write!(f, "0"),
            Actions::AggregationAction => write!(f, "1"),
        }
    }
}

impl From<Actions> for String {
    fn from(value: Actions) -> Self {
        match value {
            Actions::MpAssetAction => String::from("0"),
            Actions::AggregationAction => String::from("1"),
        }
    }
}

impl FromStr for Actions {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Actions, Self::Err> {
        match value {
            "0" => Ok(Actions::MpAssetAction),
            "1" => Ok(Actions::AggregationAction),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Action Type not supported",
            )),
        }
    }
}
