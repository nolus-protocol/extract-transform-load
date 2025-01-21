use std::{borrow::Cow, str::FromStr};

use chrono::{DateTime, Utc};
use sqlx::{
    encode::IsNull, error::BoxDynError, Database, Decode, Encode, FromRow,
    Type, ValueRef,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRow)]
pub struct Action_History {
    pub action_type: Actions,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Actions {
    MpAssetAction,
    AggregationAction,
}

impl<DB> Type<DB> for Actions
where
    DB: Database,
    str: Type<DB>,
{
    #[inline]
    fn type_info() -> DB::TypeInfo {
        str::type_info()
    }
}

impl<'q, DB> Encode<'q, DB> for Actions
where
    DB: Database,
    &'q str: Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        str::encode_by_ref(self.as_str(), buf)
    }
}

impl<'r, DB> Decode<'r, DB> for Actions
where
    DB: Database,
    Cow<'r, str>: Decode<'r, DB>,
{
    fn decode(
        value: <DB as Database>::ValueRef<'r>,
    ) -> Result<Self, BoxDynError> {
        Cow::<'r, str>::decode(value)
            .and_then(|f| f.parse().map_err(|error| Box::new(error) as Box<_>))
    }
}

impl Actions {
    const MP_ASSET_ACTION: &'static str = "0";

    const AGGREGATION_ACTION: &'static str = "1";

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MpAssetAction => Self::MP_ASSET_ACTION,
            Self::AggregationAction => Self::AGGREGATION_ACTION,
        }
    }
}

impl FromStr for Actions {
    type Err = UnknownActionType;

    fn from_str(value: &str) -> Result<Actions, Self::Err> {
        match value {
            Self::MP_ASSET_ACTION => Ok(Actions::MpAssetAction),
            Self::AGGREGATION_ACTION => Ok(Actions::AggregationAction),
            _ => Err(UnknownActionType),
        }
    }
}

#[derive(Debug, Error)]
#[error("Unknown action type")]
struct UnknownActionType;
