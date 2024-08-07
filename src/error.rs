use actix_web::{http::header::ToStrError as HEADER_TO_STR_ERROR, ResponseError};
use base64::DecodeError as BASE64_DECODE_ERROR;
use bigdecimal::ParseBigDecimalError as BIG_DECIMAL_ERROR;
use cosmos_sdk_proto::prost::DecodeError as DECODE_ERROR;
use reqwest::Error as REQWEST_ERROR;
use serde_json::Error as JSON_ERROR;
use sqlx::error::Error as SQL_ERROR;
use std::fmt::Error as FMT_ERROR;
use std::num::TryFromIntError as TRY_FROM_INT_ERROR;
use std::{
    env::VarError, io::Error as IO_ERROR, num::ParseIntError,
    str::ParseBoolError as PARSE_BOOL_ERROR, string::ParseError as StringParseError,
};
use thiserror::Error;
use tokio::task::JoinError;
use tokio::time::error::Elapsed;
use tokio_tungstenite::tungstenite::{http::Error as HTTP_ERROR, Error as WS_ERROR};
use tracing::subscriber::SetGlobalDefaultError as TRACING_GLOBAL_DEFAULT_ERROR;
use url::ParseError as URL_ERROR;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] IO_ERROR),

    #[error("{0}")]
    WS(#[from] WS_ERROR),

    #[error("{0}")]
    URL(#[from] URL_ERROR),

    #[error("{0}")]
    INT(#[from] ParseIntError),

    #[error("{0}")]
    REQWEST(#[from] REQWEST_ERROR),

    #[error("{0}")]
    SQL(#[from] SQL_ERROR),

    #[error("{0}")]
    VAR(#[from] VarError),

    #[error("{0}")]
    STRING(#[from] StringParseError),

    #[error("{0}")]
    TokioJoinError(#[from] JoinError),

    #[error("{0}")]
    TokioElapsedError(#[from] Elapsed),

    #[error("{0}")]
    FmtError(#[from] FMT_ERROR),

    #[error("{0}")]
    Base64DecodeError(#[from] BASE64_DECODE_ERROR),

    #[error("{0}")]
    BigDecimalError(#[from] BIG_DECIMAL_ERROR),

    #[error("Field not exists: {0}")]
    FieldNotExist(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("{0}")]
    JsonError(#[from] JSON_ERROR),

    #[error("Currency not supported: {0}")]
    NotSupportedCurrency(String),

    #[error("Server end with error: {0}")]
    ServerError(String),

    #[error("Parse message error: {0}")]
    ParseMessage(String),

    #[error("Task message error: {0}")]
    TaskError(String),

    #[error("Detect more than one coin")]
    CoinLengthError(),

    #[error("{0}")]
    ParseBoolError(#[from] PARSE_BOOL_ERROR),

    #[error("{0}")]
    DecodeError(#[from] DECODE_ERROR),

    #[error("Tracing error: {0}")]
    SetGlobalDefaultError(#[from] TRACING_GLOBAL_DEFAULT_ERROR),

    #[error("Decode datetime: {0}")]
    DecodeDateTimeError(String),

    #[error("{0}")]
    HeaderToStrError(#[from] HEADER_TO_STR_ERROR),

    #[error("{0}")]
    TryFromIntError(#[from] TRY_FROM_INT_ERROR),

    #[error("Protocol not found: {0}")]
    ProtocolError(String),

    #[error("Http error: {0}")]
    HttpError(#[from] HTTP_ERROR),
}

impl ResponseError for Error {}
