use std::{
    env::VarError,
    fmt::Error as FMT_ERROR,
    io::Error as IO_ERROR,
    num::{ParseIntError, TryFromIntError as TRY_FROM_INT_ERROR},
    str::ParseBoolError as PARSE_BOOL_ERROR,
    string::{
        FromUtf8Error as FROM_UTF8_ERROR, ParseError as StringParseError,
    },
};

use anyhow::Error as ANYHOW_ERROR;
use base64::DecodeError as BASE64_DECODE_ERROR;
use bigdecimal::ParseBigDecimalError as BIG_DECIMAL_ERROR;
use cosmrs::{
    proto::prost::{DecodeError as DECODE_ERROR, EncodeError as ENCODE_ERROR},
    tx::ErrorReport,
};
use ece::Error as ECE_ERROR;
use jsonwebtoken::errors::Error as JWT_ERROR;
use reqwest::header::{
    InvalidHeaderName as INVALID_HEADER_NAME,
    InvalidHeaderValue as INVALID_HEADER_VALUE,
};
use reqwest::Error as REQWEST_ERROR;
use serde_json::Error as JSON_ERROR;
use sqlx::error::Error as SQL_ERROR;
use thiserror::Error;
use tokio::{
    sync::AcquireError as ACQUIRE_ERROR, task::JoinError, time::error::Elapsed,
};
use tonic::Status;
use tracing::subscriber::SetGlobalDefaultError as TRACING_GLOBAL_DEFAULT_ERROR;
use url::ParseError as URL_ERROR;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] IO_ERROR),

    #[error("{0}")]
    URL(#[from] URL_ERROR),

    #[error("{0}")]
    INT(#[from] ParseIntError),

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

    #[error("Duplicate field: {0}")]
    DuplicateField(String),

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

    #[error("Max filter length 10")]
    MaxFilterLength(),

    #[error("{0}")]
    ParseBoolError(#[from] PARSE_BOOL_ERROR),

    #[error("{0}")]
    DecodeError(#[from] DECODE_ERROR),

    #[error("Tracing error: {0}")]
    SetGlobalDefaultError(#[from] TRACING_GLOBAL_DEFAULT_ERROR),

    #[error("Decode datetime: {0}")]
    DecodeDateTimeError(String),

    #[error("Header to str error: {0}")]
    HeaderToStrError(String),

    #[error("{0}")]
    TryFromIntError(#[from] TRY_FROM_INT_ERROR),

    #[error("Protocol not found: {0}")]
    ProtocolError(String),

    #[error("{0}")]
    AnyHowError(#[from] ANYHOW_ERROR),

    #[error("Report error: {0}")]
    Report(#[from] ErrorReport),

    #[error("EncodeError error: {0}")]
    EncodeError(#[from] ENCODE_ERROR),

    #[error("FromUtf8Error error: {0}")]
    FromUtf8Error(#[from] FROM_UTF8_ERROR),

    #[error("Policy not supported")]
    AutoClosePosition(),

    #[error("Tonic status error: {0}")]
    TonicStatus(Box<Status>),

    #[error("Grps error: {0}")]
    GrpsError(String),

    #[error("{0}")]
    ReqwestError(#[from] REQWEST_ERROR),

    #[error("{0}")]
    InvalidHeaderName(#[from] INVALID_HEADER_NAME),

    #[error("{0}")]
    InvalidHeaderValue(#[from] INVALID_HEADER_VALUE),

    #[error("Invalid option {option}")]
    InvalidOption { option: String },

    #[error("{0}")]
    EceError(#[from] ECE_ERROR),

    #[error("{0}")]
    JWT(#[from] JWT_ERROR),

    #[error("InvalidHeader error: {0}")]
    InvalidHeader(String),

    #[error("Missing params: {0}")]
    MissingParams(String),

    #[error("{0}")]
    AcquireError(#[from] ACQUIRE_ERROR),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Error::TonicStatus(Box::new(status))
    }
}
