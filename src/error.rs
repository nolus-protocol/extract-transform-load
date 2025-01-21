use std::borrow::Cow;

use actix_web::ResponseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    URL(#[from] url::ParseError),

    #[error("{0}")]
    INT(#[from] std::num::ParseIntError),

    #[error("{0}")]
    SQL(#[from] sqlx::Error),

    #[error("{0}")]
    VAR(#[from] std::env::VarError),

    #[error("{0}")]
    STRING(#[from] std::string::ParseError),

    #[error("{0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("{0}")]
    TokioElapsedError(#[from] tokio::time::error::Elapsed),

    #[error("{0}")]
    FmtError(#[from] std::fmt::Error),

    #[error("{0}")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("{0}")]
    BigDecimalError(#[from] bigdecimal::ParseBigDecimalError),

    #[error("Field not exists: {0}")]
    FieldNotExist(String),

    #[error("Duplicate field: {0}")]
    DuplicateField(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("{0}")]
    JsonError(#[from] serde_json::error::Error),

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
    ParseBoolError(#[from] std::str::ParseBoolError),

    #[error("{0}")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Tracing error: {0}")]
    SetGlobalDefaultError(#[from] tracing::subscriber::SetGlobalDefaultError),

    #[error("Decode datetime: {0}")]
    DecodeDateTimeError(String),

    #[error("{0}")]
    HeaderToStrError(#[from] actix_web::http::header::ToStrError),

    #[error("{0}")]
    TryFromIntError(#[from] std::num::TryFromIntError),

    #[error("Protocol not found: {0}")]
    ProtocolError(Cow<'static, str>),

    #[error("{0}")]
    AnyHowError(#[from] anyhow::Error),

    #[error("Report error: {0}")]
    Report(#[from] cosmrs::ErrorReport),

    #[error("EncodeError error: {0}")]
    EncodeError(#[from] prost::EncodeError),

    #[error("FromUtf8Error error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
}

impl ResponseError for Error {}
