use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use etl_core::error::Error;

/// Wrapper around core Error that implements actix_web::ResponseError
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ApiError(#[from] pub Error);

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError(Error::from(e))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError(Error::from(e))
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError(Error::from(e))
    }
}

impl From<std::num::TryFromIntError> for ApiError {
    fn from(e: std::num::TryFromIntError) -> Self {
        ApiError(Error::from(e))
    }
}

impl From<std::string::FromUtf8Error> for ApiError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        ApiError(Error::from(e))
    }
}

impl From<actix_web::http::header::ToStrError> for ApiError {
    fn from(e: actix_web::http::header::ToStrError) -> Self {
        ApiError(Error::HeaderToStrError(e.to_string()))
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match &self.0 {
            // 400 Bad Request - client sent invalid input
            Error::FieldNotExist(_)
            | Error::DuplicateField(_)
            | Error::MissingParams(_)
            | Error::MaxFilterLength()
            | Error::InvalidOption { .. }
            | Error::ParseBoolError(_)
            | Error::INT(_)
            | Error::BigDecimalError(_)
            | Error::DecodeDateTimeError(_)
            | Error::Base64DecodeError(_) => StatusCode::BAD_REQUEST,

            // 404 Not Found - requested resource does not exist
            Error::NotSupportedCurrency(_) | Error::ProtocolError(_) => {
                StatusCode::NOT_FOUND
            },

            // 502 Bad Gateway - upstream service error
            Error::ReqwestError(_)
            | Error::TonicStatus(_)
            | Error::GrpsError(_) => StatusCode::BAD_GATEWAY,

            // 504 Gateway Timeout - upstream timed out
            Error::TokioElapsedError(_) => StatusCode::GATEWAY_TIMEOUT,

            // 500 Internal Server Error - everything else
            Error::ConfigurationError(_)
            | Error::SQL(_)
            | Error::Io(_)
            | Error::URL(_)
            | Error::VAR(_)
            | Error::STRING(_)
            | Error::TokioJoinError(_)
            | Error::FmtError(_)
            | Error::JsonError(_)
            | Error::ServerError(_)
            | Error::ParseMessage(_)
            | Error::TaskError(_)
            | Error::CoinLengthError()
            | Error::DecodeError(_)
            | Error::SetGlobalDefaultError(_)
            | Error::HeaderToStrError(_)
            | Error::TryFromIntError(_)
            | Error::AnyHowError(_)
            | Error::Report(_)
            | Error::EncodeError(_)
            | Error::FromUtf8Error(_)
            | Error::AutoClosePosition()
            | Error::InvalidHeaderName(_)
            | Error::InvalidHeaderValue(_)
            | Error::InvalidHeader(_)
            | Error::EceError(_)
            | Error::JWT(_)
            | Error::AcquireError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status = self.status_code();
        let body = serde_json::json!({
            "error": status.canonical_reason().unwrap_or("Unknown"),
            "message": self.0.to_string(),
            "status": status.as_u16(),
        });
        HttpResponse::build(status).json(body)
    }
}
