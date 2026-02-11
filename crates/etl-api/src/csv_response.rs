use actix_web::{web::Bytes, HttpResponse};
use etl_core::error::Error;

use crate::error::ApiError;

/// Generate a CSV response from serializable data
pub fn to_csv_response<T: serde::Serialize>(
    data: &[T],
    filename: &str,
) -> Result<HttpResponse, ApiError> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for record in data {
        wtr.serialize(record).map_err(|e| {
            Error::ServerError(format!("CSV serialization error: {}", e))
        })?;
    }
    let csv_data = wtr
        .into_inner()
        .map_err(|e| Error::ServerError(format!("CSV writer error: {}", e)))?;
    let csv_string = String::from_utf8(csv_data)?;

    Ok(HttpResponse::Ok()
        .content_type("text/csv")
        .insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(csv_string))
}

/// Generate a streaming CSV response from serializable data.
pub fn to_streaming_csv_response<T: serde::Serialize>(
    data: Vec<T>,
    filename: &str,
) -> Result<HttpResponse, ApiError> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for record in &data {
        wtr.serialize(record).map_err(|e| {
            Error::ServerError(format!("CSV serialization error: {}", e))
        })?;
    }
    let csv_data = wtr
        .into_inner()
        .map_err(|e| Error::ServerError(format!("CSV writer error: {}", e)))?;

    let bytes = Bytes::from(csv_data);

    Ok(HttpResponse::Ok()
        .content_type("text/csv")
        .insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(bytes))
}
