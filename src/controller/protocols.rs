//! Protocol and Currency API endpoints
//!
//! Endpoints for querying protocol configuration and currency information.
//! This data is dynamically loaded from blockchain contracts at startup.

use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

// =============================================================================
// Response Types
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtocolContracts {
    pub leaser: Option<String>,
    pub lpp: Option<String>,
    pub oracle: Option<String>,
    pub profit: Option<String>,
    pub reserve: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtocolInfo {
    pub name: String,
    pub network: Option<String>,
    pub dex: Option<String>,
    pub position_type: String,
    pub lpn_symbol: String,
    pub is_active: bool,
    pub contracts: ProtocolContracts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtocolsResponse {
    pub protocols: Vec<ProtocolInfo>,
    pub count: usize,
    pub active_count: i64,
    pub deprecated_count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrencyInfo {
    pub ticker: String,
    pub bank_symbol: Option<String>,
    pub decimal_digits: i16,
    pub group: Option<String>,
    pub is_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrenciesResponse {
    pub currencies: Vec<CurrencyInfo>,
    pub count: usize,
    pub active_count: i64,
    pub deprecated_count: i64,
}

// =============================================================================
// Protocol Endpoints
// =============================================================================

/// Get all protocols (active and deprecated)
#[get("/protocols")]
pub async fn get_protocols(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let protocols = state.database.protocol_registry.get_all().await?;
    let (active_count, deprecated_count) = state
        .database
        .protocol_registry
        .count_by_status()
        .await?;

    let protocol_infos: Vec<ProtocolInfo> = protocols
        .into_iter()
        .map(|p| ProtocolInfo {
            name: p.protocol_name,
            network: p.network,
            dex: p.dex,
            position_type: p.position_type,
            lpn_symbol: p.lpn_symbol,
            is_active: p.is_active,
            contracts: ProtocolContracts {
                leaser: p.leaser_contract,
                lpp: p.lpp_contract,
                oracle: p.oracle_contract,
                profit: p.profit_contract,
                reserve: p.reserve_contract,
            },
            deprecated_at: p.deprecated_at.map(|d| d.to_rfc3339()),
        })
        .collect();

    let count = protocol_infos.len();

    Ok(HttpResponse::Ok().json(ProtocolsResponse {
        protocols: protocol_infos,
        count,
        active_count,
        deprecated_count,
    }))
}

/// Get only active protocols
#[get("/protocols/active")]
pub async fn get_active_protocols(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let protocols = state.database.protocol_registry.get_active().await?;
    let (active_count, deprecated_count) = state
        .database
        .protocol_registry
        .count_by_status()
        .await?;

    let protocol_infos: Vec<ProtocolInfo> = protocols
        .into_iter()
        .map(|p| ProtocolInfo {
            name: p.protocol_name,
            network: p.network,
            dex: p.dex,
            position_type: p.position_type,
            lpn_symbol: p.lpn_symbol,
            is_active: p.is_active,
            contracts: ProtocolContracts {
                leaser: p.leaser_contract,
                lpp: p.lpp_contract,
                oracle: p.oracle_contract,
                profit: p.profit_contract,
                reserve: p.reserve_contract,
            },
            deprecated_at: None,
        })
        .collect();

    let count = protocol_infos.len();

    Ok(HttpResponse::Ok().json(ProtocolsResponse {
        protocols: protocol_infos,
        count,
        active_count,
        deprecated_count,
    }))
}

/// Get a single protocol by name
#[get("/protocols/{name}")]
pub async fn get_protocol_by_name(
    state: web::Data<AppState<State>>,
    path: web::Path<String>,
) -> Result<impl Responder, Error> {
    let name = path.into_inner();
    let protocol = state
        .database
        .protocol_registry
        .get_by_name(&name)
        .await?;

    match protocol {
        Some(p) => {
            let info = ProtocolInfo {
                name: p.protocol_name,
                network: p.network,
                dex: p.dex,
                position_type: p.position_type,
                lpn_symbol: p.lpn_symbol,
                is_active: p.is_active,
                contracts: ProtocolContracts {
                    leaser: p.leaser_contract,
                    lpp: p.lpp_contract,
                    oracle: p.oracle_contract,
                    profit: p.profit_contract,
                    reserve: p.reserve_contract,
                },
                deprecated_at: p.deprecated_at.map(|d| d.to_rfc3339()),
            };
            Ok(HttpResponse::Ok().json(info))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Protocol not found",
            "name": name
        }))),
    }
}

// =============================================================================
// Currency Endpoints
// =============================================================================

/// Get all currencies (active and deprecated)
#[get("/currencies")]
pub async fn get_currencies(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let currencies = state.database.currency_registry.get_all().await?;
    let (active_count, deprecated_count) = state
        .database
        .currency_registry
        .count_by_status()
        .await?;

    let currency_infos: Vec<CurrencyInfo> = currencies
        .into_iter()
        .map(|c| CurrencyInfo {
            ticker: c.ticker,
            bank_symbol: c.bank_symbol,
            decimal_digits: c.decimal_digits,
            group: c.group,
            is_active: c.is_active,
            deprecated_at: c.deprecated_at.map(|d| d.to_rfc3339()),
            last_seen_protocol: c.last_seen_protocol,
        })
        .collect();

    let count = currency_infos.len();

    Ok(HttpResponse::Ok().json(CurrenciesResponse {
        currencies: currency_infos,
        count,
        active_count,
        deprecated_count,
    }))
}

/// Get only active currencies
#[get("/currencies/active")]
pub async fn get_active_currencies(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let currencies = state.database.currency_registry.get_active().await?;
    let (active_count, deprecated_count) = state
        .database
        .currency_registry
        .count_by_status()
        .await?;

    let currency_infos: Vec<CurrencyInfo> = currencies
        .into_iter()
        .map(|c| CurrencyInfo {
            ticker: c.ticker,
            bank_symbol: c.bank_symbol,
            decimal_digits: c.decimal_digits,
            group: c.group,
            is_active: c.is_active,
            deprecated_at: None,
            last_seen_protocol: c.last_seen_protocol,
        })
        .collect();

    let count = currency_infos.len();

    Ok(HttpResponse::Ok().json(CurrenciesResponse {
        currencies: currency_infos,
        count,
        active_count,
        deprecated_count,
    }))
}

/// Get a single currency by ticker
#[get("/currencies/{ticker}")]
pub async fn get_currency_by_ticker(
    state: web::Data<AppState<State>>,
    path: web::Path<String>,
) -> Result<impl Responder, Error> {
    let ticker = path.into_inner();
    let currency = state
        .database
        .currency_registry
        .get_by_ticker(&ticker)
        .await?;

    match currency {
        Some(c) => {
            let info = CurrencyInfo {
                ticker: c.ticker,
                bank_symbol: c.bank_symbol,
                decimal_digits: c.decimal_digits,
                group: c.group,
                is_active: c.is_active,
                deprecated_at: c.deprecated_at.map(|d| d.to_rfc3339()),
                last_seen_protocol: c.last_seen_protocol,
            };
            Ok(HttpResponse::Ok().json(info))
        }
        None => Ok(HttpResponse::NotFound().json(serde_json::json!({
            "error": "Currency not found",
            "ticker": ticker
        }))),
    }
}
