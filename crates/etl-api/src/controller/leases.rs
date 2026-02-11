//! Lease-related API endpoints
//!
//! Endpoints for lease queries, historical data, liquidations, and repayments.

use actix_web::{get, web, HttpResponse, Responder};
use anyhow::Context as _;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::{future::join_all, TryFutureExt as _};
use serde::{Deserialize, Serialize};

use etl_core::{
    cache_keys,
    configuration::{AppState, State},
    error::Error,
    helpers::{
        build_cache_key, build_protocol_cache_key, cached_fetch,
        parse_period_months,
    },
    model::{LS_History, LS_Opening, TokenLoan},
};

use crate::csv_response::{to_csv_response, to_streaming_csv_response};

// =============================================================================
// Leases Search
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LeasesSearchQuery {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
    search: Option<String>,
}

#[get("/leases-search")]
pub async fn leases_search(
    state: web::Data<AppState<State>>,
    query: web::Query<LeasesSearchQuery>,
) -> Result<impl Responder, crate::error::ApiError> {
    let skip = query.skip.unwrap_or(0);
    let mut limit = query.limit.unwrap_or(10);

    if limit > 100 {
        limit = 100;
    }

    let address = query.address.to_lowercase().to_owned();
    let search = query.search.to_owned();

    let data = state
        .database
        .ls_opening
        .get_leases_addresses(address, search, skip, limit)
        .await?;
    let data: Vec<String> = data.iter().map(|e| e.0.to_owned()).collect();

    Ok(web::Json(data))
}

// =============================================================================
// Leases Monthly
// =============================================================================

#[get("/leases-monthly")]
pub async fn leases_monthly(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.leases_monthly,
        cache_keys::LEASES_MONTHLY,
        || async { Ok(state.database.ls_opening.get_leases_monthly().await?) },
    )
    .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Leased Assets
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LeasedAssetsQuery {
    protocol: Option<String>,
}

#[get("/leased-assets")]
pub async fn leased_assets(
    state: web::Data<AppState<State>>,
    query: web::Query<LeasedAssetsQuery>,
) -> Result<impl Responder, crate::error::ApiError> {
    let cache_key =
        build_protocol_cache_key("leased_assets", query.protocol.as_deref());
    let protocol = query.protocol.clone();

    let data =
        cached_fetch(&state.api_cache.leased_assets, &cache_key, || async {
            let data = if let Some(protocol_key) = &protocol {
                let protocol_key = protocol_key.to_uppercase();
                if let Some(protocol) = state.protocols.get(&protocol_key) {
                    state
                        .database
                        .ls_opening
                        .get_leased_assets(protocol.contracts.lpp.to_owned())
                        .await?
                } else {
                    vec![]
                }
            } else {
                state.database.ls_opening.get_leased_assets_total().await?
            };
            Ok(data)
        })
        .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Lease Value Stats
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LeaseValueStatsQuery {
    format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaseValueStat {
    pub asset: String,
    pub avg_value: BigDecimal,
    pub max_value: BigDecimal,
}

#[get("/lease-value-stats")]
pub async fn lease_value_stats(
    state: web::Data<AppState<State>>,
    query: web::Query<LeaseValueStatsQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.lease_value_stats,
        cache_keys::LEASE_VALUE_STATS,
        || async { state.database.ls_state.get_lease_value_stats().await },
    )
    .await?;

    let stats: Vec<LeaseValueStat> = data
        .into_iter()
        .map(|s| LeaseValueStat {
            asset: s.asset,
            avg_value: s.avg_value,
            max_value: s.max_value,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&stats, "lease-value-stats.csv"),
        _ => Ok(HttpResponse::Ok().json(stats)),
    }
}

// =============================================================================
// Loans by Token
// =============================================================================

#[get("/loans-by-token")]
pub async fn loans_by_token(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.loans_by_token,
        cache_keys::LOANS_BY_TOKEN,
        || async {
            let data = state.database.ls_state.get_loans_by_token().await?;
            let loans: Vec<TokenLoan> = data
                .into_iter()
                .map(|l| TokenLoan {
                    symbol: l.symbol,
                    value: l.value,
                })
                .collect();
            Ok(loans)
        },
    )
    .await?;

    Ok(web::Json(data))
}

// =============================================================================
// Loans Granted
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LoansGrantedQuery {
    format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoanGranted {
    pub asset: String,
    pub loan: BigDecimal,
}

#[get("/loans-granted")]
pub async fn loans_granted(
    state: web::Data<AppState<State>>,
    query: web::Query<LoansGrantedQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    let data = cached_fetch(
        &state.api_cache.loans_granted,
        cache_keys::LOANS_GRANTED,
        || async { state.database.ls_opening.get_loans_granted().await },
    )
    .await?;

    let loans: Vec<LoanGranted> = data
        .into_iter()
        .map(|l| LoanGranted {
            asset: l.asset,
            loan: l.loan,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&loans, "loans-granted.csv"),
        _ => Ok(HttpResponse::Ok().json(loans)),
    }
}

// =============================================================================
// Lease Opening(s) - Single or Batch
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LsOpeningQuery {
    /// Single lease contract ID (for detailed response with fees, history, etc.)
    lease: Option<String>,
    /// Comma-separated lease contract IDs (for batch response with basic info)
    leases: Option<String>,
}

/// Detailed response for single lease lookup
#[derive(Debug, Serialize, Deserialize)]
pub struct LsOpeningResponse {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
    pub lpn_price: BigDecimal,
    pub fee: BigDecimal,
    pub repayment_value: BigDecimal,
    pub history: Vec<LS_History>,
}

/// Simplified response for batch lease lookup
#[derive(Debug, Serialize, Deserialize)]
pub struct LsOpeningBatchItem {
    pub lease: LS_Opening,
    pub downpayment_price: BigDecimal,
}

/// Combined response enum for single vs batch
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum LsOpeningResult {
    Single(Box<Option<LsOpeningResponse>>),
    Batch(Vec<LsOpeningBatchItem>),
}

async fn get_ls_opening_batch_item(
    state: web::Data<AppState<State>>,
    lease: LS_Opening,
) -> Result<Option<LsOpeningBatchItem>, Error> {
    let result = state
        .database
        .ls_opening
        .get(lease.LS_contract_id.to_owned())
        .await?;
    if let Some(lease) = result {
        let protocol = state.get_protocol_by_pool_id(&lease.LS_loan_pool_id);
        let (downpayment_price,) = state
            .database
            .mp_asset
            .get_price_by_date(
                &lease.LS_asset_symbol,
                protocol,
                &lease.LS_timestamp,
            )
            .await?;
        return Ok(Some(LsOpeningBatchItem {
            lease,
            downpayment_price,
        }));
    }
    Ok(None)
}

#[get("/ls-opening")]
pub async fn ls_opening(
    state: web::Data<AppState<State>>,
    query: web::Query<LsOpeningQuery>,
) -> Result<impl Responder, crate::error::ApiError> {
    // Handle batch request (leases parameter)
    if let Some(leases_param) = &query.leases {
        let lease_ids: Vec<&str> = leases_param.split(',').collect();
        let data = state.database.ls_opening.get_leases(lease_ids).await?;
        let mut joins = Vec::new();

        for item in data {
            joins.push(get_ls_opening_batch_item(state.clone(), item))
        }

        let result = join_all(joins).await;
        let mut items: Vec<LsOpeningBatchItem> = vec![];

        for item in result.into_iter().flatten().flatten() {
            items.push(item);
        }

        return Ok(web::Json(LsOpeningResult::Batch(items)));
    }

    // Handle single lease request (lease parameter)
    if let Some(lease_id) = &query.lease {
        let result = state.database.ls_opening.get(lease_id.to_owned()).await?;
        if let Some(lease) = result {
            let protocol = state
                .get_protocol_by_pool_id(&lease.LS_loan_pool_id)
                .context(format!(
                    "protocol not found {}",
                    &lease.LS_loan_pool_id
                ))?;

            let base_currency = state
                .config
                .hash_map_pool_currency
                .get(&lease.LS_loan_pool_id)
                .context(format!(
                    "currency not found in hash map pool in protocol {}",
                    &protocol
                ))?;

            let base_currency = &base_currency.0;
            let repayments_fn = state
                .database
                .ls_repayment
                .get_by_contract(lease.LS_contract_id.to_owned());

            let ((downpayment_price,), (lpn_price,), fee, repayments, history) =
                tokio::try_join!(
                    state
                        .database
                        .mp_asset
                        .get_price_by_date(
                            &lease.LS_asset_symbol,
                            Some(protocol.to_owned()),
                            &lease.LS_timestamp,
                        )
                        .map_err(Error::from),
                    state
                        .database
                        .mp_asset
                        .get_price_by_date(
                            base_currency,
                            Some(protocol.to_owned()),
                            &lease.LS_timestamp,
                        )
                        .map_err(Error::from),
                    state
                        .get_fees(&lease, protocol.to_owned())
                        .map_err(Error::from),
                    repayments_fn.map_err(Error::from),
                    state
                        .database
                        .ls_opening
                        .get_lease_history(lease.LS_contract_id.to_owned())
                        .map_err(Error::from),
                )
                .context(format!(
                    "could not parse currencies in lease {}",
                    &lease.LS_contract_id
                ))?;

            let mut repayment_value = BigDecimal::from(0);

            for repayment in repayments {
                let currency = state
                    .config
                    .hash_map_currencies
                    .get(&repayment.LS_payment_symbol)
                    .context(format!(
                        "currency not found  {}",
                        &repayment.LS_payment_symbol
                    ))?;
                repayment_value += repayment.LS_payment_amnt_stable
                    / BigDecimal::from(u64::pow(10, currency.1.try_into()?));
            }

            return Ok(web::Json(LsOpeningResult::Single(Box::new(Some(
                LsOpeningResponse {
                    lease,
                    downpayment_price,
                    lpn_price,
                    fee,
                    repayment_value,
                    history,
                },
            )))));
        }

        return Ok(web::Json(LsOpeningResult::Single(Box::new(None))));
    }

    // No parameters provided - return empty batch
    Ok(web::Json(LsOpeningResult::Batch(vec![])))
}

// =============================================================================
// Loan Closings
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LsLoanClosingQuery {
    skip: Option<i64>,
    limit: Option<i64>,
    address: String,
}

#[get("/ls-loan-closing")]
pub async fn ls_loan_closing(
    state: web::Data<AppState<State>>,
    query: web::Query<LsLoanClosingQuery>,
) -> Result<impl Responder, crate::error::ApiError> {
    let skip = query.skip.unwrap_or(0);
    let mut limit = query.limit.unwrap_or(10);

    if limit > 10 {
        limit = 10;
    }

    let items = state
        .database
        .ls_loan_closing
        .get_leases(query.address.to_owned(), skip, limit)
        .await?;

    Ok(web::Json(items))
}

// =============================================================================
// Liquidations
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LiquidationsQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Liquidation {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub contract_id: String,
    pub user: Option<String>,
    pub transaction_type: Option<String>,
    pub liquidation_amount: BigDecimal,
    pub closed_loan: bool,
    pub down_payment: BigDecimal,
    pub loan: BigDecimal,
    pub liquidation_price: Option<BigDecimal>,
}

impl From<etl_core::dao::postgre::ls_liquidation::LiquidationData>
    for Liquidation
{
    fn from(
        l: etl_core::dao::postgre::ls_liquidation::LiquidationData,
    ) -> Self {
        Self {
            timestamp: l.timestamp,
            ticker: l.ticker,
            contract_id: l.contract_id,
            user: l.user,
            transaction_type: l.transaction_type,
            liquidation_amount: l.liquidation_amount,
            closed_loan: l.closed_loan,
            down_payment: l.down_payment,
            loan: l.loan,
            liquidation_price: l.liquidation_price,
        }
    }
}

#[get("/liquidations")]
pub async fn liquidations(
    state: web::Data<AppState<State>>,
    query: web::Query<LiquidationsQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.liquidations,
            cache_keys::LIQUIDATIONS,
            || async {
                state.database.ls_liquidation.get_all_liquidations().await
            },
        )
        .await?;

        let response: Vec<Liquidation> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(response, "liquidations.csv");
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key = build_cache_key("liquidations", period_str, query.from);

    let fetch = || async {
        state
            .database
            .ls_liquidation
            .get_liquidations_with_window(months, query.from)
            .await
    };

    let data = if query.from.is_none() {
        cached_fetch(&state.api_cache.liquidations, &cache_key, fetch).await?
    } else {
        fetch().await?
    };

    let response: Vec<Liquidation> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "liquidations.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

// =============================================================================
// Interest Repayments
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct InterestRepaymentsQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InterestRepayment {
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
    pub position_owner: String,
    pub position_type: String,
    pub event_type: String,
    pub loan_interest_repaid: BigDecimal,
    pub margin_interest_repaid: BigDecimal,
}

impl From<etl_core::dao::postgre::ls_repayment::InterestRepaymentData>
    for InterestRepayment
{
    fn from(
        r: etl_core::dao::postgre::ls_repayment::InterestRepaymentData,
    ) -> Self {
        Self {
            timestamp: r.timestamp,
            contract_id: r.contract_id,
            position_owner: r.position_owner,
            position_type: r.position_type,
            event_type: r.event_type,
            loan_interest_repaid: r.loan_interest_repaid,
            margin_interest_repaid: r.margin_interest_repaid,
        }
    }
}

#[get("/interest-repayments")]
pub async fn interest_repayments(
    state: web::Data<AppState<State>>,
    query: web::Query<InterestRepaymentsQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.interest_repayments,
            cache_keys::INTEREST_REPAYMENTS,
            || async {
                state
                    .database
                    .ls_repayment
                    .get_interest_repayments_with_window(None, None)
                    .await
            },
        )
        .await?;

        let response: Vec<InterestRepayment> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(response, "interest-repayments.csv");
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("interest_repayments", period_str, query.from);

    let data = cached_fetch(
        &state.api_cache.interest_repayments,
        &cache_key,
        || async {
            state
                .database
                .ls_repayment
                .get_interest_repayments_with_window(months, query.from)
                .await
        },
    )
    .await?;

    let response: Vec<InterestRepayment> =
        data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "interest-repayments.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

// =============================================================================
// Historically Opened
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoricallyOpenedQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyOpened {
    pub contract_id: String,
    pub user: String,
    pub leased_asset: String,
    pub opening_date: DateTime<Utc>,
    pub position_type: String,
    pub down_payment_amount: BigDecimal,
    pub down_payment_asset: String,
    pub loan: BigDecimal,
    pub total_position_amount_lpn: BigDecimal,
    pub price: Option<BigDecimal>,
    pub open: bool,
    pub liquidation_price: Option<BigDecimal>,
}

impl From<etl_core::dao::postgre::ls_opening::HistoricallyOpened>
    for HistoricallyOpened
{
    fn from(o: etl_core::dao::postgre::ls_opening::HistoricallyOpened) -> Self {
        Self {
            contract_id: o.contract_id,
            user: o.user,
            leased_asset: o.leased_asset,
            opening_date: o.opening_date,
            position_type: o.position_type,
            down_payment_amount: o.down_payment_amount,
            down_payment_asset: o.down_payment_asset,
            loan: o.loan,
            total_position_amount_lpn: o.total_position_amount_lpn,
            price: o.price,
            open: o.open,
            liquidation_price: o.liquidation_price,
        }
    }
}

#[get("/historically-opened")]
pub async fn historically_opened(
    state: web::Data<AppState<State>>,
    query: web::Query<HistoricallyOpenedQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.historically_opened,
            cache_keys::HISTORICALLY_OPENED,
            || async {
                state
                    .database
                    .ls_opening
                    .get_all_historically_opened()
                    .await
            },
        )
        .await?;

        let response: Vec<HistoricallyOpened> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(response, "historically-opened.csv");
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("historically_opened", period_str, query.from);

    let data = cached_fetch(
        &state.api_cache.historically_opened,
        &cache_key,
        || async {
            state
                .database
                .ls_opening
                .get_historically_opened_with_window(months, query.from)
                .await
        },
    )
    .await?;

    let response: Vec<HistoricallyOpened> =
        data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historically-opened.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

// =============================================================================
// Historically Repaid
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoricallyRepaidQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyRepaid {
    pub contract_id: String,
    pub symbol: String,
    pub loan: BigDecimal,
    pub total_repaid: BigDecimal,
    pub close_timestamp: Option<DateTime<Utc>>,
    pub loan_closed: String,
}

impl From<etl_core::dao::postgre::ls_repayment::HistoricallyRepaid>
    for HistoricallyRepaid
{
    fn from(
        r: etl_core::dao::postgre::ls_repayment::HistoricallyRepaid,
    ) -> Self {
        Self {
            contract_id: r.contract_id,
            symbol: r.symbol,
            loan: r.loan,
            total_repaid: r.total_repaid,
            close_timestamp: r.close_timestamp,
            loan_closed: r.loan_closed,
        }
    }
}

#[get("/historically-repaid")]
pub async fn historically_repaid(
    state: web::Data<AppState<State>>,
    query: web::Query<HistoricallyRepaidQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.historically_repaid,
            cache_keys::HISTORICALLY_REPAID,
            || async {
                state.database.ls_repayment.get_historically_repaid().await
            },
        )
        .await?;

        let response: Vec<HistoricallyRepaid> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(response, "historically-repaid.csv");
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("historically_repaid", period_str, query.from);

    let data = cached_fetch(
        &state.api_cache.historically_repaid,
        &cache_key,
        || async {
            state
                .database
                .ls_repayment
                .get_historically_repaid_with_window(months, query.from)
                .await
        },
    )
    .await?;

    let response: Vec<HistoricallyRepaid> =
        data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "historically-repaid.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

// =============================================================================
// Historically Liquidated
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoricallyLiquidatedQuery {
    format: Option<String>,
    period: Option<String>,
    from: Option<DateTime<Utc>>,
    export: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyLiquidated {
    pub contract_id: String,
    pub asset: String,
    pub loan: BigDecimal,
    pub total_liquidated: Option<BigDecimal>,
}

impl From<etl_core::dao::postgre::ls_liquidation::HistoricallyLiquidated>
    for HistoricallyLiquidated
{
    fn from(
        l: etl_core::dao::postgre::ls_liquidation::HistoricallyLiquidated,
    ) -> Self {
        Self {
            contract_id: l.contract_id,
            asset: l.asset,
            loan: l.loan,
            total_liquidated: l.total_liquidated,
        }
    }
}

#[get("/historically-liquidated")]
pub async fn historically_liquidated(
    state: web::Data<AppState<State>>,
    query: web::Query<HistoricallyLiquidatedQuery>,
) -> Result<HttpResponse, crate::error::ApiError> {
    // Handle export=true: return all data as streaming CSV
    if query.export.unwrap_or(false) {
        let data = cached_fetch(
            &state.api_cache.historically_liquidated,
            cache_keys::HISTORICALLY_LIQUIDATED,
            || async {
                state
                    .database
                    .ls_liquidation
                    .get_historically_liquidated()
                    .await
            },
        )
        .await?;

        let response: Vec<HistoricallyLiquidated> =
            data.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(
            response,
            "historically-liquidated.csv",
        );
    }

    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("3m");
    let cache_key =
        build_cache_key("historically_liquidated", period_str, query.from);

    let data = cached_fetch(
        &state.api_cache.historically_liquidated,
        &cache_key,
        || async {
            state
                .database
                .ls_liquidation
                .get_historically_liquidated_with_window(months, query.from)
                .await
        },
    )
    .await?;

    let response: Vec<HistoricallyLiquidated> =
        data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => {
            to_csv_response(&response, "historically-liquidated.csv")
        },
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}
