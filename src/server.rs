use actix_cors::Cors;
use actix_files::Files;
use actix_web::{dev::Server, http::header, middleware, web, App, HttpServer};

use crate::{
    configuration::{AppState, State},
    controller::{
        leases, liquidity, metrics, misc, pnl, positions, protocols, treasury,
    },
    error::Error,
};

pub async fn server_task(app_state: &AppState<State>) -> Result<(), Error> {
    let app = app_state.clone();
    tokio::spawn(async move {
        let server = init_server(app)?;
        server.await?;
        Ok(())
    })
    .await?
}

fn init_server(app_state: AppState<State>) -> Result<Server, Error> {
    let host = app_state.config.server_host.to_owned();
    let port = app_state.config.port;

    let server = HttpServer::new(move || {
        let app = app_state.clone();
        let static_dir = app_state.config.static_dir.to_owned();
        let allowed_cors = String::from("*");
        let cors_access_all =
            app.config.allowed_origins.contains(&allowed_cors);
        let cors = Cors::default()
            .allowed_origin_fn(move |origin, _| {
                if cors_access_all {
                    return true;
                }
                let allowed = &app.config.allowed_origins;
                if let Ok(origin) = origin.to_str() {
                    return allowed.contains(&origin.to_owned());
                }
                false
            })
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![header::AUTHORIZATION, header::ACCEPT])
            .allowed_header(header::CONTENT_TYPE);

        App::new()
            .wrap(cors)
            .wrap(middleware::Compress::default())
            .app_data(web::Data::new(app_state.clone()))
            .app_data(web::JsonConfig::default().limit(4096))
            .service(
                web::scope("/api")
                    // Treasury endpoints
                    .service(treasury::revenue)
                    .service(treasury::revenue_series)
                    .service(treasury::distributed)
                    .service(treasury::buyback)
                    .service(treasury::buyback_total)
                    .service(treasury::incentives_pool)
                    .service(treasury::earnings)
                    // Metrics endpoints
                    .service(metrics::total_value_locked)
                    .service(metrics::total_tx_value)
                    .service(metrics::supplied_funds)
                    .service(metrics::open_interest)
                    .service(metrics::open_position_value)
                    .service(metrics::borrowed)
                    .service(metrics::supplied_borrowed_history)
                    .service(metrics::monthly_active_wallets)
                    // PnL endpoints
                    .service(pnl::realized_pnl)
                    .service(pnl::realized_pnl_data)
                    .service(pnl::realized_pnl_stats)
                    .service(pnl::realized_pnl_wallet)
                    .service(pnl::unrealized_pnl)
                    .service(pnl::unrealized_pnl_by_address)
                    .service(pnl::pnl_over_time)
                    // Lease endpoints
                    .service(leases::leases_search)
                    .service(leases::leases_monthly)
                    .service(leases::leased_assets)
                    .service(leases::lease_value_stats)
                    .service(leases::loans_by_token)
                    .service(leases::loans_granted)
                    .service(leases::ls_opening)
                    .service(leases::ls_loan_closing)
                    .service(leases::liquidations)
                    .service(leases::interest_repayments)
                    .service(leases::historically_opened)
                    .service(leases::historically_repaid)
                    .service(leases::historically_liquidated)
                    // Position endpoints
                    .service(positions::positions)
                    .service(positions::position_buckets)
                    .service(positions::daily_positions)
                    .service(positions::open_positions_by_token)
                    .service(positions::position_debt_value)
                    // Liquidity endpoints
                    .service(liquidity::pools)
                    .service(liquidity::lp_withdraw)
                    .service(liquidity::current_lenders)
                    .service(liquidity::historical_lenders)
                    // Misc endpoints
                    .service(misc::prices)
                    .service(misc::blocks)
                    .service(misc::txs)
                    .service(misc::history_stats)
                    .service(misc::version)
                    .service(misc::subscribe_get)
                    .service(misc::subscribe_post)
                    .service(misc::test_push)
                    // Protocol & Currency endpoints
                    .service(protocols::get_protocols)
                    .service(protocols::get_active_protocols)
                    .service(protocols::get_protocol_by_name)
                    .service(protocols::get_currencies)
                    .service(protocols::get_active_currencies)
                    .service(protocols::get_currency_by_ticker),
            )
            .service(Files::new("/", static_dir).index_file("index.html"))
    })
    .bind((host, port))?
    .disable_signals()
    .run();
    Ok(server)
}
