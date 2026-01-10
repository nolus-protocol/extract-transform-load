use actix_cors::Cors;
use actix_web::{dev::Server, http::header, middleware, web, App, HttpServer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    configuration::{AppState, State},
    openapi::ApiDoc,
    controller::{
        backfill_ls_opening, blocks, borrow_apr, borrowed, buyback, buyback_total,
        current_lenders, daily_positions, deposit_suspension, distributed, earn_apr,
        earnings, get_position_debt_value, historical_lenders, historically_liquidated,
        historically_opened, historically_repaid, history_stats, incentives_pool,
        interest_repayments, leased_assets, lease_value_stats, leases, leases_monthly,
        leases_search, liquidations, loans_by_token, loans_granted, lp_withdraw,
        ls_loan_closing, ls_opening, ls_openings, monthly_active_wallets, open_interest,
        open_position_value, open_positions_by_token, optimal, pnl_over_time,
        position_buckets, positions, prices, realized_pnl, realized_pnl_data,
        realized_pnl_stats, realized_pnl_wallet, revenue, revenue_series, subscribe,
        supplied_borrowed_series, supplied_funds, test_push, total_tx_value,
        total_value_locked, txs, unrealized_pnl, unrealized_pnl_by_address,
        update_raw_txs, utilization_level, utilization_levels, version,
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
                    .service(total_value_locked::index)
                    .service(borrow_apr::index)
                    .service(supplied_borrowed_series::index)
                    .service(utilization_level::index)
                    .service(utilization_levels::index)
                    .service(optimal::index)
                    .service(deposit_suspension::index)
                    .service(buyback::index)
                    .service(distributed::index)
                    .service(leased_assets::index)
                    .service(borrowed::index)
                    .service(revenue::index)
                    .service(revenue_series::index)
                    .service(daily_positions::index)
                    .service(incentives_pool::index)
                    .service(buyback_total::index)
                    .service(ls_opening::index)
                    .service(earn_apr::index)
                    .service(blocks::index)
                    .service(ls_openings::index)
                    .service(total_tx_value::index)
                    .service(version::index)

                    .service(txs::index)
                    .service(leases::index)
                    .service(prices::index)
                    .service(ls_loan_closing::index)
                    .service(realized_pnl::index)
                    .service(leases_monthly::index)
                    .service(monthly_active_wallets::index)
                    .service(open_position_value::index)
                    .service(open_positions_by_token::index)
                    .service(open_interest::index)
                    .service(unrealized_pnl::index)
                    .service(unrealized_pnl_by_address::index)
                    .service(pnl_over_time::index)
                    .service(position_buckets::index)
                    .service(realized_pnl_stats::index)
                    .service(supplied_funds::index)
                    .service(get_position_debt_value::index)
                    .service(subscribe::get_index)
                    .service(subscribe::post_index)
                    .service(test_push::index)
                    .service(realized_pnl_data::index)
                    .service(history_stats::index)
                    .service(leases_search::index)
                    .service(loans_by_token::index)
                    .service(earnings::index)
                    .service(lp_withdraw::index)
                    .service(update_raw_txs::index)
                    .service(current_lenders::index)
                    .service(liquidations::index)
                    .service(lease_value_stats::index)
                    .service(historical_lenders::index)
                    .service(loans_granted::index)
                    .service(historically_liquidated::index)
                    .service(historically_liquidated::export)
                    .service(historically_repaid::index)
                    .service(historically_repaid::export)
                    .service(historically_opened::index)
                    .service(historically_opened::export)
                    .service(interest_repayments::index)
                    .service(interest_repayments::export)
                    .service(realized_pnl_wallet::index)
                    .service(realized_pnl_wallet::export)
                    .service(positions::index)
                    .service(positions::export)
                    .service(liquidations::export)
                    .service(historical_lenders::export)
                    .service(backfill_ls_opening::index),
            )
            .service(
                SwaggerUi::new("/{_:.*}")
                    .url("/api-docs/openapi.json", ApiDoc::openapi()),
            )
    })
    .bind((host, port))?
    .disable_signals()
    .run();
    Ok(server)
}
