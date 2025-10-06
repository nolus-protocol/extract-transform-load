use actix_cors::Cors;
use actix_files::Files;
use actix_web::{dev::Server, http::header, middleware, web, App, HttpServer};

use crate::{
    configuration::{AppState, State},
    controller::{
        blocks, borrow_apr, borrowed, buyback, buyback_total,
        deposit_suspension, distributed, earn_apr, get_position_debt_value,
        incentives_pool, leased_assets, leases, leases_monthly,
        ls_loan_closing, ls_opening, ls_openings, max_lp_ratio,
        max_ls_interest_7d, open_interest, open_position_value, optimal,
        pnl_over_time, prices, realized_pnl, realized_pnl_data,
        realized_pnl_stats, revenue, subscribe, supplied_borrowed_series,
        supplied_funds, test_push, total_tx_value, total_value_locked, txs,
        unrealized_pnl, unrealized_pnl_by_address, update_ls_loan_collect,
        utilization_level, version,
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
                    .service(total_value_locked::index)
                    .service(borrow_apr::index)
                    .service(supplied_borrowed_series::index)
                    .service(utilization_level::index)
                    .service(optimal::index)
                    .service(deposit_suspension::index)
                    .service(buyback::index)
                    .service(distributed::index)
                    .service(leased_assets::index)
                    .service(borrowed::index)
                    .service(revenue::index)
                    .service(incentives_pool::index)
                    .service(buyback_total::index)
                    .service(ls_opening::index)
                    .service(earn_apr::index)
                    .service(blocks::index)
                    .service(ls_openings::index)
                    .service(total_tx_value::index)
                    .service(version::index)
                    .service(max_ls_interest_7d::index)
                    .service(max_lp_ratio::index)
                    .service(txs::index)
                    .service(leases::index)
                    .service(prices::index)
                    .service(ls_loan_closing::index)
                    .service(realized_pnl::index)
                    .service(leases_monthly::index)
                    .service(open_position_value::index)
                    .service(open_interest::index)
                    .service(unrealized_pnl::index)
                    .service(unrealized_pnl_by_address::index)
                    .service(pnl_over_time::index)
                    .service(realized_pnl_stats::index)
                    .service(supplied_funds::index)
                    .service(get_position_debt_value::index)
                    .service(subscribe::get_index)
                    .service(subscribe::post_index)
                    .service(test_push::index)
                    .service(update_ls_loan_collect::index)
                    .service(realized_pnl_data::index),
            )
            .service(Files::new("/", static_dir).index_file("index.html"))
    })
    .bind((host, port))?
    .disable_signals()
    .run();
    Ok(server)
}
