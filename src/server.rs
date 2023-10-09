use actix_cors::Cors;
use actix_files::Files;
use actix_web::{dev::Server, http::header, middleware, web, App, HttpServer};

use crate::{
    configuration::{AppState, State},
    controller::{
        borrow_apr, buyback, deposit_suspension, optimal, supplied_borrowed_series,
        total_value_locked, utilization_level, yield_value, distributed, leased_assets, borrowed, revenue, buyback_total, incentives_pool, total_value_locked_series
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
    let host = app_state.config.server_host.to_string();
    let port = app_state.config.port;

    let server = HttpServer::new(move || {
        let app = app_state.clone();
        let static_dir = app_state.config.static_dir.to_string();

        let cors = Cors::default()
            .allowed_origin_fn(move |origin, _| {
                let allowed = &app.config.allowed_origins;
                if let Ok(origin) = origin.to_str() {
                    return allowed.contains(&origin.to_string());
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
                    .service(yield_value::index)
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
                    .service(total_value_locked_series::index)
            )
            .service(Files::new("/", static_dir).index_file("index.html"))
    })
    .bind((host, port))?
    .disable_signals()
    .run();
    Ok(server)
}
