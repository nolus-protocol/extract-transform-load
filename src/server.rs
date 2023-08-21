use actix_cors::Cors;
use actix_web::{dev::Server, HttpServer, http::header, web, App, middleware};

use crate::{configuration::{AppState, State}, error::Error, controller::test_message};

pub async fn server_task(app_state: &AppState<State>) -> Result<(), Error> {
    let app = app_state.clone();
    tokio::spawn(async move {
        let server = init_server(app)?;
        server.await?;
        Ok(())
    }).await?
}

fn init_server(app_state: AppState<State>) -> Result<Server, Error> {
    let host = app_state.config.server_host.to_string();
    let port = app_state.config.port;

    let server = HttpServer::new(move || {
        let app = app_state.clone();
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
                    .service(test_message::index),
            )
    })
    .bind((host, port))?
    .disable_signals()
    .run();
    Ok(server)
}