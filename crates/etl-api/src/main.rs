use tracing::{error, Level};

use etl_core::{
    configuration::{
        get_configuration, set_configuration, AppState, Config, State,
    },
    error::Error,
    provider::{DatabasePool, Grpc, HTTP},
};

mod controller;
mod csv_response;
mod error;
mod handler;
mod server;

use handler::cache_refresher;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let result = app_main().await;

    if let Err(err) = &result {
        error!("{}", err);
    }

    result
}

async fn app_main() -> Result<(), Error> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_level(true)
        .with_max_level({
            #[cfg(debug_assertions)]
            {
                Level::INFO
            }

            #[cfg(not(debug_assertions))]
            {
                Level::INFO
            }
        })
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    run_server().await
}

async fn run_server() -> Result<(), Error> {
    let (config, database) = match init().await {
        Ok((config, database)) => (config, database),
        Err(e) => return Err(Error::ConfigurationError(e.to_string())),
    };

    let grpc = Grpc::new(config.clone()).await?;
    let http = HTTP::new(config.clone())?;

    let state = State::new(config.clone(), database, grpc, http).await?;
    let app_state = AppState::new(state);

    let (_, _) = tokio::try_join!(
        server::server_task(&app_state),
        cache_refresher::cache_refresh_task(app_state.clone()),
    )?;

    Ok(())
}

async fn init() -> Result<(Config, DatabasePool), Error> {
    set_configuration()?;
    let config = get_configuration()?;

    etl_core::migration::run_migrations(&config.database_url).await?;

    let database = DatabasePool::new(&config).await?;
    Ok((config, database))
}
