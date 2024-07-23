use std::str::FromStr;

use crate::configuration::Config;
use crate::error::Error;

use anyhow::Context;
use cosmos_sdk_proto::cosmos::base::tendermint::v1beta1::GetLatestBlockRequest;
use cosmrs::proto::cosmos::base::tendermint::v1beta1::service_client::ServiceClient as TendermintServiceClient;
use tonic::{
    transport::{Endpoint, Uri},
    Response as TonicResponse,
};

#[derive(Debug)]
pub struct Grpc {
    pub config: Config,
}

impl Grpc {
    pub async fn new(config: Config) -> Result<Grpc, Error> {
        let host = config.grpc_host.to_owned();
        let uri = match Uri::from_str(&host) {
            Ok(uri) => uri,
            Err(e) => {
                return Err(Error::ServerError(format!("Invalid grpc uri: {}", e)));
            }
        };

        let endpoint = Endpoint::from(uri.clone())
            .origin(uri.clone())
            .keep_alive_while_idle(true);

        let channel = match endpoint
            .connect()
            .await
            .with_context(|| format!(r#"Failed to parse gRPC URI, "{uri}"!"#))
        {
            Ok(c) => c,
            Err(e) => {
                return Err(Error::ServerError(format!("Channel error: {}", e)));
            }
        };

        let mut client = TendermintServiceClient::with_origin(channel, uri);

        let data = client
            .get_latest_block(GetLatestBlockRequest {})
            .await
            .map(TonicResponse::into_inner)
            .unwrap();
        dbg!(data.block.unwrap().header.unwrap().height);
        Ok(Grpc { config })
    }
}
