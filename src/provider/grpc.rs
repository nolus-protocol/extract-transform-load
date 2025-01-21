use std::{str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use cosmrs::proto::{
    cosmos::{
        bank::v1beta1::{
            query_client::QueryClient as BankQueryClient,
            QueryAllBalancesRequest, QueryAllBalancesResponse,
        },
        base::{
            abci::v1beta1::TxResponse,
            query::v1beta1::PageRequest,
            tendermint::v1beta1::{
                service_client::ServiceClient as TendermintServiceClient,
                GetBlockByHeightRequest, GetLatestBlockRequest,
            },
        },
        tx::v1beta1::{
            service_client::ServiceClient as TxServiceClient, GetTxRequest,
        },
    },
    cosmwasm::wasm::v1::{
        query_client::QueryClient as WasmQueryClient,
        QuerySmartContractStateRequest,
    },
    Timestamp,
};
use sha256::digest;
use tokio::time::sleep;
use tonic::{
    codegen::http::Uri,
    transport::{Channel, ClientTlsConfig, Endpoint},
    IntoRequest,
};
use tracing::error;

use crate::{
    configuration::Config,
    error::Error,
    try_join_with_capacity,
    types::{
        AdminProtocolExtendType, AdminProtocolType, AmountObject, Balance,
        LPP_Price, LP_Pool_Config_State_Type, LP_Pool_State_Type,
        LS_State_Type, Prices,
    },
};

#[derive(Debug)]
pub struct Grpc {
    pub config: Config,
    pub endpoint: Endpoint,
    pub tendermint_client: TendermintServiceClient<Channel>,
    pub wasm_query_client: WasmQueryClient<Channel>,
    pub bank_query_client: BankQueryClient<Channel>,
    pub tx_service_client: TxServiceClient<Channel>,
}

impl Grpc {
    pub async fn new(config: Config) -> Result<Grpc, Error> {
        let host = config.grpc_host.to_owned();
        let uri = Uri::from_str(&host).context("Invalid grpc url")?;
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let limit = 10 * 1024 * 1024;

        let endpoint = Endpoint::from(uri.clone())
            .keep_alive_while_idle(true)
            .tls_config(tls_config)
            .context("Could not parse tls config")?;

        let channel = endpoint.connect().await.with_context(|| {
            format!(r#"Failed to parse gRPC URI, "{uri}"!"#)
        })?;

        let tendermint_client =
            TendermintServiceClient::with_origin(channel.clone(), uri.clone())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(limit);
        let wasm_query_client =
            WasmQueryClient::with_origin(channel.clone(), uri.clone())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(limit);
        let bank_query_client =
            BankQueryClient::with_origin(channel.clone(), uri.clone())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(limit);
        let tx_service_client =
            TxServiceClient::with_origin(channel.clone(), uri.clone())
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .max_decoding_message_size(limit);

        Ok(Grpc {
            config,
            endpoint,
            tendermint_client: tendermint_client.clone(),
            wasm_query_client: wasm_query_client.clone(),
            bank_query_client: bank_query_client.clone(),
            tx_service_client: tx_service_client.clone(),
        })
    }

    pub async fn prepare_block(
        &self,
        height: i64,
    ) -> Result<(Vec<Option<TxResponse>>, Timestamp), anyhow::Error> {
        let mut sync = 5;
        loop {
            let blocks = self.get_block(height).await;
            match blocks {
                Ok((data, time_stamp)) => {
                    return Ok((data, time_stamp));
                },
                Err(err) => {
                    let s = tonic::Status::from_error(err.try_into()?);
                    let message = s.message();
                    match s.code() {
                        tonic::Code::NotFound | tonic::Code::InvalidArgument => {}
                        s => {
                            return Err(anyhow!("Error")).with_context(|| {
                                format!(
                                    "cloud not parse transaction block {}, message: {}, code {}",
                                    &height, &message, &s
                                )
                            })
                        }
                    }
                },
            };

            if sync <= 0 {
                break;
            }

            sync -= 1;

            sleep(Duration::from_secs(1)).await;
        }

        Err(anyhow!(
            "transaction not found in block in 5 getters {}",
            &height
        ))
    }

    pub async fn get_latest_block(&self) -> Result<i64> {
        const QUERY_NODE_INFO_ERROR: &str =
            "Failed to query node's latest block!";

        const MISSING_BLOCK_INFO_ERROR: &str =
            "Query response doesn't contain block information!";

        const MISSING_BLOCK_HEADER_INFO_ERROR: &str =
            "Query response doesn't contain block's header information!";

        self.tendermint_client
            .clone()
            .get_latest_block(GetLatestBlockRequest {})
            .await
            .context(QUERY_NODE_INFO_ERROR)
            .and_then(|response| {
                response
                    .into_inner()
                    .sdk_block
                    .context(MISSING_BLOCK_INFO_ERROR)
                    .and_then(|block| {
                        block
                            .header
                            .map(|header| header.height)
                            .context(MISSING_BLOCK_HEADER_INFO_ERROR)
                    })
            })
    }

    pub async fn get_block(
        &self,
        height: i64,
    ) -> Result<(Vec<Option<TxResponse>>, Timestamp), Error> {
        const QUERY_NODE_INFO_ERROR: &str = "Failed to query node's block!";

        const MISSING_BLOCK_INFO_ERROR: &str =
            "Query response doesn't contain block information!";

        const MISSING_BLOCK_DATA_INFO_ERROR: &str =
            "Query response doesn't contain block's data information!";

        let block = self
            .tendermint_client
            .clone()
            .get_block_by_height(GetBlockByHeightRequest { height })
            .await
            .context(QUERY_NODE_INFO_ERROR)
            .and_then(|response| {
                response
                    .into_inner()
                    .sdk_block
                    .context(MISSING_BLOCK_INFO_ERROR)
            })?;

        let timestamp = block
            .header
            .context("Missing header in block")?
            .time
            .context("Missing header time in block")?;

        let tx_responses = try_join_with_capacity(
            block
                .data
                .context(MISSING_BLOCK_DATA_INFO_ERROR)?
                .txs
                .into_iter()
                .map(|tx| {
                    Self::get_tx_internal(
                        self.tx_service_client.clone(),
                        &digest(&tx),
                        height,
                    )
                }),
            self.config.max_tasks,
        )
        .await?;

        Ok((tx_responses, timestamp))
    }

    pub async fn get_tx(
        &self,
        hash: &str,
        height: i64,
    ) -> Result<Option<TxResponse>, Error> {
        Self::get_tx_internal(self.tx_service_client.clone(), hash, height)
            .await
    }

    pub async fn get_balances(
        &self,
        address: String,
    ) -> Result<QueryAllBalancesResponse, Error> {
        const QUERY_NODE_INFO_ERROR: &str = "Failed to query all balances!";

        self.bank_query_client
            .clone()
            .all_balances(QueryAllBalancesRequest {
                address,
                pagination: Some(PageRequest {
                    key: vec![],
                    offset: 0,
                    limit: 1,
                    count_total: true,
                    reverse: false,
                }),
                resolve_denom: false,
            })
            .await
            .map(|response| response.into_inner())
            .context(QUERY_NODE_INFO_ERROR)
            .map_err(From::from)
    }

    pub async fn get_protocol_config(
        wasm_query_client: WasmQueryClient<Channel>,
        contract: String,
        protocol: &str,
    ) -> Result<AdminProtocolExtendType, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against contract!";

        wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: format!(r#"{{"protocol":{protocol:?}}}"#)
                    .into_bytes(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice::<AdminProtocolType>(&data)
                    .context(PARCE_MESSAGE_ERROR)
                    .map(|data| AdminProtocolExtendType {
                        contracts: data.contracts,
                        network: data.network,
                        protocol: protocol.to_owned(),
                    })
            })
            .map_err(From::from)
    }

    pub async fn get_prices(
        &self,
        contract: String,
        protocol: String,
        _height: Option<String>,
    ) -> Result<(Prices, String), Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle contract!";

        const QUERY: &[u8] = br#"{"prices":{}}"#;

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok((data, protocol))
    }

    pub async fn get_base_currency(
        &self,
        contract: String,
    ) -> Result<String, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle base_currency contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle base_currency contract!";

        const QUERY: &[u8] = br#"{"base_currency":{}}"#;

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_stable_price(
        &self,
        contract: String,
        ticker: &str,
    ) -> Result<AmountObject, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle stable_price contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle stable_price contract!";

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: format!(
                    r#"{{"stable_price":{{"currency":{ticker:?}}}}}"#
                )
                .into_bytes(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_admin_config(
        &self,
        contract: String,
    ) -> Result<Vec<String>, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against admin config contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against admin config contract!";

        const QUERY: &[u8] = br#"{"protocols":{}}"#;

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_lease_state(
        &self,
        contract: String,
    ) -> Result<LS_State_Type, Error> {
        const QUERY: &[u8] = br#"{}"#;

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lease contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract!";

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_lease_state_by_block(
        &self,
        contract: String,
        height: i64,
    ) -> Result<LS_State_Type, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lease contract by block!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract!";

        const BYTES: &[u8] = b"{}";

        let mut request = QuerySmartContractStateRequest {
            address: contract,
            query_data: BYTES.to_vec(),
        }
        .into_request();

        request
            .metadata_mut()
            .append("x-cosmos-block-height", height.into());

        self.wasm_query_client
            .clone()
            .smart_contract_state(request)
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_balance_state(
        &self,
        contract: String,
        address: &str,
    ) -> Result<Balance, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query balance contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query balance contract!";

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: format!(
                    r#"{{"balance":{{"address":{:?}}}}}"#,
                    address
                )
                .into_bytes(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_lpp_price(
        &self,
        contract: String,
    ) -> Result<LPP_Price, Error> {
        const QUERY_CONTRACT_ERROR: &str = "Failed to run query lpp contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp contract!";

        const QUERY: &[u8] = br#"{"price":[]}"#;

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_lpp_balance_state(
        &self,
        contract: String,
    ) -> Result<LP_Pool_State_Type, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lpp balance state contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp balance state contract!";

        const BYTES: &[u8] = br#"{"lpp_balance":[]}"#;

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: BYTES.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    pub async fn get_lpp_config_state(
        &self,
        contract: String,
    ) -> Result<LP_Pool_Config_State_Type, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lpp config state contract!";

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp config state contract!";

        const QUERY: &[u8] = br#"{"config":[]}"#;

        self.wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: QUERY.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })
            .map_err(From::from)
    }

    async fn get_tx_internal(
        mut tx_service_client: TxServiceClient<Channel>,
        hash: &str,
        height: i64,
    ) -> Result<Option<TxResponse>, Error> {
        let tx = tx_service_client
            .clone()
            .get_tx(GetTxRequest {
                hash: hash.to_string(),
            })
            .await;

        match tx {
            Ok(tx) => {
                tx.into_inner().tx_response.map(Some).context(format!(
                    "Query response doesn't contain tx information tx_response {}, block {}",
                    hash,
                    height
                ))
                    .map_err(From::from)
            }
            Err(error) => {
                if matches!(error.code(), tonic::Code::Internal | tonic::Code::Unknown) {
                    error!("tx decode with internal error: {}", error)
                }

                Err(anyhow!(error).context(format!(
                    "Query response doesn't contain tx information {}, block {}",
                    hash, height
                )).into())
            }
        }
    }
}
