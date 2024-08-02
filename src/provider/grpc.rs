use std::str::FromStr;
use std::time::Duration;

use crate::configuration::Config;
use crate::error::Error;
use crate::types::{
    AdminProtocolExtendType, AdminProtocolType, Balance, LPP_Price,
    LP_Pool_Config_State_Type, LP_Pool_State_Type, LS_State_Type, Prices,
};

use anyhow::{anyhow, Context, Result};
use cosmos_sdk_proto::cosmos::bank::v1beta1::{
    query_client::QueryClient as BankQueryClient, QueryAllBalancesRequest,
    QueryAllBalancesResponse,
};
use cosmos_sdk_proto::cosmos::base::abci::v1beta1::TxResponse;
use cosmos_sdk_proto::cosmos::tx::v1beta1::GetTxRequest;
use cosmos_sdk_proto::Timestamp;
use cosmos_sdk_proto::{
    cosmos::base::{
        query::v1beta1::PageRequest,
        tendermint::v1beta1::GetBlockByHeightRequest,
    },
    cosmwasm::wasm::v1::QuerySmartContractStateRequest,
};

use cosmrs::{
    proto::cosmos::base::tendermint::v1beta1::{
        service_client::ServiceClient as TendermintServiceClient,
        GetLatestBlockRequest,
    },
    proto::cosmos::tx::v1beta1::service_client::ServiceClient as TxServiceClient,
    proto::cosmwasm::wasm::v1::query_client::QueryClient as WasmQueryClient,
};
use sha256::digest;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::error;

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

        let endpoint = Endpoint::from(uri.clone())
            .origin(uri.clone())
            .concurrency_limit(1024)
            .keep_alive_while_idle(true);

        let channel = endpoint.connect().await.with_context(|| {
            format!(r#"Failed to parse gRPC URI, "{uri}"!"#)
        })?;

        let tendermint_client =
            TendermintServiceClient::with_origin(channel.clone(), uri.clone());
        let wasm_query_client =
            WasmQueryClient::with_origin(channel.clone(), uri.clone());
        let bank_query_client =
            BankQueryClient::with_origin(channel.clone(), uri.clone());
        let tx_service_client =
            TxServiceClient::with_origin(channel.clone(), uri.clone());

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
                        tonic::Code::NotFound | tonic::Code::InvalidArgument => {},
                        s => {
                            return Err(anyhow!("Error")).with_context(|| {
                                format!(
                                "cloud not parse transaction block {}, message: {}, code {}",
                                &height, &message, &s
                            )
                            })
                        },
                    }
                },
            };

            if sync <= 0 {
                break;
            }

            sync -= 1;
            sleep(Duration::from_secs(1)).await;
        }

        return Err(anyhow!("Error")).with_context(|| {
            format!("transaction not found in block in 5 getters {}", &height)
        });
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

        let time_stamp = block
            .header
            .context("Missing header in block")?
            .time
            .context("Missing header time in block")?;

        let txs = block.data.context(MISSING_BLOCK_DATA_INFO_ERROR)?.txs;

        let mut tx_responses = vec![];

        for tx in txs {
            let hash = digest(&tx);

            tx_responses.push(self.get_tx(hash).await?);
        }

        Ok((tx_responses, time_stamp))
    }

    pub async fn get_tx(
        &self,
        tx_hash: String,
    ) -> Result<Option<TxResponse>, Error> {
        let hash = tx_hash.to_string();

        let tx = self
            .tx_service_client
            .clone()
            .get_tx(GetTxRequest { hash: tx_hash })
            .await;

        if let Err(err) = &tx {
            match err.code() {
                tonic::Code::Internal => {
                    error!("tx decode with internal error: {}", err);
                    return Ok(None);
                },
                _ => {},
            }
        }
        let tx = tx
            .context(format!(
                "Query response doesn't contain tx information {}",
                hash
            ))
            .and_then(|response| {
                let data = response.into_inner();
                data.tx_response.context(format!(
                "Query response doesn't contain tx information tx_response {}",
                hash
            ))
            })?;

        Ok(Some(tx))
    }

    pub async fn get_balances(
        &self,
        address: String,
    ) -> Result<QueryAllBalancesResponse, Error> {
        const QUERY_NODE_INFO_ERROR: &str = "Failed to query all balances!";

        let data = QueryAllBalancesRequest {
            address,
            pagination: Some(PageRequest {
                key: vec![],
                offset: 0,
                limit: 1,
                count_total: true,
                reverse: false,
            }),
        };

        let data = self
            .bank_query_client
            .clone()
            .all_balances(data)
            .await
            .map(|response| response.into_inner())
            .context(QUERY_NODE_INFO_ERROR)?;

        Ok(data)
    }

    pub async fn get_protocol_config(
        &self,
        contract: String,
        protocol: String,
    ) -> Result<AdminProtocolExtendType, Error> {
        let bytes = format!(r#"{{"protocol": "{}"}}"#, protocol).to_owned();
        let bytes = bytes.as_bytes();

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
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
            })?;
        Ok(data)
    }

    pub async fn get_prices(
        &self,
        contract: String,
        protocol: String,
        _height: Option<String>,
    ) -> Result<(Prices, String), Error> {
        let bytes = b"{\"prices\": {}}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;
        Ok((data, protocol))
    }

    pub async fn get_admin_config(
        &self,
        contract: String,
    ) -> Result<Vec<String>, Error> {
        let bytes = b"{\"protocols\": {}}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against admin config contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against admin config contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }

    pub async fn get_lease_state(
        &self,
        contract: String,
    ) -> Result<LS_State_Type, Error> {
        let bytes = b"{}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lease contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }

    pub async fn get_balance_state(
        &self,
        contract: String,
        address: String,
    ) -> Result<Balance, Error> {
        let request =
            format!(r#"{{"balance":{{"address": "{}" }} }}"#, address);
        let bytes = request.as_bytes();

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query balance contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query balance contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }

    pub async fn get_lpp_price(
        &self,
        contract: String,
    ) -> Result<LPP_Price, Error> {
        let bytes = b"{\"price\": []}";

        const QUERY_CONTRACT_ERROR: &str = "Failed to run query lpp contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }

    pub async fn get_lpp_balance_state(
        &self,
        contract: String,
    ) -> Result<LP_Pool_State_Type, Error> {
        let bytes = b"{\"lpp_balance\": []}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lpp balance state contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp balance state contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }

    pub async fn get_lpp_config_state(
        &self,
        contract: String,
    ) -> Result<LP_Pool_Config_State_Type, Error> {
        let bytes = b"{\"config\": []}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lpp config state contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lpp config state contract!";

        let data = self
            .wasm_query_client
            .clone()
            .smart_contract_state(QuerySmartContractStateRequest {
                address: contract,
                query_data: bytes.to_vec(),
            })
            .await
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
    }
}
