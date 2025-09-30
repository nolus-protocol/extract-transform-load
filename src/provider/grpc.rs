use std::{fmt::Debug, str::FromStr, time::Duration};

use anyhow::{anyhow, Context as _, Result};
use cosmos_sdk_proto::cosmwasm::wasm::v1::QueryRawContractStateRequest;
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
    IntoRequest, Status,
};
use tracing::error;

use crate::{
    configuration::Config,
    error::Error,
    types::{
        AdminProtocolExtendType, AdminProtocolType, AmountObject, Balance,
        LPP_Price, LP_Pool_Config_State_Type, LP_Pool_State_Type, LS_Raw_State,
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
    pub limit: usize,
}

impl Grpc {
    pub async fn new(config: Config) -> Result<Grpc, Error> {
        let host = config.grpc_host.to_owned();

        let uri = Uri::from_str(&host).context("Invalid grpc url")?;
        let tls_config = ClientTlsConfig::new().with_native_roots();
        let limit = 10 * 1024 * 1024;

        let endpoint = Endpoint::from(uri.clone())
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(10))
            .tls_config(tls_config)
            .context("Could not parse tls config")?;

        let channel = endpoint.connect_lazy();

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
            limit,
        })
    }

    async fn with_retry<F>(&self, mut f: F) -> Result<Vec<u8>, Error>
    where
        F: AsyncFnMut(
            WasmQueryClient<Channel>,
        ) -> std::result::Result<Vec<u8>, Status>,
    {
        let mut attempts = 5;

        loop {
            let c = self.wasm_query_client.clone();
            let k = f(c).await;

            match k {
                Ok(e) => return Ok(e),
                Err(e) => match e.code() {
                    tonic::Code::Cancelled
                    | tonic::Code::Internal
                    | tonic::Code::Unknown => {
                        if attempts < 0 {
                            return Err(Error::GrpsError(String::from(
                                e.message(),
                            )));
                        }
                        attempts -= 1;
                        sleep(Duration::from_millis(
                            self.config.tasks_interval,
                        ))
                        .await;
                    },
                    _ => {
                        return Err(e.into());
                    },
                },
            }
        }
    }

    pub async fn get_lease_state_new(
        &self,
        contract: String,
    ) -> Result<LS_State_Type, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lease contract new!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract new!";

        let data = self
            .with_retry(async move |mut client| {
                let bytes = b"{\"state\": {}}";
                let data = client
                    .smart_contract_state(QuerySmartContractStateRequest {
                        address: contract.clone(),
                        query_data: bytes.to_vec(),
                    })
                    .await
                    .map(|response| response.into_inner().data);

                data
            })
            .await
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice::<LS_State_Type>(&data)
                    .context(PARCE_MESSAGE_ERROR)
            })?;

        Ok(data)
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

        let mut client = self.tendermint_client.clone();

        client
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

        let mut client = self.tendermint_client.clone();
        let block = client
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

            tx_responses.push(self.get_tx(hash, height).await?);
        }

        Ok((tx_responses, time_stamp))
    }

    pub async fn get_tx(
        &self,
        tx_hash: String,
        height: i64,
    ) -> Result<Option<TxResponse>, Error> {
        let hash = tx_hash.to_string();

        let mut client = self.tx_service_client.clone();
        let tx = client.get_tx(GetTxRequest { hash: tx_hash }).await;

        if let Err(err) = &tx {
            match err.code() {
                tonic::Code::Internal | tonic::Code::Unknown => {
                    error!("tx decode with internal error: {}", err);
                    return Ok(None);
                },
                _ => {},
            }
        }
        let tx = tx
            .context(format!(
                "Query response doesn't contain tx information {}, block {}",
                hash,
                height
            ))
            .and_then(|response| {
                let data = response.into_inner();
                data.tx_response.context(format!(
                "Query response doesn't contain tx information tx_response {}, block {}",
                hash,
                height
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
            resolve_denom: false,
        };

        let mut client = self.bank_query_client.clone();

        let data = client
            .all_balances(data)
            .await
            .map(|response| response.into_inner())
            .context(QUERY_NODE_INFO_ERROR)?;
        Ok(data)
    }

    pub async fn get_balances_by_block(
        &self,
        address: String,
        height: i64,
    ) -> Result<QueryAllBalancesResponse, Error> {
        let QUERY_NODE_INFO_ERROR =
            format!("Failed to query all balances {}!", &height);

        let mut request = QueryAllBalancesRequest {
            address,
            pagination: Some(PageRequest {
                key: vec![],
                offset: 0,
                limit: 10,
                count_total: true,
                reverse: false,
            }),
            resolve_denom: false,
        }
        .into_request();

        let metetadata = request.metadata_mut();
        metetadata.append("x-cosmos-block-height", height.into());

        let mut client = self.bank_query_client.clone();
        let data = client.all_balances(request).await;

        let data = data
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
        let mut client = self.wasm_query_client.clone();
        let data = client
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

        let mut client = self.wasm_query_client.clone();
        let data = client
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

    pub async fn get_base_currency(
        &self,
        contract: String,
    ) -> Result<String, Error> {
        let bytes = b"{\"base_currency\": {}}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle base_currency contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle base_currency contract!";

        let mut client = self.wasm_query_client.clone();
        let data = client
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

    pub async fn get_stable_price(
        &self,
        contract: String,
        ticker: String,
    ) -> Result<AmountObject, Error> {
        let bytes =
            format!(r#"{{"stable_price": {{ "currency": "{}" }} }}"#, ticker)
                .to_owned();
        let bytes = bytes.as_bytes();

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against oracle stable_price contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against oracle stable_price contract!";

        let mut client = self.wasm_query_client.clone();
        let data = client
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

    pub async fn get_admin_config(
        &self,
        contract: String,
    ) -> Result<Vec<String>, Error> {
        let bytes = b"{\"protocols\": {}}";
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query against admin config contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query against admin config contract!";

        let mut client = self.wasm_query_client.clone();
        let data = client
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
        let bytes = b"{\"state\": {}}";

        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query lease contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract get_lease_state!";

        let mut client = self.wasm_query_client.clone();
        let data = client
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

    pub async fn get_lease_state_by_block(
        &self,
        contract: String,
        height: i64,
    ) -> Result<LS_State_Type, Error> {
        const STATE_CHANGE_BLOCK: i64 = 10958318;

        let bytes: &[u8] = if height >= STATE_CHANGE_BLOCK {
            b"{\"state\": {}}"
        } else {
            b"{}"
        };
        let QUERY_CONTRACT_ERROR = format!(
            "Failed to run query lease contract by block {} {}!",
            height, contract
        );

        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query lease contract get_lease_state_by_block!";
        let mut request = QuerySmartContractStateRequest {
            address: contract.to_owned(),
            query_data: bytes.to_vec(),
        }
        .into_request();

        let metetadata = request.metadata_mut();
        metetadata.append("x-cosmos-block-height", height.into());

        let mut client = self.wasm_query_client.clone();
        let data = client.smart_contract_state(request).await;

        let data = data
            .map(|response| response.into_inner().data)
            .context(QUERY_CONTRACT_ERROR);

        let data = data.and_then(|data| {
            let k = serde_json::from_slice(&data).context(PARCE_MESSAGE_ERROR);
            k
        })?;
        Ok(data)
    }

    pub async fn get_lease_raw_state_by_block(
        &self,
        contract: String,
        height: i64,
    ) -> Result<LS_Raw_State, Error> {
        let bytes = "state";

        let QUERY_CONTRACT_ERROR = format!(
            "Failed to run query lease contract by block raw {} {}!",
            height, contract
        );

        let PARCE_MESSAGE_ERROR =
            format!("Failed to parse message query lease contract get_lease_raw_state_by_block  {} {}!",height, contract);
        let mut request = QueryRawContractStateRequest {
            address: contract,
            query_data: bytes.as_bytes().to_vec(),
        }
        .into_request();

        let metetadata = request.metadata_mut();
        metetadata.append("x-cosmos-block-height", height.into());

        let mut client = self.wasm_query_client.clone();
        let data = client.raw_contract_state(request).await;
        let data = data
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

        let mut client = self.wasm_query_client.clone();
        let data = client
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

    pub async fn get_balance_state_new(
        &self,
        contract: String,
        address: String,
    ) -> Result<Balance, Error> {
        const QUERY_CONTRACT_ERROR: &str =
            "Failed to run query balance contract!";
        const PARCE_MESSAGE_ERROR: &str =
            "Failed to parse message query balance contract!";

        let data = self
            .with_retry(async move |mut client| {
                let request =
                    format!(r#"{{"balance":{{"address": "{}" }} }}"#, address);
                let bytes = request.as_bytes();

                let data = client
                    .smart_contract_state(QuerySmartContractStateRequest {
                        address: contract.clone(),
                        query_data: bytes.to_vec(),
                    })
                    .await
                    .map(|response| response.into_inner().data);

                data
            })
            .await
            .context(QUERY_CONTRACT_ERROR)
            .and_then(|data| {
                serde_json::from_slice::<Balance>(&data)
                    .context(PARCE_MESSAGE_ERROR)
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

        let mut client = self.wasm_query_client.clone();
        let data = client
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

        let mut client = self.wasm_query_client.clone();
        let data = client
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

        let mut client = self.wasm_query_client.clone();
        let data = client
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
