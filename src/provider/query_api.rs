use crate::{
    configuration::Config,
    error::{self, Error},
    types::{Balance, LPP_Price, LP_Pool_State_Type, LS_State_Type, QueryBody},
};
use base64::engine::general_purpose;
use base64::Engine;
use cosmos_sdk_proto::{
    cosmos::{
        bank::v1beta1::{QueryAllBalancesRequest, QueryAllBalancesResponse},
        base::query::v1beta1::PageRequest,
    },
    cosmwasm::wasm::v1::{QuerySmartContractStateRequest, QuerySmartContractStateResponse},
    traits::Message,
};
use reqwest::Client;
use std::fmt::Write;
use std::time::Duration;

#[derive(Debug)]
pub struct QueryApi {
    config: Config,
    pub http: Client,
}

impl QueryApi {
    pub fn new(config: Config) -> Result<QueryApi, Error> {
        let http = match Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return Err(error::Error::REQWEST(e));
            }
        };

        Ok(QueryApi { config, http })
    }

    pub async fn lease_state(&self, contract: String) -> Result<Option<LS_State_Type>, Error> {
        let bytes = b"{\"status_query\":[]}";
        let res = self.query_state(bytes, contract).await?;
        if let Some(item) = res {
            let data = serde_json::from_str(&item)?;
            return Ok(Some(data));
        }

        Ok(None)
    }

    pub async fn lpp_balance_state(
        &self,
        contract: String,
    ) -> Result<Option<LP_Pool_State_Type>, Error> {
        let bytes = b"{\"lpp_balance\": []}";
        let res = self.query_state(bytes, contract).await?;
        if let Some(item) = res {
            let data = serde_json::from_str(&item)?;
            return Ok(Some(data));
        }

        Ok(None)
    }

    pub async fn lpp_price_state(&self, contract: String) -> Result<Option<LPP_Price>, Error> {
        let bytes = b"{\"price\": []}";
        let res = self.query_state(bytes, contract).await?;

        if let Some(item) = res {
            let data = serde_json::from_str(&item)?;
            return Ok(Some(data));
        }

        Ok(None)
    }

    pub async fn balanace_state(
        &self,
        contract: String,
        address: String,
    ) -> Result<Option<Balance>, Error> {
        let request = format!(r#"{{"balance":{{"address": "{}" }} }}"#, address);
        let bytes = request.as_bytes();
        let res = self.query_state(bytes, contract).await?;
        if let Some(item) = res {
            let data = serde_json::from_str(&item)?;
            return Ok(Some(data));
        }

        Ok(None)
    }

    async fn query_state(&self, bytes: &[u8], contract: String) -> Result<Option<String>, Error> {
        let data = self.state_from_proto(bytes, contract)?;

        let res = self
            .http
            .post(self.config.get_abci_query_url())
            .body(format!(
                r#"{{
                    "method": "abci_query",
                    "jsonrpc": "2.0",
                    "params": [
                        "/cosmwasm.wasm.v1.Query/SmartContractState", 
                        "{}",
                        "0",
                        true
                    ],
                    "id": -1
                }}
                "#,
                data
            ))
            .send()
            .await?
            .json::<QueryBody>()
            .await?;

        let value = res.result.response.value;

        if let Some(v) = value {
            return Ok(Some(self.decode_state(&v)?));
        }

        Ok(None)
    }

    pub async fn query_balance(
        &self,
        address: String,
    ) -> Result<Option<QueryAllBalancesResponse>, Error> {
        let data = self.balances_from_proto(address)?;

        let res = self
            .http
            .post(self.config.get_abci_query_url())
            .body(format!(
                r#"{{
                    "method": "abci_query",
                    "jsonrpc": "2.0",
                    "params": [
                        "/cosmos.bank.v1beta1.Query/AllBalances", 
                        "{}",
                        "0",
                        true
                    ],
                    "id": -1
                }}
                "#,
                data
            ))
            .send()
            .await?
            .json::<QueryBody>()
            .await?;

        let value = res.result.response.value;

        if let Some(v) = value {
            return Ok(Some(self.decode_balances(&v)?));
        }

        Ok(None)
    }

    fn decode_state(&self, state: &str) -> Result<String, Error> {
        let data = general_purpose::STANDARD.decode(state)?;
        let response = QuerySmartContractStateResponse::decode(data.as_ref())?;
        let c = String::from_utf8_lossy(&response.data);
        Ok(c.to_string())
    }

    fn state_from_proto(&self, data: &[u8], contract_address: String) -> Result<String, Error> {
        let k = QuerySmartContractStateRequest {
            address: contract_address,
            query_data: data.to_vec(),
        };
        let c = k.encode_to_vec();
        let s = self.encode_bytes(&c)?;

        Ok(s)
    }

    fn decode_balances(&self, state: &str) -> Result<QueryAllBalancesResponse, Error> {
        let data = general_purpose::STANDARD.decode(state)?;
        let response = QueryAllBalancesResponse::decode(data.as_ref())?;
        Ok(response)
    }

    fn balances_from_proto(&self, address: String) -> Result<String, Error> {
        let k = QueryAllBalancesRequest {
            address,
            pagination: Some(PageRequest {
                key: vec![],
                offset: 0,
                limit: 1,
                count_total: true,
                reverse: false,
            }),
        };
        let c = k.encode_to_vec();
        let s = self.encode_bytes(&c)?;

        Ok(s)
    }

    fn encode_bytes(&self, bytes: &Vec<u8>) -> Result<String, Error> {
        let mut s = String::new();

        for byte in bytes {
            write!(s, "{:02X}", byte)?;
        }

        Ok(s)
    }
}
