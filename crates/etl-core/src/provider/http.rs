use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client,
};
use std::str::FromStr;
use std::time::Duration;

use crate::{
    configuration::Config,
    error::{self, Error},
    types::PushHeader,
};

#[derive(Debug)]
pub struct HTTP {
    pub config: Config,
    pub http: Client,
}

impl HTTP {
    pub fn new(config: Config) -> Result<HTTP, Error> {
        let http = match Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return Err(error::Error::ReqwestError(e));
            },
        };

        Ok(HTTP { config, http })
    }

    pub async fn post_push(
        &self,
        url: String,
        signature: String,
        push_header: PushHeader,
        data: Vec<u8>,
    ) -> Result<u16, Error> {
        let client = Client::new();
        let mut header_map = HeaderMap::new();
        let bearer = format!("WebPush {}", &signature);

        header_map.insert(
            HeaderName::from_str("User-Agent")?,
            HeaderValue::from_str(String::from("nolus-etl").as_str())?,
        );

        header_map.insert(
            HeaderName::from_str("authorization")?,
            HeaderValue::from_str(bearer.as_str())?,
        );
        header_map.insert(
            HeaderName::from_str("content-encoding")?,
            HeaderValue::from_str("aes128gcm")?,
        );
        header_map.insert(
            HeaderName::from_str("ttl")?,
            HeaderValue::from_str(&push_header.ttl.to_string())?,
        );

        header_map.insert(
            HeaderName::from_str("urgency")?,
            HeaderValue::from_str(&push_header.urgency.to_string())?,
        );

        let vapid_pub_b64 = String::from_utf8(
            self.config.vapid_public_key.clone(),
        )
        .map_err(|_| {
            error::Error::InvalidHeader(String::from("invalid VAPID key"))
        })?;
        let crypto_key_value = format!("p256ecdsa={}", vapid_pub_b64.trim());
        header_map.insert(
            HeaderName::from_static("crypto-key"),
            HeaderValue::from_str(&crypto_key_value)?,
        );
        let data = client
            .post(url)
            .headers(header_map)
            .body(data)
            .send()
            .await?;
        let status = data.status().as_u16();

        Ok(status)
    }
}
