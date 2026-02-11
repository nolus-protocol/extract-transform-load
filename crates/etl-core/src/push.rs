use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL, Engine};
use chrono::Local;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Url;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Subscription,
    types::{Claims, PushData, PushHeader, Urgency},
};

pub async fn send(
    app_state: AppState<State>,
    address: String,
    push_data: PushData,
) -> Result<(), Error> {
    let items = app_state
        .database
        .subscription
        .get_by_address(address.to_owned())
        .await?;

    let push_headers = PushHeader {
        ttl: 24 * 60 * 60,
        urgency: Urgency::High,
    };

    for subscription in items {
        let app_state = app_state.clone();
        send_push_task(
            app_state,
            subscription,
            push_headers.clone(),
            push_data.clone(),
        );
    }
    Ok(())
}

pub fn send_push_task(
    state: AppState<State>,
    subscription: Subscription,
    push_header: PushHeader,
    push_data: PushData,
) {
    let permits = state.push_permits.clone();
    tokio::spawn(async move {
        // Acquire permit to limit concurrent push tasks
        let _permit = match permits.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                tracing::error!("Push notification semaphore closed");
                return;
            },
        };
        let res = send_push(state, subscription, push_header, push_data).await;
        if let Err(e) = res {
            tracing::error!("Push notification failed: {}", e);
        };
    });
}

pub async fn send_push(
    state: AppState<State>,
    subscription: Subscription,
    push_header: PushHeader,
    push_data: PushData,
) -> Result<(), Error> {
    let url = Url::parse(&subscription.endpoint)?;
    let exp = Local::now().timestamp_millis() / 1000 + push_header.ttl;

    let scheme = url.scheme();
    let host = if let Some(h) = url.host() {
        h.to_string()
    } else {
        return Err(Error::InvalidOption {
            option: String::from("host"),
        });
    };

    let aud = format!("{}://{}", scheme, host);
    let sub = format!("mailto:{}", &state.config.mail_to);

    let key = EncodingKey::from_ec_pem(&state.config.vapid_private_key)?;
    let claims = Claims { aud, sub, exp };
    let token = encode(&Header::new(Algorithm::ES256), &claims, &key)?;

    let p256dh = BASE64_URL.decode(subscription.p256dh)?;
    let auth = BASE64_URL.decode(subscription.auth)?;

    let data = ece::encrypt(&p256dh, &auth, push_data.to_string().as_bytes())?;
    let endpoint = subscription.endpoint.to_string();

    let _status = state
        .http
        .post_push(subscription.endpoint, token, push_header, data)
        .await?;

    if state.config.status_code_to_delete.contains(&_status) {
        state.database.subscription.deactivate(endpoint).await?;
    }

    Ok(())
}
