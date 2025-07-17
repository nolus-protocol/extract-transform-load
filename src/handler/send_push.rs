use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::send_push_task,
    types::{PushData, PushHeader, Urgency},
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
