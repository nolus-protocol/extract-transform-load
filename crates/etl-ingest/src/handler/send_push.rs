use etl_core::{
    configuration::{AppState, State},
    error::Error,
    push,
    types::PushData,
};

pub async fn send(
    app_state: AppState<State>,
    address: String,
    push_data: PushData,
) -> Result<(), Error> {
    push::send(app_state, address, push_data).await
}
