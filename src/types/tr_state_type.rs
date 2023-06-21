use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TR_State_Type {
    pub balances: Vec<(String,)>,
}
