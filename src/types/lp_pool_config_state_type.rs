use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LP_Pool_Config_State_Type {
    pub borrow_rate: Borrow_Rate,
    pub min_utilization: u128,
}

#[derive(Debug, Deserialize)]
pub struct Borrow_Rate {
    pub addon_optimal_interest_rate: u128,
    pub base_interest_rate: u128,
    pub utilization_optimal: u128,
}
