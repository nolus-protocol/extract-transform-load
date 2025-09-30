use serde::Deserialize;

use super::AmountTicker;

#[derive(Debug, Deserialize)]
pub struct LS_State_Type {
    pub opened: Option<Status_Opened>,
    pub paid: Option<Status_Paid>,
    pub closing: Option<Status_Paid>,
}

#[derive(Debug, Deserialize, Default)]
pub struct Status_Opened {
    pub amount: AmountTicker,
    pub loan_interest_rate: u128,
    pub margin_interest_rate: u128,
    pub principal_due: AmountTicker,
    //old
    pub previous_margin_due: Option<AmountTicker>,
    pub previous_interest_due: Option<AmountTicker>,
    pub current_margin_due: Option<AmountTicker>,
    pub current_interest_due: Option<AmountTicker>,
    //new
    pub overdue_margin: Option<AmountTicker>,
    pub overdue_interest: Option<AmountTicker>,
    pub due_margin: Option<AmountTicker>,
    pub due_interest: Option<AmountTicker>,
}

#[derive(Debug, Deserialize)]
pub struct Status_Paid {
    pub amount: AmountTicker,
}

#[derive(Debug, Deserialize)]
pub struct LS_Raw_State {
    pub FullClose: Option<TransferInInit>,
    pub PartialClose: Option<TransferInInit>,
    pub OpenedActive: Option<Lease>,
    pub ClosingTransferIn: Option<TransferInInit>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct TransferInInit {
    pub TransferInInit: Option<AmountIn>,
    pub TransferInFinish: Option<AmountIn>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct AmountIn {
    pub amount_in: AmountTicker,
}

#[derive(Debug, Deserialize)]
pub struct Lease {
    pub lease: LeaseData,
}

#[derive(Debug, Deserialize)]
pub struct LeaseData {
    pub lease: PositionData,
}

#[derive(Debug, Deserialize)]
pub struct PositionData {
    pub position: Option<Position>,
    pub amount: Option<AmountTicker>,
}

#[derive(Debug, Deserialize)]
pub struct Position {
    pub amount: AmountTicker,
}
