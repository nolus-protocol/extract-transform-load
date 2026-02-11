use chrono::{DateTime, Utc};

use etl_core::error::Error;

pub use self::aggregation_task::aggregation_task;

/// Parses a nanosecond timestamp string into a DateTime<Utc>.
/// Blockchain events use nanosecond timestamps, this converts them to DateTime.
pub fn parse_event_timestamp(nanos_str: &str) -> Result<DateTime<Utc>, Error> {
    let nanos: i64 = nanos_str.parse()?;
    let secs = nanos / 1_000_000_000;
    DateTime::from_timestamp(secs, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!("timestamp: {}", secs))
    })
}

mod aggregation_task;
pub mod lp_lender_state;
pub mod lp_pool_state;
pub mod ls_loan_closing;
pub mod ls_state;
pub mod mp_assets;
pub mod pl_state;
pub mod send_push;
pub mod tr_state;
pub mod wasm_lp_deposit;
pub mod wasm_lp_withdraw;
pub mod wasm_ls_auto_close_position;
pub mod wasm_ls_close;
pub mod wasm_ls_close_position;
pub mod wasm_ls_liquidation;
pub mod wasm_ls_liquidation_warning;
pub mod wasm_ls_open;
pub mod wasm_ls_repay;
pub mod wasm_ls_slippage_anomaly;
pub mod wasm_reserve_cover_loss;
pub mod wasm_tr_profit;
pub mod wasm_tr_rewards;
