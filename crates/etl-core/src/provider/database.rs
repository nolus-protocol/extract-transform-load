use std::time::Duration;

use crate::{
    configuration::Config,
    dao::{PoolOption, PoolType},
    error::Error,
    model::{
        Action_History, Block, CurrencyProtocol, CurrencyRegistry, LP_Deposit,
        LP_Lender_State, LP_Pool, LP_Pool_State, LP_Withdraw,
        LS_Auto_Close_Position, LS_Close_Position, LS_Closing, LS_Liquidation,
        LS_Liquidation_Warning, LS_Loan_Closing, LS_Loan_Collect, LS_Opening,
        LS_Repayment, LS_Slippage_Anomaly, LS_State, MP_Asset, MP_Yield,
        PL_State, Pool_Config, ProtocolRegistry, Raw_Message,
        Reserve_Cover_Loss, Subscription, TR_Profit, TR_Rewards_Distribution,
        TR_State, Table,
    },
};

#[derive(Debug)]
pub struct DatabasePool {
    pub block: Table<Block>,
    pub ls_opening: Table<LS_Opening>,
    pub ls_closing: Table<LS_Closing>,
    pub ls_repayment: Table<LS_Repayment>,
    pub ls_liquidation: Table<LS_Liquidation>,
    pub ls_liquidation_warning: Table<LS_Liquidation_Warning>,
    pub ls_auto_close_position: Table<LS_Auto_Close_Position>,
    pub ls_state: Table<LS_State>,
    pub lp_deposit: Table<LP_Deposit>,
    pub lp_withdraw: Table<LP_Withdraw>,
    pub lp_lender_state: Table<LP_Lender_State>,
    pub lp_pool: Table<LP_Pool>,
    pub lp_pool_state: Table<LP_Pool_State>,
    pub tr_profit: Table<TR_Profit>,
    pub tr_rewards_distribution: Table<TR_Rewards_Distribution>,
    pub tr_state: Table<TR_State>,
    pub mp_asset: Table<MP_Asset>,
    pub mp_yield: Table<MP_Yield>,
    pub pl_state: Table<PL_State>,
    pub action_history: Table<Action_History>,
    pub ls_close_position: Table<LS_Close_Position>,
    pub reserve_cover_loss: Table<Reserve_Cover_Loss>,
    pub raw_message: Table<Raw_Message>,
    pub ls_loan_closing: Table<LS_Loan_Closing>,
    pub ls_slippage_anomaly: Table<LS_Slippage_Anomaly>,
    pub subscription: Table<Subscription>,
    pub ls_loan_collect: Table<LS_Loan_Collect>,
    pub pool_config: Table<Pool_Config>,
    pub currency_registry: Table<CurrencyRegistry>,
    pub currency_protocol: Table<CurrencyProtocol>,
    pub protocol_registry: Table<ProtocolRegistry>,
    pub pool: PoolType,
}

impl DatabasePool {
    pub async fn new(config: &Config) -> Result<DatabasePool, Error> {
        let statement_timeout_ms = config.db_statement_timeout * 1000;

        let pool = PoolOption::new()
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    // Set statement timeout to prevent runaway queries
                    sqlx::query(&format!(
                        "SET statement_timeout = '{}'",
                        statement_timeout_ms
                    ))
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .max_connections(config.db_max_connections)
            .min_connections(config.db_min_connections)
            .acquire_timeout(Duration::from_secs(config.db_acquire_timeout))
            .idle_timeout(Duration::from_secs(config.db_idle_timeout))
            .connect(config.database_url.as_str())
            .await?;

        let lp_pool = Table::new(pool.clone());

        Ok(DatabasePool {
            pool: pool.clone(),
            lp_pool,
            block: Table::new(pool.clone()),
            ls_opening: Table::new(pool.clone()),
            ls_closing: Table::new(pool.clone()),
            ls_repayment: Table::new(pool.clone()),
            ls_liquidation: Table::new(pool.clone()),
            ls_liquidation_warning: Table::new(pool.clone()),
            ls_auto_close_position: Table::new(pool.clone()),
            ls_state: Table::new(pool.clone()),
            lp_deposit: Table::new(pool.clone()),
            lp_withdraw: Table::new(pool.clone()),
            lp_lender_state: Table::new(pool.clone()),
            lp_pool_state: Table::new(pool.clone()),
            tr_profit: Table::new(pool.clone()),
            tr_rewards_distribution: Table::new(pool.clone()),
            tr_state: Table::new(pool.clone()),
            mp_asset: Table::new(pool.clone()),
            mp_yield: Table::new(pool.clone()),
            pl_state: Table::new(pool.clone()),
            ls_close_position: Table::new(pool.clone()),
            action_history: Table::new(pool.clone()),
            reserve_cover_loss: Table::new(pool.clone()),
            ls_loan_closing: Table::new(pool.clone()),
            ls_slippage_anomaly: Table::new(pool.clone()),
            subscription: Table::new(pool.clone()),
            ls_loan_collect: Table::new(pool.clone()),
            pool_config: Table::new(pool.clone()),
            currency_registry: Table::new(pool.clone()),
            currency_protocol: Table::new(pool.clone()),
            protocol_registry: Table::new(pool.clone()),
            raw_message: Table::new(pool),
        })
    }

    pub fn get_pool(&self) -> &PoolType {
        &self.pool
    }
}
