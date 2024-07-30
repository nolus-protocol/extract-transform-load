use crate::configuration::Config;
use crate::dao::{PoolOption, PoolType};
use crate::error::Error;
use crate::model::{
    Action_History, Block, LP_Deposit, LP_Lender_State, LP_Pool, LP_Pool_State,
    LP_Withdraw, LS_Closing, LS_Liquidation, LS_Opening, LS_Repayment,
    LS_State, MP_Asset, MP_Asset_Mapping, MP_Asset_State, MP_Yield, PL_State,
    TR_Profit, TR_Rewards_Distribution, TR_State,
};
use crate::model::{LS_Close_Position, Table};

#[derive(Debug)]
pub struct DatabasePool {
    pub block: Table<Block>,
    pub ls_opening: Table<LS_Opening>,
    pub ls_closing: Table<LS_Closing>,
    pub ls_repayment: Table<LS_Repayment>,
    pub ls_liquidation: Table<LS_Liquidation>,
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
    pub mp_asset_state: Table<MP_Asset_State>,
    pub mp_asset_mapping: Table<MP_Asset_Mapping>,
    pub mp_yield: Table<MP_Yield>,
    pub pl_state: Table<PL_State>,
    pub action_history: Table<Action_History>,
    pub ls_close_position: Table<LS_Close_Position>,
    pub pool: PoolType,
}

impl<'c> DatabasePool {
    pub async fn new(config: &Config) -> Result<DatabasePool, Error> {
        let pool = PoolOption::new()
            .after_connect(|_conn, _meta| Box::pin(async move { Ok(()) }))
            .connect(config.database_url.as_str())
            .await?;

        let lp_pool = Table::new(pool.clone());
        let mp_asset_mapping = Table::new(pool.clone());

        Ok(DatabasePool {
            pool: pool.clone(),
            lp_pool,
            block: Table::new(pool.clone()),
            ls_opening: Table::new(pool.clone()),
            ls_closing: Table::new(pool.clone()),
            ls_repayment: Table::new(pool.clone()),
            ls_liquidation: Table::new(pool.clone()),
            ls_state: Table::new(pool.clone()),
            lp_deposit: Table::new(pool.clone()),
            lp_withdraw: Table::new(pool.clone()),
            lp_lender_state: Table::new(pool.clone()),
            lp_pool_state: Table::new(pool.clone()),
            tr_profit: Table::new(pool.clone()),
            tr_rewards_distribution: Table::new(pool.clone()),
            tr_state: Table::new(pool.clone()),
            mp_asset: Table::new(pool.clone()),
            mp_asset_state: Table::new(pool.clone()),
            mp_asset_mapping,
            mp_yield: Table::new(pool.clone()),
            pl_state: Table::new(pool.clone()),
            ls_close_position: Table::new(pool.clone()),
            action_history: Table::new(pool),
        })
    }

    pub fn get_pool(&self) -> &PoolType {
        &self.pool
    }
}
