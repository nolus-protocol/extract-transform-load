/// Parameters for Total Value Locked calculation
#[derive(Debug, Clone)]
pub struct TvlPoolParams {
    pub osmosis_usdc: String,
    pub neutron_axelar: String,
    pub osmosis_usdc_noble: String,
    pub neutron_usdc_noble: String,
    pub osmosis_st_atom: String,
    pub osmosis_all_btc: String,
    pub osmosis_all_sol: String,
    pub osmosis_akt: String,
}
