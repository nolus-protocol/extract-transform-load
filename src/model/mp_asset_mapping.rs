use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct MP_Asset_Mapping {
    pub MP_asset_symbol: String,
    pub MP_asset_symbol_coingecko: String,
}
