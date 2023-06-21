use crate::{model::MP_Asset_Mapping, model::Table};

pub async fn get_mappings(model: &Table<MP_Asset_Mapping>) -> (Vec<MP_Asset_Mapping>, Vec<String>) {
    let data = model.get_all().await.unwrap_or(vec![]);
    let items = data
        .iter()
        .map(|item| item.MP_asset_symbol_coingecko.to_owned())
        .collect();
    (data, items)
}
