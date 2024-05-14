use crate::error::Error;
use std::fs;

pub fn get_path(dir: &str, file: &str) -> Result<String, Error> {
    let data = fs::read_to_string(format!("{}/migration/postgresql/{}", dir, file))?;
    Ok(data)
}
