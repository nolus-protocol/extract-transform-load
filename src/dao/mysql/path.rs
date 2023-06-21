use std::fs;
use crate::error::Error;

pub fn get_path(dir: &str, file: &str) -> Result<String, Error>{
    let data = fs::read_to_string(format!("{}/migration/mysql/{}", dir, file))?;
    Ok(data)
}