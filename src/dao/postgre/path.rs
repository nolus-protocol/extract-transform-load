use std::path::PathBuf;

pub fn get_path(dir: &str, file: &str) -> PathBuf {
    let mut buf = PathBuf::new();

    for chunk in [dir, "migration", "postgresql", file] {
        buf.push(chunk);
    }

    buf
}
