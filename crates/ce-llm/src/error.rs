use thiserror::Error;

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("api: {0}")]
    Api(String),
    #[error("parse: {0}")]
    Parse(String),
    #[error("config: {0}")]
    Config(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}
