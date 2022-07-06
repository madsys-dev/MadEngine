use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("cannot init bs")]
    BsInitError,
    #[error("chunk metadata not found")]
    MetaNotExist,
}

pub type Result<T> = std::result::Result<T, EngineError>;
