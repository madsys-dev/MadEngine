use async_spdk::SpdkError;
// use serde_json::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("cannot init bs")]
    BsInitError,
    #[error("chunk metadata not found")]
    MetaNotExist,
    #[error("chunksum mismatch")]
    CheckSumErr,
    #[error("read out of range")]
    ReadOutRange,
    #[error("hole is not allowed")]
    HoleNotAllowed,
    #[error("restore fail")]
    RestoreFail,
    #[error("fail to get global metadata")]
    GlobalGetFail,
    #[error("RocksDB Error: {0}")]
    RocksDBError(#[from] rocksdb::Error),
    #[error("serde_json Error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("spdk Error: {0}")]
    SPDKError(#[from] SpdkError),
}

pub type Result<T> = std::result::Result<T, EngineError>;
