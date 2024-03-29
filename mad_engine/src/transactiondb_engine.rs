//! Rocksdb TransactionDB implementation
//!
//! This module is like a TransactionDB wrapper
//! in order to realize transaction write

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::ffi::c_void;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_spdk::blobfs::SpdkFilesystem;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, MergeOperands, SingleThreaded, TransactionDB,
    TransactionDBOptions, TransactionOptions, WriteOptions,
};

const JNL_CF_NAME: &str = "journal_cf";

#[allow(unused)]
pub struct RocksdbEngine {
    pub db: TransactionDB,
    jnl_cf: &'static ColumnFamily,
    write_opts: WriteOptions,
    txn_opts: TransactionOptions,
    pub db_opts: rocksdb::Options,
}

impl Drop for RocksdbEngine {
    fn drop(&mut self) {}
}

unsafe impl Send for RocksdbEngine {}
unsafe impl Sync for RocksdbEngine {}

fn rocksdb_txn_options() -> TransactionOptions {
    let mut opts = TransactionOptions::default();
    opts.set_lock_timeout(0);
    opts
}

fn rocksdb_options(
    fs: Arc<Mutex<SpdkFilesystem>>,
    fs_core: u32,
    data_path: impl AsRef<Path>,
    config: &str,
    bdev: &str,
    cache_size_in_mb: u64,
) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    // let fs = fs.lock().unwrap();
    let env = {
        rocksdb::Env::rocksdb_use_spdk_env(
            fs.lock().unwrap().ptr as *mut c_void,
            fs_core,
            data_path.as_ref().to_str().unwrap(),
            config,
            bdev,
            cache_size_in_mb,
        )
        .expect("fail to initilize spdk env")
    };
    opts.create_if_missing(true);
    opts.set_env(&env);
    opts.increase_parallelism(4);
    opts.create_missing_column_families(true);
    // drop(fs);
    opts
}

fn rocksdb_txn_db_options() -> TransactionDBOptions {
    let mut opts = TransactionDBOptions::default();
    opts.set_default_lock_timeout(0);
    opts.set_txn_lock_timeout(0);
    opts
}

fn default_cf_options(
    fs: Arc<Mutex<SpdkFilesystem>>,
    fs_core: u32,
    data_path: impl AsRef<Path>,
    config: &str,
    bdev: &str,
    cache_size_in_mb: u64,
) -> rocksdb::Options {
    let mut opts = rocksdb_options(fs, fs_core, data_path, config, bdev, cache_size_in_mb);
    opts.set_merge_operator("merge", full_merge, partial_merge);
    opts
}

impl RocksdbEngine {
    /// Create a RocksdbEngine based on given blobfs
    pub fn new(
        fs: Arc<Mutex<SpdkFilesystem>>,
        fs_core: u32,
        data_path: impl AsRef<Path>,
        config: &str,
        bdev: &str,
        cache_size_in_mb: u64,
    ) -> Result<Self> {
        let write_opts = WriteOptions::default();
        let opts = rocksdb_options(
            fs.clone(),
            fs_core,
            data_path.as_ref().to_str().unwrap(),
            config,
            bdev,
            cache_size_in_mb,
        );
        let cf_opts = default_cf_options(
            fs,
            fs_core,
            data_path.as_ref().to_str().unwrap(),
            config,
            bdev,
            cache_size_in_mb,
        );
        let db = TransactionDB::<SingleThreaded>::open_cf_descriptors(
            &opts,
            &rocksdb_txn_db_options(),
            data_path,
            vec![
                ColumnFamilyDescriptor::new(rocksdb::DEFAULT_COLUMN_FAMILY_NAME, cf_opts),
                ColumnFamilyDescriptor::new(JNL_CF_NAME, opts.clone()),
            ],
        )
        .unwrap();
        let jnl_cf = unsafe { std::mem::transmute(db.cf_handle(JNL_CF_NAME).unwrap()) };
        let rocksdb_engine = RocksdbEngine {
            db,
            jnl_cf,
            write_opts,
            txn_opts: rocksdb_txn_options(),
            db_opts: opts,
        };
        Ok(rocksdb_engine)
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        let ret = self.db.get(key)?;
        Ok(ret)
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.db.delete(key)?;
        Ok(())
    }
}

fn full_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut new_val = match existing_val {
        Some(v) => v.into(),
        None => vec![],
    };
    for op in operands {
        match bincode::deserialize(op).unwrap() {
            MergeOp::UpdateU64NoLessThan { offset, value } => {
                if new_val.len() >= offset + 8 {
                    let v = unsafe { &mut *(new_val.as_mut_ptr().add(offset) as *mut u64) };
                    if *v < value {
                        *v = value;
                    }
                }
            }
            MergeOp::SetU64 { offset, value } => {
                if new_val.len() >= offset + 8 {
                    let v = unsafe { &mut *(new_val.as_mut_ptr().add(offset) as *mut u64) };
                    *v = value;
                }
            }
            MergeOp::PutIfAbsent(value) => {
                if new_val.is_empty() {
                    new_val.extend_from_slice(value);
                }
            }
        }
    }
    match new_val.is_empty() {
        true => None,
        false => Some(new_val),
    }
}

fn partial_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    _operands: &MergeOperands,
) -> Option<Vec<u8>> {
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeOp<'a> {
    UpdateU64NoLessThan {
        offset: usize,
        value: u64,
    },
    SetU64 {
        offset: usize,
        value: u64,
    },
    #[serde(with = "serde_bytes")]
    PutIfAbsent(&'a [u8]),
}
