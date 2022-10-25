//! RocksdbEngine implementation

use crate::error::Result;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use std::ffi::c_void;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_spdk::blobfs::SpdkFilesystem;

pub struct DbEngine{
    pub db: DB,
    pub db_opts: Options,

}

unsafe impl Send for DbEngine {}
unsafe impl Sync for DbEngine {}

fn rocksdb_options(
    fs: Arc<Mutex<SpdkFilesystem>>,
    fs_core: u32,
    data_path: impl AsRef<Path>,
    config: &str,
    bdev: &str,
    cache_size_in_mb: u64,
) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
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
    opts
}

impl DbEngine {
    /// Create a RocksdbEngine based on given blobfs
    pub fn new(
        fs: Arc<Mutex<SpdkFilesystem>>,
        fs_core: u32,
        data_path: impl AsRef<Path>,
        config: &str,
        bdev: &str,
        cache_size_in_mb: u64,
    ) -> Result<Self> {
        let mut opts = rocksdb_options(
            fs.clone(),
            fs_core,
            data_path.as_ref().to_str().clone().unwrap(),
            config,
            bdev,
            cache_size_in_mb,
        );
        let db = DB::open(&opts, data_path)?;
        let db_engine = DbEngine{
            db,
            db_opts: opts,
        };
        Ok(db_engine)
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

