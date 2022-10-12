//! Transaction DB wrapper

use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use async_spdk::blobfs::SpdkFilesystem;
use rocksdb::{
    ColumnFamily, TransactionDB, TransactionDBOptions, TransactionOptions, WriteOptions,
};

const JNL_CF_NAME: &str = "journal_cf";

pub struct RocksdbEngine {
    pub db: TransactionDB,
    txn_cf: &'static ColumnFamily,
    write_opts: WriteOptions,
    txn_opts: TransactionOptions,
}

fn rocksdb_options(
    fs: Arc<Mutex<SpdkFilesystem>>,
    fs_core: u32,
    data_path: &str,
    config: &str,
    bdev: &str,
    cache_size_in_mb: u64,
) -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    let fs = fs.lock().unwrap();
    let env = rocksdb::Env::rocksdb_use_spdk_env(
        fs.ptr as *mut c_void,
        fs_core,
        data_path,
        config,
        bdev,
        cache_size_in_mb,
    )
    .expect("fail to initilize spdk env");
    opts.create_if_missing(true);
    opts.set_env(&env);
    opts.increase_parallelism(4);
    opts.create_missing_column_families(true);
    opts
}

fn rocksdb_txn_db_options() -> TransactionDBOptions {
    let mut opts = TransactionDBOptions::default();
    opts.set_default_lock_timeout(0);
    opts.set_txn_lock_timeout(0);
    opts
}

// fn journal_cf_options() -> rocksdb::Options {
//     rocksdb_options()
// }

// fn default_cf_options() -> rocksdb::Options {
//     let mut opts = rocksdb_options();
//     opts.set_merge_operator("merge", full_merge, partial_merge);
//     opts
// }

impl RocksdbEngine {
    pub fn new() -> Self {
        let mut write_opts = WriteOptions::default();
        // let db = TransactionDB::open_cf_descriptors(
        // )
        // let mut write_opts = WriteOptions::default();
        // write_opts.disable_wal(!bool_from_env("MADFS_ENABLE_WAL"));
        // let db = TransactionDB::<SingleThreaded>::open_cf_descriptors(
        //     &rocksdb_options(),
        //     &rocksdb_txn_db_options(),
        //     meta_path.as_ref().join("rocksdb"),
        //     vec![
        //         ColumnFamilyDescriptor::new(
        //             rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
        //             default_cf_options(),
        //         ),
        //         ColumnFamilyDescriptor::new(TXN_CF_NAME, txn_cf_options()),
        //     ],
        // )?;
        // let txn_cf = unsafe { std::mem::transmute(db.cf_handle(TXN_CF_NAME).unwrap()) };
        // let storage = RocksdbStorage {
        //     db,
        //     txn_cf,
        //     write_opts,
        //     txn_opts: rocksdb_txn_options(),
        //     txns: Mutex::new(TxnTable::new()),
        //     objects: Mutex::new(ObjectTable::new()),
        // };
        // Ok(storage)
    }
}
