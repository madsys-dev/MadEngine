use crate::common::*;
use crate::device_engine::*;
use crate::error::{EngineError, Result};
use crate::utils::*;
use crate::BlobEngine;
use crate::BsBindOpts;
use crate::EngineOpts;
use crate::RocksdbEngine;
use async_spdk::env::DmaBuf;
use async_spdk::event;
use db::*;
use futures::TryFutureExt;
use log::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use rusty_pool::ThreadPool;
use std::time::Duration;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

pub struct FileEngine {
    db: Arc<RocksdbEngine>,
    blob_engine: Arc<BlobEngine>,
    mad_engine: Arc<Mutex<MadEngine>>,
    pool: ThreadPool,
}

impl Drop for FileEngine {
    fn drop(&mut self) {}
}

impl FileEngine {
    pub async fn new(
        path: impl AsRef<Path>,
        config_file: String,
        reactor_mask: &str,
        blobfs_dev: &str,
        // now only provide one single blobstore
        bs_dev: &str,
        bs_core: u32,
        app_name: &str,
        cache_size_in_mb: u64,
    ) -> Result<Self> {
        // Set SPDK opts
        let mut opts = EngineOpts::default();
        opts.set_blobfs(blobfs_dev);
        opts.set_reactor_mask("0x3");
        opts.set_blobstore(vec![BsBindOpts {
            bdev_name: bs_dev.to_string(),
            core: bs_core,
        }]);
        opts.set_config_file(config_file.clone());
        opts.set_name(app_name);

        // Start SPDK environment
        opts.start_spdk();

        // Wait for blobfs and blobstore establishing
        opts.ready();

        // Build TransactionDB
        let db = Arc::new(RocksdbEngine::new(
            opts.fs.clone(),
            0,
            path,
            &config_file,
            blobfs_dev,
            cache_size_in_mb,
        ));

        let be = Arc::new(opts.create_be());

        let pool = ThreadPool::new(NUM_THREAD, NUM_THREAD, Duration::from_secs(1));
        let num_init = NUM_THREAD;
        // TODO: finish cluster count API
        let mad_engine = Arc::new(Mutex::new(MadEngine::new(256)));
        for _ in 0..num_init {
            let handle = be.clone();
            let blob_id = handle.create_blob().await.unwrap();
            let blob = handle.open_blob(blob_id).await.unwrap();
            handle.resize_blob(blob, 64).await.unwrap();
            handle.sync_blob(blob).await.unwrap();

            let db = db.clone();
            let me = mad_engine.clone();

            // do the initialization work for each thread
            pool.spawn(async move {
                TLS.with(move |f| {
                    let mut br = f.borrow_mut();
                    br.tblobs = vec![blob_id.get_id().unwrap()];
                    br.tfree_list = HashMap::new();
                    let bitmap = BitMap::new(BLOB_SIZE * CLUSTER_SIZE);
                    br.tfree_list
                        .insert(blob_id.get_id().unwrap(), bitmap.clone());
                    let mut l = me.lock().unwrap();
                    l.blobs.push(blob_id.get_id().unwrap());
                    l.free_list
                        .insert(blob_id.get_id().unwrap().to_string(), bitmap.clone());
                    drop(l);
                    br.db = Some(db);
                });
            });
        }
        pool.join();
        let magic = mad_engine.lock().unwrap();
        let global = magic.clone();
        drop(magic);

        db.db
            .put(
                Hasher::new().checksum(MAGIC.as_bytes()).to_string(),
                // MAGIC.to_string(),
                serde_json::to_string(&global).unwrap().as_bytes(),
            )
            .unwrap();

        Ok(Self {
            db: db.clone(),
            blob_engine: be,
            mad_engine: mad_engine,
            pool,
        })
    }
}
