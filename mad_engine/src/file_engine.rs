use crate::common::*;
use crate::device_engine::*;
use crate::error::{EngineError, Result};
use crate::utils::*;
use crate::BlobEngine;
use crate::BsBindOpts;
use crate::EngineOpts;
use crate::RocksdbEngine;
use async_spdk::event;
use crate::db::*;
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
    opts: EngineOpts,
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
        let db = Arc::new(
            RocksdbEngine::new(
                opts.fs.clone(),
                0,
                path,
                &config_file,
                blobfs_dev,
                cache_size_in_mb,
            )
            .unwrap(),
        );

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
                    br.tblobs = vec![blob_id.clone()];
                    br.tfree_list = HashMap::new();
                    let bitmap = BitMap::new(BLOB_SIZE * CLUSTER_SIZE);
                    br.tfree_list.insert(blob_id.clone(), bitmap.clone());
                    let mut l = me.lock().unwrap();
                    l.blobs.push(blob_id.clone());
                    l.free_list
                        .insert(blob_id.clone().to_string(), bitmap.clone());
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
            mad_engine,
            pool,
            opts,
        })
    }

    pub async fn unload_bs(&self) -> Result<()> {
        self.blob_engine.unload().await;
        Ok(())
    }

    pub fn close_engine(&mut self) -> Result<()> {
        self.opts.finish();
        Ok(())
    }

    pub fn remove(&self, name: String) -> Result<()> {
        self.db.db.delete(name)?;
        Ok(())
    }

    pub fn create(&self, name: String) -> Result<()> {
        let chunk_meta = ChunkMeta::default();
        self.db
            .db
            .put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())?;
        Ok(())
    }

    pub fn stat(&self, name: String) -> Result<StatMeta> {
        let chunk_meta = self.db.db.get(name)?;
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())?;
        let ret = StatMeta {
            size: chunk_meta.size,
            csum_type: chunk_meta.csum_type.clone(),
        };
        Ok(ret)
    }

    pub async fn write(&mut self, name: String, offset: u64, data: &[u8]) -> Result<()> {
        todo!()
    }

    pub async fn read(&self, name: String, offset: u64, data: &mut [u8]) -> Result<()> {
        todo!()
    }
}
