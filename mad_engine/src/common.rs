use crate::utils::*;
use async_spdk::blob::{self, BlobId as SBlobId, Blobstore, IoChannel};
use async_spdk::blob_bdev;
use rayon::{ThreadPool, ThreadPoolBuilder};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::io::Result;
use std::{
    cell::RefCell,
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    thread::ThreadId,
};
use thread_local::ThreadLocal;

#[derive(Debug)]
pub struct Chunk {
    // this should be oid + chunkId intuitively
    name: String,
    meta: ChunkMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkMeta {
    // size in pages
    size: u64,
    // page -> (BlobId, offset)
    location: Option<HashMap<u64, PagePos>>,
    // checksum algorithm type
    csum_type: String,
    csum_data: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PagePos {
    bid: SBlobId,
    offset: u64,
}

impl Default for ChunkMeta {
    fn default() -> Self {
        Self {
            size: 0,
            location: None,
            csum_type: "crc32".to_owned(),
            csum_data: 0,
        }
    }
}

impl Chunk {
    pub fn new(name: String, size: u64) -> Self {
        Self {
            name,
            meta: ChunkMeta::default(),
        }
    }
}

#[derive(Debug)]
pub struct MadEngineHandle {
    db: Arc<Mutex<DB>>,
    engine: Arc<Mutex<MadEngine>>,
    pool: ThreadPool,
    bs: Blobstore,
    channel: IoChannel,
}

impl MadEngineHandle {
    /// new basic MadEngineHandler
    pub async fn new(path: impl AsRef<Path>, device_name: &'_ str) -> Self {
        let db = DB::open_default(path).unwrap();
        let pool = ThreadPoolBuilder::new().num_threads(4).build().unwrap();
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(device_name).unwrap();
        let bs = blob::Blobstore::init(&mut bs_dev).await.unwrap();
        let total_cluster = bs.total_data_cluster_count();
        let channel = bs.alloc_io_channel().unwrap();
        Self {
            db: Arc::new(Mutex::new(db)),
            engine: Arc::new(Mutex::new(MadEngine::new(total_cluster))),
            pool,
            bs,
            channel
        }
    }

    pub async fn read(&self, name: String, offset: u64, data: &mut [u8]) {
        unimplemented!()
    }

    pub async fn write(&self, name: String, offset: u64, data: &[u8]) {
        unimplemented!()
    }

    /// create a new chunk with no space allocated
    ///
    /// store default data in RocksDB
    pub fn create(&self, name: String) -> Result<()> {
        let db = self.db.lock().unwrap();
        let chunk_meta = ChunkMeta::default();
        db.put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())
            .unwrap();
        Ok(())
    }

    /// remove a chunk's metadata from RocksDB
    ///
    /// todo: clear global freelist
    pub fn remove(&self, name: String) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.delete(name).unwrap();
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MadEngine {
    // global free list
    free_list: HashMap<SBlobId, BitMap>,
    // allocated blobs
    blobs: Vec<SBlobId>,
    // information about lower device
    device: DeviceInfo,
    // each thread local variable allocation info
    //alloc_list: HashMap<ThreadId, ThreadData>,
}

impl MadEngine {
    pub fn new(total_cluster: u64) -> Self {
        Self {
            free_list: HashMap::new(),
            blobs: Vec::new(),
            device: DeviceInfo::new(total_cluster),
        }
    }

    pub fn set_total_cluster(&mut self, total_cluster: u64) {
        self.device.cluster_size = total_cluster;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceInfo {
    // cluster size in MB
    cluster_size: u64,
    // page size in KB
    page_size: u64,
    // this should be euqal to page_size
    io_unit: u64,
    total_cluster: u64,
}

impl DeviceInfo {
    pub fn new(total_cluster: u64) -> Self {
        Self {
            cluster_size: 1,
            page_size: 4,
            io_unit: 4,
            total_cluster,
        }
    }
}

#[derive(Debug)]
pub struct ThreadData {
    // allocated blobs in the thread
    tblobs: Vec<SBlobId>,
    // self owned free list, can 'steal' others' space
    tfree_list: HashMap<SBlobId, BitMap>,
}
