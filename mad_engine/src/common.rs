use crate::utils::*;
use async_spdk::blob::{self, BlobId as SBlobId, Blobstore, IoChannel};
use async_spdk::blob_bdev;
use async_spdk::env::DmaBuf;
use rayon::vec;
// use rayon::{ThreadPool, ThreadPoolBuilder};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::io::Result;
use std::sync::Barrier;
use std::{
    cell::RefCell,
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    thread::ThreadId,
};
use thread_local::ThreadLocal;
use threadpool::ThreadPool;

const PAGE_SIZE: u64 = 0x1000;

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
    bs: Arc<Blobstore>,
}

impl MadEngineHandle {
    /// new basic MadEngineHandler
    pub async fn new(path: impl AsRef<Path>, device_name: &'_ str) -> Self {
        let db = DB::open_default(path).unwrap();
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(device_name).unwrap();
        let bs = Arc::new(blob::Blobstore::init(&mut bs_dev).await.unwrap());
        let pool = ThreadPool::new(NUM_THREAD);
        let num_init = NUM_THREAD;
        let barrier = Arc::new(Barrier::new(num_init + 1));
        for _ in 0..num_init {
            let barrier = barrier.clone();
            let io_channel = bs.alloc_io_channel().unwrap();
            let blob_id = bs.create_blob().await.unwrap();
            let blob = bs.open_blob(blob_id).await.unwrap();
            blob.resize(BLOB_SIZE).await.unwrap();
            blob.sync_metadata().await.unwrap();
            // do the initialization work for each thread
            // use barrier to sync all of them
            pool.execute(move || {
                TLS.with(move |f| {
                    let mut br = f.borrow_mut();
                    br.channel = Some(io_channel);
                    br.tblobs = vec![blob_id];
                    br.tfree_list = HashMap::new();
                    let bitmap = BitMap::new(BLOB_SIZE * CLUSTER_SIZE);
                    br.tfree_list.insert(blob_id, bitmap);
                });
                barrier.wait();
            });
        }
        barrier.wait();
        let total_cluster = bs.total_data_cluster_count();
        Self {
            db: Arc::new(Mutex::new(db)),
            engine: Arc::new(Mutex::new(MadEngine::new(total_cluster))),
            pool,
            bs,
        }
    }

    pub async fn read(&self, name: String, offset: u64, data: &mut [u8]) {
        let len = data.len();
        let start_page = offset / PAGE_SIZE;
        let end_page = (offset + len as u64) / PAGE_SIZE;
        // self.pool.install(move ||{
        //     let io_channel = TLS.with(|t|{
        //         let tmp = t.borrow_mut();
        //     });
        // });
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
    channel: Option<IoChannel>,
}

impl Default for ThreadData {
    fn default() -> Self {
        Self {
            tblobs: Vec::new(),
            tfree_list: HashMap::new(),
            channel: None,
        }
    }
}

thread_local! {
    static TLS: RefCell<ThreadData> = RefCell::new(ThreadData::default());
}
