use crate::utils::*;
use async_spdk::blob::{BlobId as SBlobId};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::Arc,
};

#[derive(Debug)]
pub struct Chunk {
    // this should be oid + chunkId intuitively
    name: String,
    meta: ChunkMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkMeta {
    // size in bytes
    pub(crate) size: u64,
    // page -> (BlobId, offset)
    pub(crate) location: Option<HashMap<u64, PagePos>>,
    // checksum algorithm type
    pub(crate) csum_type: String,
    pub(crate) csum_data: Vec<u32>,
}

// structure used for stat
pub struct StatMeta {
    pub(crate) size: u64,
    pub(crate) csum_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct PagePos {
    pub(crate) bid: SBlobId,
    pub(crate) offset: u64,
}

impl Default for ChunkMeta {
    fn default() -> Self {
        Self {
            size: 0,
            location: None,
            csum_type: "crc32".to_owned(),
            csum_data: vec![],
        }
    }
}

impl ChunkMeta {
    pub fn get_size(&self) -> u64 {
        self.size
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MadEngine {
    // global free list
    pub free_list: HashMap<SBlobId, BitMap>,
    // allocated blobs
    pub blobs: Vec<SBlobId>,
    // information about lower device
    pub(crate) device: DeviceInfo,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeviceInfo {
    // cluster size in MB
    pub(crate) cluster_size: u64,
    // page size in KB
    pub(crate) page_size: u64,
    // this should be euqal to page_size
    pub(crate) io_unit: u64,
    pub(crate) total_cluster: u64,
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
    pub(crate) tblobs: Vec<SBlobId>,
    // self owned free list, can 'steal' others' space
    pub(crate) tfree_list: HashMap<SBlobId, BitMap>,
    // channel: Option<IoChannel>,
    pub(crate) db: Option<Arc<DB>>,
    // bs: Option<Arc<Blobstore>>,
    // handle: Option<Arc<DeviceEngine>>,
}

impl Default for ThreadData {
    fn default() -> Self {
        Self {
            tblobs: Vec::new(),
            tfree_list: HashMap::new(),
            // channel: None,
            db: None,
            // bs: None,
            // handle: None,
        }
    }
}

thread_local! {
    pub static TLS: RefCell<ThreadData> = RefCell::new(ThreadData::default());
}
