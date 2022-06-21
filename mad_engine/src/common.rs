use crate::utils::*;
use async_spdk::blob::*;
use std::{collections::HashMap, sync::Mutex, thread::ThreadId};

#[derive(Debug, Clone)]
pub struct Chunk {
    // this should be oid + chunkId intuitively
    name: String,
    // size in pages
    size: u64,
    // page -> (BlobId, offset)
    location: HashMap<u64, (BlobId, u64)>,
    // checksum algorithm type
    csum_type: String,
    csum_data: u32,
}

#[derive(Debug)]
pub struct MadEngine {
    // global free list
    free_list: HashMap<BlobId, BitMap>,
    // allocated blobs
    blobs: Vec<BlobId>,
    // information about lower device
    cluster_size: u64,
    page_size: u64,
    io_unit: u64,
    total_cluster: u64,
    // each thread local variable allocation info
    alloc_list: HashMap<ThreadId, ThreadLocal>,
}

#[derive(Debug)]
pub struct ThreadLocal {
    // allocated blobs in the thread
    tblobs: Mutex<Vec<BlobId>>,
    // self owned free list, can 'steal' others' space
    tfree_list: Mutex<HashMap<BlobId, BitMap>>,
}
