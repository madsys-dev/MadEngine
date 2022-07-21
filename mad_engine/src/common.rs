use crate::error::{EngineError, Result};
use crate::{utils::*, DeviceEngine};
use async_spdk::blob::BlobId as SBlobId;
use async_spdk::env::DmaBuf;
use async_spdk::event;
use rocksdb::DB;
use rusty_pool::ThreadPool;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{
    cell::RefCell,
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

const PAGE_SIZE: u64 = 0x1000;

#[derive(Debug)]
pub struct Chunk {
    // this should be oid + chunkId intuitively
    name: String,
    meta: ChunkMeta,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkMeta {
    // size in bytes
    size: u64,
    // page -> (BlobId, offset)
    location: Option<HashMap<u64, PagePos>>,
    // checksum algorithm type
    csum_type: String,
    csum_data: Vec<u32>,
}

// structure used for stat
pub struct StatMeta {
    size: u64,
    csum_type: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
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

pub struct MadEngineHandle {
    db: Arc<DB>,
    device_engine: Arc<DeviceEngine>,
    mad_engine: Arc<Mutex<MadEngine>>,
    pool: ThreadPool,
}

impl MadEngineHandle {
    /// new basic MadEngineHandler
    pub async fn new(path: impl AsRef<Path>, device_name: &str) -> Result<Self> {
        let handle = Arc::new(DeviceEngine::new(device_name).await.unwrap());
        let db = Arc::new(DB::open_default(path).unwrap());
        let pool = ThreadPool::new(NUM_THREAD, NUM_THREAD, Duration::from_secs(1));
        let num_init = NUM_THREAD;
        for _ in 0..num_init {
            let handle = handle.clone();
            let blob_id = handle.create_blob(64).await.unwrap();
            let db = db.clone();
            // do the initialization work for each thread
            pool.spawn(async move {
                TLS.with(move |f| {
                    let mut br = f.borrow_mut();
                    br.tblobs = vec![blob_id.get_id().unwrap()];
                    br.tfree_list = HashMap::new();
                    let bitmap = BitMap::new(BLOB_SIZE * CLUSTER_SIZE);
                    br.tfree_list.insert(blob_id.get_id().unwrap(), bitmap);
                    br.db = Some(db);
                });
            });
        }
        pool.join();
        let total_cluster = handle.total_data_cluster_count().unwrap();
        Ok(Self {
            db: db.clone(),
            device_engine: handle,
            mad_engine: Arc::new(Mutex::new(MadEngine::new(total_cluster))),
            pool,
        })
    }

    /// read a chunk, this should return the read length
    ///
    /// TODO: check read range, return read length
    pub async fn read(&self, name: String, offset: u64, data: &mut [u8]) -> Result<()> {
        let len = data.len() as u64;
        let start_page = offset / PAGE_SIZE;
        let end_page = (offset + len - 1) / PAGE_SIZE;
        event::spawn(async {
            let chunk_meta = self.db.clone().get(name).unwrap();
            if chunk_meta.is_none() {
                return Err(EngineError::MetaNotExist);
            }
            let chunk_meta: ChunkMeta =
                serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())
                    .unwrap();
            let size = chunk_meta.size;
            if offset >= size || offset + len > size {
                return Err(EngineError::ReadOutRange);
            }
            let poses = (start_page..=end_page)
                .map(|p| {
                    chunk_meta
                        .location
                        .clone()
                        .unwrap()
                        .get(&p)
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();
            let checksum_vec = chunk_meta.csum_data.clone();
            // there should be a merger to merge succesive pages
            let mut buf = DmaBuf::alloc((PAGE_SIZE) as usize, 0x1000);
            let mut anchor = 0;
            for (i, pos) in poses.iter().enumerate() {
                self.device_engine
                    .clone()
                    .read(pos.offset, pos.bid, buf.as_mut())
                    .await
                    .unwrap();
                if Hasher::new().checksum(buf.as_ref()) != checksum_vec[i + start_page as usize] {
                    return Err(EngineError::CheckSumErr);
                }
                if i == poses.len() - 1 {
                    let end = offset + len - 1 - end_page * PAGE_SIZE;
                    let buffer = buf.as_ref();
                    data[anchor..].copy_from_slice(&buffer[0..=end as usize]);
                } else if i == 0 {
                    let buffer = buf.as_ref();
                    data[anchor..((start_page + 1) * PAGE_SIZE - offset) as usize]
                        .copy_from_slice(&buffer[((offset - start_page * PAGE_SIZE) as usize)..]);
                    anchor += ((start_page + 1) * PAGE_SIZE - offset) as usize;
                } else {
                    data[anchor..(anchor + PAGE_SIZE as usize)].copy_from_slice(&buf.as_ref());
                    anchor += PAGE_SIZE as usize;
                }
            }
            Ok(())
        })
        .await
        .unwrap();
        Ok(())
    }

    pub async fn write(&self, name: String, offset: u64, data: &[u8]) -> Result<()> {
        let len = data.len() as u64;
        let chunk_meta = self.db.clone().get(name.clone()).unwrap();
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let mut chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())
                .unwrap();
        let size = chunk_meta.get_size();
        if offset >= size {
            return Err(EngineError::HoleNotAllowed);
        }
        let mut checksum_vec = chunk_meta.csum_data.clone();
        let start_page = offset / PAGE_SIZE;
        let cover_end_page = size.min(offset + len - 1) / PAGE_SIZE;
        let end_page = (offset + len - 1) / PAGE_SIZE;
        let mut last_page = size as i64 / PAGE_SIZE as i64;
        if size == 0 {
            last_page = -1;
        }
        let total_page_num = end_page - start_page + 1;
        // first write
        let poses = if size == 0 {
            vec![]
        } else {
            (start_page..=cover_end_page)
                .map(|p| {
                    chunk_meta
                        .location
                        .clone()
                        .unwrap()
                        .get(&p)
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>()
        };
        // let poses = (start_page..=cover_end_page)
        //     .map(|p| {
        //         chunk_meta
        //             .location
        //             .clone()
        //             .unwrap()
        //             .get(&p)
        //             .unwrap()
        //             .clone()
        //     })
        //     .collect::<Vec<_>>();
        // get all the new positions to write new data
        // recycle old positions
        let poses_copy = poses.clone();
        let new_poses = self
            .pool
            .complete(async move {
                let mut ret = vec![];
                TLS.with(|f| {
                    let mut br = f.borrow_mut();
                    let mut cnt = total_page_num;
                    while cnt > 0 {
                        let tblobs = br.tblobs.clone();
                        for bid in tblobs.iter() {
                            let bm = br.tfree_list.get_mut(&bid);
                            if bm.is_none() {
                                break;
                            }
                            let bm = bm.unwrap();
                            let idx = bm.find().unwrap();
                            ret.push(PagePos {
                                bid: bid.clone(),
                                offset: idx,
                            });
                            bm.set(idx);
                            cnt -= 1;
                            if cnt == 0 {
                                break;
                            }
                        }
                    }
                    // recycle old pages
                    // fix bug: do a merger
                    let mut new_blobs = vec![];
                    for pos in poses_copy {
                        let mut flag = false;
                        let tblobs = br.tblobs.clone();
                        for bid in tblobs.iter() {
                            if *bid == pos.bid {
                                let bm = br.tfree_list.get_mut(&bid);
                                bm.unwrap().clear(pos.offset);
                                flag = true;
                                break;
                            }
                        }
                        // not in the old tblobs, but maybe in the newblobs
                        if flag == false {
                            for bid in new_blobs.iter() {
                                if *bid == pos.bid {
                                    flag = true;
                                    break;
                                }
                            }
                        }
                        if flag == false {
                            new_blobs.push(pos.bid);
                            let mut bm = BitMap::new_set_ones(BLOB_SIZE * CLUSTER_SIZE);
                            bm.clear(pos.offset);
                            br.tfree_list.insert(pos.bid, bm);
                        } else {
                            let bm = br.tfree_list.get_mut(&pos.bid).unwrap();
                            bm.clear(pos.offset);
                        }
                    }
                    for new_blob in new_blobs {
                        br.tblobs.push(new_blob);
                    }
                });
                ret
            })
            .await_complete();
        event::spawn(async {
            let mut idx_anchor = 0;
            let mut data_anchor = 0;
            for (i, pos) in poses.iter().enumerate() {
                let mut buf = DmaBuf::alloc((PAGE_SIZE) as usize, 0x1000);
                // read first page
                if i == 0 {
                    self.device_engine
                        .clone()
                        .read(pos.offset, pos.bid, buf.as_mut())
                        .await
                        .unwrap();
                    let buffer = buf.as_mut();
                    buffer[(offset - start_page * PAGE_SIZE) as usize..].copy_from_slice(
                        &data
                            [data_anchor..(PAGE_SIZE - (offset - start_page * PAGE_SIZE)) as usize],
                    );
                    checksum_vec[i + start_page as usize] = Hasher::new().checksum(buffer.as_ref());
                    self.device_engine
                        .clone()
                        .write(
                            new_poses[idx_anchor].offset,
                            new_poses[idx_anchor].bid,
                            buffer,
                        )
                        .await
                        .unwrap();
                    idx_anchor += 1;
                    data_anchor += (PAGE_SIZE - (offset - start_page * PAGE_SIZE)) as usize;
                } else if i == poses.len() - 1 {
                    // all overwrite
                    if end_page as i64 == last_page {
                        if size <= offset + len {
                            let buffer = buf.as_mut();
                            buffer.fill(0);
                            buffer[0..(data.len() - data_anchor)]
                                .copy_from_slice(&data[data_anchor..]);
                            checksum_vec[i + start_page as usize] =
                                Hasher::new().checksum(buffer.as_ref());
                            self.device_engine
                                .clone()
                                .write(
                                    new_poses[idx_anchor].offset,
                                    new_poses[idx_anchor].bid,
                                    buffer,
                                )
                                .await
                                .unwrap();
                            idx_anchor += 1;
                            data_anchor = data.len();
                        } else {
                            self.device_engine
                                .clone()
                                .read(pos.offset, pos.bid, buf.as_mut())
                                .await
                                .unwrap();
                            let buffer = buf.as_mut();
                            buffer[0..(offset + len - end_page * PAGE_SIZE) as usize]
                                .copy_from_slice(&data[data_anchor..]);
                            checksum_vec[i + start_page as usize] =
                                Hasher::new().checksum(buffer.as_ref());
                            self.device_engine
                                .clone()
                                .write(
                                    new_poses[idx_anchor].offset,
                                    new_poses[idx_anchor].bid,
                                    buffer,
                                )
                                .await
                                .unwrap();
                            idx_anchor += 1;
                            data_anchor = data.len();
                        }
                    } else if last_page > end_page as i64 {
                        self.device_engine
                            .clone()
                            .read(pos.offset, pos.bid, buf.as_mut())
                            .await
                            .unwrap();
                        let buffer = buf.as_mut();
                        buffer[0..(offset + len - end_page * PAGE_SIZE) as usize]
                            .copy_from_slice(&data[data_anchor..]);
                        checksum_vec[i + start_page as usize] =
                            Hasher::new().checksum(buffer.as_ref());
                        self.device_engine
                            .clone()
                            .write(
                                new_poses[idx_anchor].offset,
                                new_poses[idx_anchor].bid,
                                buffer,
                            )
                            .await
                            .unwrap();
                        idx_anchor += 1;
                        data_anchor = data.len();
                    } else {
                        self.device_engine
                            .clone()
                            .write(
                                new_poses[idx_anchor].offset,
                                new_poses[idx_anchor].bid,
                                &data[data_anchor..(data_anchor + PAGE_SIZE as usize)],
                            )
                            .await
                            .unwrap();
                        checksum_vec[i + start_page as usize] = Hasher::new()
                            .checksum(&data[data_anchor..(data_anchor + PAGE_SIZE as usize)]);
                        idx_anchor += 1;
                        data_anchor += PAGE_SIZE as usize;
                    }
                } else {
                    self.device_engine
                        .clone()
                        .write(
                            new_poses[idx_anchor].offset,
                            new_poses[idx_anchor].bid,
                            &data[data_anchor..(data_anchor + PAGE_SIZE as usize)],
                        )
                        .await
                        .unwrap();
                    checksum_vec[i + start_page as usize] = Hasher::new()
                        .checksum(&data[data_anchor..(data_anchor + PAGE_SIZE as usize)]);
                    idx_anchor += 1;
                    data_anchor += PAGE_SIZE as usize;
                }
            }
            if end_page as i64 > last_page {
                for page in last_page + 1..=end_page as i64 {
                    let mut buf = DmaBuf::alloc((PAGE_SIZE) as usize, 0x1000);
                    buf.as_mut().fill(0);
                    if page as u64 == end_page {
                        let buffer = buf.as_mut();
                        buffer[0..(data.len() - data_anchor)].copy_from_slice(&data[data_anchor..]);
                        checksum_vec.push(Hasher::new().checksum(buffer.as_ref()));
                        self.device_engine
                            .clone()
                            .write(
                                new_poses[idx_anchor].offset,
                                new_poses[idx_anchor].bid,
                                buffer,
                            )
                            .await
                            .unwrap();
                        idx_anchor += 1;
                        data_anchor = data.len();
                    } else {
                        self.device_engine
                            .clone()
                            .write(
                                new_poses[idx_anchor].offset,
                                new_poses[idx_anchor].bid,
                                &data[data_anchor..(data_anchor + PAGE_SIZE as usize)],
                            )
                            .await
                            .unwrap();
                        checksum_vec.push(
                            Hasher::new()
                                .checksum(&data[data_anchor..(data_anchor + PAGE_SIZE as usize)]),
                        );
                        idx_anchor += 1;
                        data_anchor += PAGE_SIZE as usize;
                    }
                }
            }
        })
        .await;
        chunk_meta.size = size.max(offset + data.len() as u64);
        let mut locations = match chunk_meta.location {
            Some(l) => l.clone(),
            None => HashMap::new(),
        };
        let mut idx = 0;
        for page in start_page..=end_page {
            let pos = PagePos {
                bid: new_poses[idx].bid,
                offset: new_poses[idx].offset,
            };
            locations.insert(page, pos);
            idx += 1;
        }
        chunk_meta.location = Some(locations);
        chunk_meta.csum_data = checksum_vec;
        chunk_meta.csum_type = "CRC32".into();
        self.db
            .put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())
            .unwrap();
        Ok(())
    }

    /// create a new chunk with no space allocated
    ///
    /// store default data in RocksDB
    pub fn create(&self, name: String) -> Result<()> {
        let db = self.db.clone();
        let chunk_meta = ChunkMeta::default();
        db.put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())
            .unwrap();
        Ok(())
    }

    /// remove a chunk's metadata from RocksDB
    ///
    /// todo: clear global freelist
    pub fn remove(&self, name: String) -> Result<()> {
        let db = self.db.clone();
        db.delete(name).unwrap();
        Ok(())
    }

    pub fn stat(&self, name: String) -> Result<StatMeta> {
        let chunk_meta = self.db.clone().get(name).unwrap();
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())
                .unwrap();
        let ret = StatMeta {
            size: chunk_meta.size,
            csum_type: chunk_meta.csum_type.clone(),
        };
        Ok(ret)
    }

    pub async fn unload(&self) -> Result<()> {
        self.device_engine.close_bs().await
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
    // channel: Option<IoChannel>,
    db: Option<Arc<DB>>,
    // bs: Option<Arc<Blobstore>>,
    handle: Option<Arc<DeviceEngine>>,
}

impl Default for ThreadData {
    fn default() -> Self {
        Self {
            tblobs: Vec::new(),
            tfree_list: HashMap::new(),
            // channel: None,
            db: None,
            // bs: None,
            handle: None,
        }
    }
}

thread_local! {
    static TLS: RefCell<ThreadData> = RefCell::new(ThreadData::default());
}
