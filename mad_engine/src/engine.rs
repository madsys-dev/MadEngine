use crate::common::*;
use crate::device_engine::*;
use crate::error::{EngineError, Result};
use crate::utils::*;
use async_spdk::env::DmaBuf;
use async_spdk::event;
use log::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use rusty_pool::ThreadPool;
use std::time::Duration;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

const PAGE_SIZE: u64 = 0x1000;

pub struct MadEngineHandle {
    db: Arc<DB>,
    device_engine: Arc<DeviceEngine>,
    mad_engine: Arc<Mutex<MadEngine>>,
    pool: ThreadPool,
}

impl Drop for MadEngineHandle {
    fn drop(&mut self) {
        self.db.flush().unwrap();
        let _ = DB::destroy(&Options::default(), "data");
    }
}

impl MadEngineHandle {
    /// create or restore a madengine handle
    pub async fn new(path: impl AsRef<Path>, device_name: &str) -> Result<Self> {
        let db = Arc::new(DB::open_default(path).unwrap());
        if let Some(_) = db
            .get(Hasher::new().checksum(MAGIC.as_bytes()).to_string())
            // .get(MAGIC.to_string())
            .unwrap()
        {
            info!("start to restore metadata");
            return Self::restore(db, device_name).await;
        } else {
            info!("create new engine");
            return Self::create_me(db.clone(), device_name).await;
        }
    }

    pub async fn restore(
        db: Arc<DBWithThreadMode<SingleThreaded>>,
        device_name: &str,
    ) -> Result<Self> {
        let handle = Arc::new(DeviceEngine::new(device_name, true).await.unwrap());
        let pool = ThreadPool::new(NUM_THREAD, NUM_THREAD, Duration::from_secs(1));
        let global = db
            // .get(MAGIC.to_string())
            .get(Hasher::new().checksum(MAGIC.as_bytes()).to_string())
            .unwrap();
        if global.is_none() {
            return Err(EngineError::RestoreFail);
        }
        let global_meta: MadEngine =
            serde_json::from_slice(&String::from_utf8(global.unwrap()).unwrap().as_bytes())
                .unwrap();
        let mad_engine = Arc::new(Mutex::new(global_meta));
        let num_init = NUM_THREAD;
        let num_blobs = {
            let l = mad_engine.lock().unwrap();
            l.blobs.len()
        };
        for i in 0..num_init {
            // let handle = handle.clone();
            let db = db.clone();
            let me = mad_engine.clone();
            let mut thread_blobids = vec![];
            let mut blob2map = HashMap::new();
            {
                let l = me.lock().unwrap();
                let mut id = i;
                while id < num_blobs {
                    thread_blobids.push(l.blobs[id]);
                    blob2map.insert(
                        l.blobs[id],
                        l.free_list.get(&l.blobs[id].to_string()).unwrap().clone(),
                    );
                    id += num_init;
                }
            }
            pool.spawn(async move {
                TLS.with(move |f| {
                    let mut br = f.borrow_mut();
                    br.tblobs = thread_blobids;
                    br.tfree_list = blob2map;
                    br.db = Some(db);
                });
            });
        }
        pool.join();
        Ok(Self {
            db: db.clone(),
            device_engine: handle,
            mad_engine,
            pool,
        })
    }

    /// create basic MadEngineHandle
    pub async fn create_me(
        db: Arc<DBWithThreadMode<SingleThreaded>>,
        device_name: &str,
    ) -> Result<Self> {
        let handle = Arc::new(DeviceEngine::new(device_name, false).await.unwrap());
        let pool = ThreadPool::new(NUM_THREAD, NUM_THREAD, Duration::from_secs(1));
        let total_cluster = handle.total_data_cluster_count().unwrap();
        let num_init = NUM_THREAD;
        let mad_engine = Arc::new(Mutex::new(MadEngine::new(total_cluster)));
        for _ in 0..num_init {
            let handle = handle.clone();
            let blob_id = handle.create_blob(64).await.unwrap();
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
        db.put(
            Hasher::new().checksum(MAGIC.as_bytes()).to_string(),
            // MAGIC.to_string(),
            serde_json::to_string(&global).unwrap().as_bytes(),
        )
        .unwrap();
        Ok(Self {
            db: db.clone(),
            device_engine: handle,
            mad_engine,
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
            let chunk_meta = self.db.get(name)?;
            if chunk_meta.is_none() {
                return Err(EngineError::MetaNotExist);
            }
            let chunk_meta: ChunkMeta = serde_json::from_slice(
                &String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes(),
            )?;
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
                    .await?;
                if Hasher::new().checksum(buf.as_ref()) != checksum_vec[i + start_page as usize] {
                    return Err(EngineError::CheckSumErr);
                }
                // last page
                if i == poses.len() - 1 {
                    let end = offset + len - 1 - end_page * PAGE_SIZE;
                    let buffer = buf.as_ref();
                    data[anchor..].copy_from_slice(&buffer[0..=end as usize]);
                }
                // first page
                else if i == 0 {
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
        .await?;
        Ok(())
    }

    pub async fn write(&mut self, name: String, offset: u64, data: &[u8]) -> Result<()> {
        let len = data.len() as u64;
        // get the chunk metadata
        let chunk_meta = self.db.get(&name).unwrap();
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let mut chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())
                .unwrap();
        let size = chunk_meta.get_size();
        if offset > size {
            return Err(EngineError::HoleNotAllowed);
        }
        let mut checksum_vec = chunk_meta.csum_data.clone();
        let start_page = offset / PAGE_SIZE;
        // how many pages are overwritten
        let cover_end_page = size.min(offset + len - 1) / PAGE_SIZE;
        let end_page = (offset + len - 1) / PAGE_SIZE;
        // last page before this write, -1 if this is the first write
        let mut last_page = size as i64 / PAGE_SIZE as i64;
        // first write
        if size == 0 {
            last_page = -1;
        }
        let total_page_num = end_page - start_page + 1;
        let global = self
            .db
            .get(Hasher::new().checksum(MAGIC.as_bytes()).to_string())
            // .get(MAGIC.to_string())
            ?;
        if global.is_none() {
            return Err(EngineError::GlobalGetFail);
        }
        let mut global_meta: MadEngine =
            serde_json::from_slice(&String::from_utf8(global.unwrap()).unwrap().as_bytes())
                .unwrap();
        // let mad_engine = Arc::new(Mutex::new(global_meta));
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
        // get all the new positions to write new data
        // recycle old positions
        let poses_copy = poses.clone();
        let db_copy = self.db.clone();
        let (new_poses, new_blob2map) = self
            .pool
            .complete(async move {
                let mut ret = vec![];
                let mut new_blob2map = global_meta.free_list.clone();
                TLS.with(|f| {
                    let mut br = f.borrow_mut();
                    let mut cnt = total_page_num;
                    while cnt > 0 {
                        let tblobs = br.tblobs.clone();
                        for bid in tblobs.iter() {
                            let bm = br.tfree_list.get_mut(&bid);
                            if bm.is_none() {
                                continue;
                            }
                            let bm = bm.unwrap();
                            let idx = bm.find().unwrap();
                            // here should check full or not
                            ret.push(PagePos {
                                bid: bid.clone(),
                                offset: idx,
                            });
                            bm.set(idx);
                            global_meta
                                .free_list
                                .insert(bid.clone().to_string(), bm.clone());
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
                        let e = new_blob2map.get_mut(&pos.bid.to_string()).unwrap();
                        e.clear(pos.offset);
                        drop(e);
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
                    global_meta.free_list = new_blob2map.clone();
                    db_copy
                        .put(
                            Hasher::new().checksum(MAGIC.as_bytes()).to_string(),
                            // MAGIC.to_string(),
                            serde_json::to_string(&global_meta.clone())
                                .unwrap()
                                .as_bytes(),
                        )
                        .unwrap();
                });
                (ret, new_blob2map)
            })
            .await_complete();
        let mut l = self.mad_engine.lock().unwrap();
        l.free_list = new_blob2map;
        drop(l);
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
            .put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())?;
        Ok(())
    }

    /// create a new chunk with no space allocated
    ///
    /// store default data in RocksDB
    pub fn create(&self, name: String) -> Result<()> {
        let db = self.db.clone();
        let chunk_meta = ChunkMeta::default();
        db.put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())?;
        Ok(())
    }

    /// remove a chunk's metadata from RocksDB
    ///
    /// todo: clear global freelist
    pub fn remove(&self, name: String) -> Result<()> {
        let db = self.db.clone();
        db.delete(name)?;
        Ok(())
    }

    pub fn stat(&self, name: String) -> Result<StatMeta> {
        let chunk_meta = self.db.clone().get(name).unwrap();
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

    pub async fn unload(&self) -> Result<()> {
        self.device_engine.close_bs().await
    }
}
